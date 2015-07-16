# encoding: utf-8
require 'logstash/inputs/base'
require 'logstash/namespace'

require 'csv'
require_relative 'client_with_streaming_support'


# This plugin enables Salesforce customers to load EventLogFile(ELF) data from their Force.com orgs.
# The plugin will handle downloading ELF CSV file, parsing them, and handling any schema changes transparently.
class LogStash::Inputs::SfdcElf < LogStash::Inputs::Base

  # Constants
  SEPARATOR      = ','
  QUOTE_CHAR     = '"'
  DEFAULT_TIME   = '0001-01-01T00:00:00Z'
  FILE_PREFIX    = 'sfdc_elf_logstash'
  LOG_KEY        = 'SFDC'
  RETRY_ATTEMPTS = 3


  config_name 'sfdc_elf'
  default :codec, 'plain'

  #todo how to publish doc to logstash

  # Username to your Force.com organization.
  config :username, :validate => :string, :required => true

  # Password to your Force.com organization.
  config :password, :validate => :password, :required => true

  # Security token to you Force.com organization, can be found in My Settings > Personal > Reset My Security Token. Then
  # it will take you to "Reset My Security Token" page, and click on the "Reset Security Token" button. The token will
  # be emailed to you.
  # Todo make make a simple .gif that shows this process? and attach it?
  config :security_token, :validate => :password, :required => true

  # The path to the .sfdc_info to use as an input. You set the path like so, `/var/log` Paths must be absolute and
  # cannot be relative.
  # todo: abi..
  config :sfdc_info_path, :validate => :string, :default => Dir.home

  # How often this plugin should grab new data.
  config :poll_interval_in_hours, :validate => [*6..24], :default => 24




  # The first part of logstash pipeline is register, where all instance variables are initialized.

  public
  def register
    # Initialize client
    @client = ClientWithStreamingSupport.new

    # Do not change id and secret. Currently pointing to "Event Log File Logstash Plugin," a long running app for this plugin.
    @client.client_id = '3MVG9xOCXq4ID1uGlgyzp8E4HENTnwB05RL1qOmas88eMfE0mk7h0duhs3EnEY2v7Khs9aUXQnrUdB_wm.yJx'
    @client.client_secret = '5847713965780458928'
    @client.version = '33.0'

    # Authenticate the client
    @logger.info("#{LOG_KEY}: tyring to authenticate client")
    @client.retryable_authenticate(username: @username, password: @password.value + @security_token.value, retry_attempts: RETRY_ATTEMPTS)

    # Save org id to distinguish between multiple orgs.
    # @org_id = @client.org_id #todo(mo) why doesnt this work???
    @org_id = @client.query('select id from Organization')[0]['Id']

    # Set up time interval for forever while loop.
    @interval = @poll_interval_in_hours * 3600

    # Set @sfdc_info_path to home directory if provided path from config does not exist.
    unless File.directory?(@sfdc_info_path)
      @logger.warn("#{LOG_KEY}: provided path does not exist or is invalid. sfdc_info_path=#{@sfdc_info_path}")
      @sfdc_info_path = Dir.home
    end

    # Generate the path using org_id to keep track of the last read log file date based on the org rather than users.
    @path = "#{@sfdc_info_path}/.#{FILE_PREFIX}_#{@org_id}"
    @logger.info("#{LOG_KEY}: generated info path = #{@path}")

    # Read from .sfdc_info if it exists, otherwise load @last_read_log_date with DEFAULT_TIME.
    if File.exist?(@path)
      # Load last read LogDate from .sfdc_info.
      @last_read_log_date = File.read(@path)
      @logger.info("#{LOG_KEY}: .#{@path} does exist, read time from it and stored it in @last_read_instant")
    else

      # Load default time to ensure getting all possible EventLogFiles from oldest to current.
      # Note in create_event_list(), which is called in run(), is where .sfdc_info is created or overwritten, so no need to create it here.
      @last_read_log_date = DEFAULT_TIME
      @logger.info("#{LOG_KEY}: .sfdc_elf_logstatsh does not exist and loaded DEFAULT_TIME to @last_read_instant")
    end

    @logger.info("#{LOG_KEY}: @last_read_instant =  #{@last_read_log_date}")
  end # def register




  # The second part of logstash pipeline is run, where it expects to have event objects generated and passed into the queue.

  public
  def run(queue)
    # Grab the current time and one @interval to it so that the while loop knows when it need to compute again.
    next_schedule_time = Time.now + @interval

    while true
      # Line for readable log statements.
      @logger.info('---------------------------------------------------')

      # Grab a list of Sobjects, specifically EventLogFiles.
      soql_expr = "SELECT Id, EventType, Logfile, LogDate, LogFileLength FROM EventLogFile WHERE LogDate > #{@last_read_log_date} ORDER BY LogDate DESC "
      query_result_list = @client.retryable_query(username: @username, password: @password.value + @security_token.value, retry_attempts: RETRY_ATTEMPTS, soql_expr: soql_expr)

      if !query_result_list.empty?
        # Creates events from query_result_list, then simply append the events to the queue.
        @logger.info("#{LOG_KEY}: query result is NOT empty, size = #{query_result_list.size.to_s}")
        enqueue_events(query_result_list, queue)
      else
        # Make sure to save the last read LogDate even when query_result_list is empty
        @logger.info("#{LOG_KEY}: query result is empty")
        # save_last_read_log_date(Time.now.utc.iso8601.to_s)
        save_last_read_log_date(DateTime.now.new_offset(0).strftime("%FT%T.%LZ"))
      end

      # Depending on the next_schedule_time and the time taking the compute the code above, sleep this loop and adjust the next_schedule_time.
      @logger.info("#{LOG_KEY}: next_schedule_time = #{next_schedule_time.to_s}")
      next_schedule_time = stall_schedule(next_schedule_time)

    end # while loop
  end # def run




  # Given a list of query result that are Sobjects, iterate through the list and grab all the CSV files that each
  # Sobject points to via get_csv_tempfile_list(). Once that is done we save the first LogDate in the list to the
  # @path file. After that we parse the CSV files parse it line by line and generate the events for the parsed CSV
  # line, append it to the queue.
  #
  # Note: when grabbing the CSV files, they are stored as Tempfiles and deleted after parsed.

  private
  def enqueue_events(query_result_list, queue)
    @logger.info("#{LOG_KEY}: enqueue events")
    # query_result_list is in descending order based on the LogDate, so grab the first one of the list and save the LogDate to @last_read_log_date and .sfdc_info
    @last_read_log_date = query_result_list.first.LogDate.strftime('%FT%T.%LZ')

    # Overwrite the .sfdc_elf_logstash file with the @last_read_log_date.
    # Note: we currently do not support deduplication, but will implement it soon. todo:need to implement deduplication
    save_last_read_log_date(@last_read_log_date) #todo might have to move this to the end of the method, in case of a crash in between.

    # Grab a list of Tempfiles that contains CSV file data.
    tempfile_list = get_csv_tempfile_list(query_result_list)

    # Loop though each tempfile.
    tempfile_list.each do |tmp|

      # Get the column from Tempfile, which is in the first line and in CSV format, then parse it. It will return an array.
      column = CSV.parse_line(tmp.readline, :col_sep => SEPARATOR, :quote_char => QUOTE_CHAR)

      # Loop through tempfile, line by line.
      tmp.each_line do |data|

        # Parse the current line, it will return an array.
        parsed_data = CSV.parse_line(data, :col_sep => SEPARATOR, :quote_char => QUOTE_CHAR)

        # create_event will return a event object.
        queue << create_event(column, parsed_data)
      end

      # Close tmp file and unlink it, doing this will delete the actual tempfile.
      tmp.close
      tmp.unlink
    end # do loop, tempfile_list
  end # def create_event_list


  # This helper method takes as input a key data and val data that is in CSV format. Using
  # CSV.parse_line we will get back an array for each then one of them. Then create a new
  # Event object where we will place all of the key value pairs into the Event object and then
  # return it.d

  private
  def create_event(column, data)

    # Initaialize event to be used. @timestamp and @version is automatically added
    event = LogStash::Event.new()

    # Add column data pair to event.
    data.each_index do |i|

      # Grab current key.
      column_name = column[i]

      # Handle when field_name is 'TIMESTAMP', Change the @timestamp field to the actual time on the CSV file, but convert it to iso8601.
      event.timestamp = DateTime.parse(data[i]).strftime("%FT%T.%LZ") if column_name == 'TIMESTAMP'

      # Add the column data pair to event object.
      event[column_name] = data[i]
    end

    # Return the event
    event
  end # def create_event




  # This helper method takes as input a list/collection of Sobjects which each
  # contains a path to their respective CSV files. The path is stored in the
  # LogFile field. Using that path, we are able to grab the actual CSV file via
  # @client.http_get method.
  #
  # After grabbing the CSV file we then store them using the standard Tempfile library.
  # Tempfile will create a unique file each time using 'sfdc_elf_tempfile' as the prefix and
  # finally we will be returning a list of Tempfile object, where the user can read the
  # Tempfile and then close it and unlink it, which will delete the file.
  #
  # Note: for debugging tmp.path will help find the path where the Tempfile is stored. #

  private
  def get_csv_tempfile_list(query_result_list)
    @logger.info("#{LOG_KEY}: generating tempfile list")
    result =[]
    query_result_list.each do |event_log_file|

      # Get the path of the CSV file from the LogFile field, then stream the data to the .write method of the Tempfile
      tmp = Tempfile.new('sfdc_elf_tempfile')
      @client.streaming_download(event_log_file.LogFile, tmp)

      # Flushing will write the buffer into the Tempfile itself.
      tmp.flush

      # Rewind will move the file pointer from the end to the beginning of the file, so that users can simple call the Read method.
      tmp.rewind

      # Append the Tempfile object into the result list
      result << tmp

      # Log the info from event_log_file object.
      @logger.info("  #{LOG_KEY}: Id = #{event_log_file.Id}")
      @logger.info("  #{LOG_KEY}: EventType = #{event_log_file.EventType}")
      @logger.info("  #{LOG_KEY}: LogFile = #{event_log_file.LogFile}")
      @logger.info("  #{LOG_KEY}: LogDate = #{event_log_file.LogDate.to_s}")
      @logger.info("  #{LOG_KEY}: LogFileLength = #{event_log_file.LogFileLength}")
      @logger.info('  ......................................')
    end
    result
  end # def get_csv_files




  # Take as input a time sting that is in iso8601 format. The overwrite .sfdc_elf_logstash with the time string,
  # because of the 'w' flag.

  private
  def save_last_read_log_date(time)
    @logger.info("#{LOG_KEY}: overwriting #{@path} with #{time}")
    f = File.open(@path, 'w')
    f.write(time)
    f.flush
    f.close
  end

  # Given as input the next schedule time, stall_schedule() will decide if we need to sleep until the next
  # schedule time or skip sleeping because of missing the next schedule time.
  #
  # For both examples, the time interval is 1 hour.
  # Example 1:
  # started time       = 1:00pm
  # next_schedule_time = 2:00pm
  # current_time       = 1:30pm
  # In this example you will need to sleep for 30 mins, so you will be on schedule.
  #
  # Example 2:
  # started time       = 1:00pm
  # next_schedule_time = 2:00pm
  # current_time       = 2:30pm
  # In this example you will not be allowed to sleep, and will proceed to compute again since you missed the schedule time.

  private
  def stall_schedule(next_schedule_time)
    current_time = Time.now
    @logger.info("#{LOG_KEY}: time before sleep  = #{current_time.to_s}")

    # Example 2 case from above.
    if current_time > next_schedule_time
      @logger.info("#{LOG_KEY}: missed next schedule time, proceeding to next task without sleeping")
      while current_time > next_schedule_time
        next_schedule_time += @interval
      end

    # Example 1 case from above.
    else
      @logger.info("#{LOG_KEY}: sleeping for #{(next_schedule_time - current_time).to_s} seconds")
      sleep(next_schedule_time - current_time)
      next_schedule_time += @interval
    end
    @logger.info("#{LOG_KEY} time after sleep   = #{Time.now.to_s}")
    next_schedule_time
  end # def determine_loop_stall


end # class LogStash::Inputs::File