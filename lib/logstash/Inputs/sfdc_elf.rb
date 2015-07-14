# encoding: utf-8
require 'logstash/inputs/base'
require 'logstash/namespace'

require 'stud/interval'
require 'csv'
require_relative '../../logstash/Inputs/client_with_streaming_support'


# This plugin enables Salesforce customers to use ELK stack as there choice of exploration and visualization of their
# EventLogFile(ELF) data from their Force.com ordinations.
#
# The plugin will handle downloading ELF CSV file and parsing them, any schema changes transparently, and event
# deduplication.
class LogStash::Inputs::SfdcElf < LogStash::Inputs::Base

  # Constants
  SEPARATOR = ','
  QUOTE_CHAR = '"'
  DEFAULT_TIME = '0001-01-01T00:00:00Z'
  FILE_PREFIX = 'sfdc_elf_logstash'
  LOG_KEY = 'SFDC'

  config_name 'sfdc_elf'
  default :codec, 'plain'

  # Username to your Force.com organization.
  config :username, :validate => :string, :required => true

  # Password to your Force.com organization.
  config :password, :validate => :password, :required => true

  # Security token to you Force.com organization, can be found in My Settings > Personal > Reset My Security Token. Then
  # it will take you to "Reset My Security Token" page, and click on the "Reset Security Token" button. The token will
  # be emailed to you.
  config :security_token, :validate => :password, :required => true

  # The path to the .sfdc_info to use as an input. You set the path like so, `/var/log` Paths must be absolute and
  # cannot be relative.
  config :sfdc_info_path, :validate => :string, :default => Dir.home

  # How often this plugin should grab new data.
  config :poll_interval_in_hours, :validate => :number, :default => 24 #todo range, poll intervals 6 to 24


  public
  def register
    # Initaialize client
    @client = ClientWithStreamingSupport.new
    @client.client_id = '3MVG9xOCXq4ID1uGlgyzp8E4HEHfzI4iryotXS3FtHQIZ5VYhE8.JPehyksO.uYZmZHct.xlXVxqDCih35j0.'
    @client.client_secret = '7829455833495769170'
    @client.version = '33.0'

    # Authenticate the client
    authenticate

    # Save org id to distinguish between multiple orgs.
    #todo do i really need this?? only used it to create @path variable
    # @org_id = @client.org_id #todo why doesnt this work???
    @org_id = @client.query("select id from Organization")[0]["Id"]

    # Set up time interval for forever while loop.
    # @interval = @poll_interval_in_hours * 3600
    @interval = 30

    # Set @sfdc_info_path to home directory if provided path from config does not exist.
    @sfdc_info_path = Dir.home unless File.directory?(@sfdc_info_path)

    # Generate the path using org_id to keep track of the last read log file date based on the org rather than users.
    @path = "#{@sfdc_info_path}/.#{FILE_PREFIX}_#{@org_id}"
    @logger.info("#{LOG_KEY}: genarted path = #{@path}")

    # Read from .sfdc_info if it exists, otherwise load @last_read_log_date with DEFAULT_TIME.
    if File.exist?(@path)
      # Load last read LogDate from .sfdc_info.
      @last_read_log_date = File.read(@path)
      @logger.info("#{LOG_KEY}: .#{@path} does exist, read time from it and stored it in @last_read_instant")
    else

      # Load default time to ensure getting all possible EventLogFiles from oldest to current.
      # Note in create_event_list(), which is called in run(), is where .sfdc_info is created or overwritten, so no need to create it here.
      @last_read_log_date = DEFAULT_TIME
      @logger.info('MO: .sfdc_info does not exist and loaded DEFAULT_TIME to @last_read_instant')
    end

    @logger.info("#{LOG_KEY}: @last_read_instant =  #{@last_read_log_date}")
  end

  # def register


  public
  def run(queue)
    # Grab the current time and one @interval to it so that the while loop knows when it need to compute again.
    next_schedule_time = Time.now + @interval

    while true
      begin
        # Line for readable log statements.
        @logger.info('---------------------------------------------------')

        # Grab a list of Sobjects, specifically EventLogFiles.
        query_result_list = @client.query("SELECT Id, EventType, Logfile, LogDate FROM EventLogFile WHERE LogDate > #{@last_read_log_date} ORDER BY LogDate DESC ")

        if !query_result_list.empty?
          @logger.info("#{LOG_KEY}: query result is NOT empty, size = #{query_result_list.size.to_s}")

          # Grab an list of events based on the query result. Then simply append the events to the queue.
          @logger.info('MO: going into create_event_list')
          event_list = create_event_list(query_result_list)
          @logger.info('MO: going into create_event_list')
          event_list.each do |event| queue << event end
        else
          @logger.info("#{LOG_KEY}: query result is empty")
          save_last_read_log_date(Time.now.utc.iso8601.to_s)
        end

        # Depending on the next_schedule_time and the time taking the compute the code above, sleep this loop and adjust the next_schedule_time.
        @logger.info("#{LOG_KEY}: next_schedule_time = #{next_schedule_time.to_s}")
        next_schedule_time = stall_schedule(next_schedule_time)

      rescue Databasedotcom::SalesForceError => e

        # Session has expired. Force user logout. Then re-authenticate
        if e.message == 'Session expired or invalid'
          @logger.info("#{LOG_KEY}: Session expired or invalid, authenticating again")
          authenticate

          # todo adjust time???
        else
          @logger.error("SFDC: #{e.message}")
        end

      end # rescue / begin
    end # while loop
  end

  # def run


  # This helper method is called whenever initaialize the client object or whenever the
  # client token expires. It will attempt 3 times with a 30 second delay between each retry.

  private
  def authenticate
    @logger.info("#{LOG_KEY}: tyring to authenticate client")
    3.times do |count|
      begin
       # If exception is not thrown, then break out of loop.
        @client.authenticate username: @username, password: @password.value + @security_token.value
        @logger.info("#{LOG_KEY}: client has been authenticated")
        break
      rescue Exception => e
        # Sleep for 30 seconds.
        unless (count == 2)
          @logger.error("#{LOG_KEY}: Failed to authenticate going to try again in 30 seconds")
          sleep(30)
        else
          raise e
        end
      end
    end
  end # def authenticate


  # Given a list of query result that are Sobjects, iterate through the list and grab all the CSV files that each
  # Sobject points to via get_csv_tempfile_list(). Once that is done we save the first LogDate in the list to the
  # @path file. After that we parse the CSV files parse it line by line and generate the events for the parsed CSV
  # line, append it to a list and the finally return the event_list.
  #
  # Note: when grabbing the CSV files, they are stored as Tempfiles and deleted after parsed.

  private
  def create_event_list(query_result_list)
    event_list = []

    # query_result_list is in descending order based on the LogDate, so grab the first one of the list and save the LogDate to @last_read_log_date and .sfdc_info
    # @last_read_log_date = Time.parse(query_result_list.first.LogDate.to_s).utc.iso8601.to_s
    @last_read_log_date = Time.parse(query_result_list.first.LogDate.to_s).utc.iso8601.to_s

    # Overwrite the .sfdc_elf_logstash file with the @last_read_log_date.
    save_last_read_log_date(@last_read_log_date)

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
        event_list << create_event(column, parsed_data)
      end

      # Close tmp file and unlink it, doing this will delete the actual tempfile.
      tmp.close
      tmp.unlink
    end # do loop, tempfile_list

    # Return event_list
    event_list
  end # def create_event_list


  # This helper method takes as input a key data and val data that is in CSV format. Using
  # CSV.parse_line we will get back an array for each then one of them. Then create a new
  # Event object where we will place all of the key value pairs into the Event object and then
  # return it.
  #
  # TODO: event.timestamp     = Time.strptime(values[i], 'YYYYMMddHHmmss.SSS').utc.iso8601

  private
  def create_event(column, data)

    # Initaialize event to be used. @timestamp and @version is automatically added
    event = LogStash::Event.new()

    # Add column data pair to event.
    data.each_index do |i|

      # Grab current key.
      column_name = column[i]

      # Handle when field_name is 'TIMESTAMP', Change the @timestamp field to the actual time on the CSV file, but convert it to iso8601.
      event.timestamp = Time.parse(data[i]).utc.iso8601 if column_name == 'TIMESTAMP' #TODO check to see if we can add mill

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
  # Tempfile will create a unique file each time using 'sfdc_elf' as the beginning and
  # finally we will be returning a list of Tempfile object, where the user can read the
  # Tempfile and then close it and unlink it, which will delete the file.
  #
  # Note: for debugging tmp.path will help find the path where the Tempfile is stored. #

  private
  def get_csv_tempfile_list(query_result_list)
    #todo add try catch
    #todo isolate failes from one another, example large file

    result =[]
    query_result_list.each do |event_log_file|

      # Get the path of the CSV file from the LogFile field, then stream the data to the .write method of the Tempfile
      tmp = Tempfile.new('sfdc_elf_tempfile')
      @client.streaming_download(event_log_file.LogFile, tmp)

      # Flushing will write the buffer into the Tempfile itself.
      tmp.flush

      # Rewind will move the file pointer from the end to the beginning of the file, so that users can simple call the Read method
      tmp.rewind

      # Append the Tempfile object into the result list
      result << tmp

      # @logger.error('MO: Id = '        << event_log_file.Id)
      # @logger.error('MO: EventType = ' << event_log_file.EventType)
      # @logger.error('MO: LogFile = '   << event_log_file.LogFile)
      # @logger.error('MO: LogDate = '   << event_log_file.LogDate.to_s)
    end

    # Return the result list
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


  private
  def stall_schedule(next_schedule_time)
    current_time = Time.now

    @logger.info("#{LOG_KEY}: time before sleep  = #{current_time.to_s}")

    if current_time > next_schedule_time
      @logger.info("#{LOG_KEY}: missed next schedule time, proceeding to next task without sleeping")
      while current_time > next_schedule_time
        next_schedule_time += @interval
      end
    else
      @logger.info("#{LOG_KEY}: sleeping for #{(next_schedule_time - current_time).to_s} seconds")
      sleep(next_schedule_time - current_time)
      next_schedule_time += @interval

    end
    @logger.info("#{LOG_KEY} time after sleep   = #{Time.now.to_s}")

    next_schedule_time
  end # def determine_loop_stall


end # class LogStash::Inputs::File