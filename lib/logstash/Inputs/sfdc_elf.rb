# encoding: utf-8
require 'logstash/inputs/base'
require 'logstash/namespace'

require 'stud/interval'
require 'csv'
require_relative '../../logstash/Inputs/client_with_streaming_support'

# TODO describe what class does
class LogStash::Inputs::SfdcElf < LogStash::Inputs::Base

  # Constants
  SEPARATOR = ','
  QUOTE_CHAR = '"'
  DEFAULT_TIME = '0001-01-01T00:00:00Z'

  config_name 'sfdc_elf'
  default :codec, 'plain'

  config :username, :validate => :string, :required => true
  config :password, :validate => :password, :required => true       # todo might need to change validate to :password
  config :security_token, :validate => :password, :required => true # todo might need to change validate to :password
  config :sfdc_info_path, :validate => :string, :default => Dir.home
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

    # Set up time interval for forever while loop
    # @interval = @poll_interval_in_hours * 3600
    @interval = 60

    # Set @sfdc_info_path to home directory if provided path from config does not exist.
    @sfdc_info_path = Dir.home unless File.directory?(@sfdc_info_path)


    # Append .sfdc_info to the @sfdc_info_path.
    @path = "#{@sfdc_info_path}/.sfdc_info"

    # Read from .sfdc_info if it exists, otherwise create it.
    if File.exist?(@path)

      # Load last read LogDate from .sfdc_info.
      @last_read_instant = File.read(@path)
      @logger.info('MO: .sfdc_info does exist and read from it to @last_read_instant')
    else

      # Load default time to ensure getting all possible EventLogFiles from oldest to current.
      # Note in run() .sfdc_info is created or overwritten, so no need to create it here.
      @last_read_instant = DEFAULT_TIME

      @logger.info('MO: .sfdc_info does not exist and loaded DEFAULT_TIME to @last_read_instant')
    end

    @logger.info('MO: @last_read_instant =  ' << @last_read_instant)
  end # def register



  public
  def run(queue)
    begin

      # next_schedule_time = Time.now
      next_schedule_time = Time.now  + @interval

      while true

        # Grab a list of Sobjects, specifically EventLogFiles.
        query_result_list = @client.query("SELECT Id, EventType, Logfile, LogDate FROM EventLogFile WHERE LogDate > #{@last_read_instant} ORDER BY LogDate DESC ")

        if !query_result_list.empty?
          @logger.info('MO: query result is NOT empty, size = ' << query_result_list.size.to_s)

          #
          @last_read_instant = Time.parse(query_result_list.first.LogDate.to_s).utc.iso8601.to_s

          f = File.open(@path, 'w')
          f.write(@last_read_instant)
          f.flush
          f.close

          # Grab a list of Tempfiles that contains CSV file data.
          tempfile_list = get_csv_files(query_result_list)

          tempfile_list.each do |tmp|

            # Get the column from Tempfile, which is in the first line and in CSV format, then parse it. It will return an array.
            column = CSV.parse_line(tmp.readline, :col_sep => SEPARATOR, :quote_char => QUOTE_CHAR)

            tmp.each_line do |data|
              # Parse the current line, it will return an array.
              parsed_data = CSV.parse_line(data, :col_sep => SEPARATOR, :quote_char => QUOTE_CHAR)

              # create_event will return a event object.
              queue << create_event(column, parsed_data)
            end

            # Close tmp file and unlink it, doing this will delete the actual tempfile.
            tmp.close
            tmp.unlink
          end # do

        else
          @logger.info('MO: query result is empty')
        end

        # Stall ...
        @logger.info('MO: next_schedule_time = ' << next_schedule_time.to_s)
        next_schedule_time = determine_loop_stall(next_schedule_time)

      end # while loop

    rescue Databasedotcom::SalesForceError => e

      # Session has expired. Force user logout. Then re-authenticate
      if e.message == 'Session expired or invalid'
        authenticate
        @logger.info('MO: SalesForceError, authenticating again')
      else
        @logger.warn('MO: '  << e.message)
        raise e
      end

    end # rescue / begin
  end # def run




  # This helper method is called whenever initaialize the client object or whenever the
  # client token expires.

  private
  def authenticate
    @client.authenticate username: @username, password: @password.value + @security_token.value
    @logger.info('MO: client has been authenticated')
  end # def authenticate




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
      event.timestamp = Time.parse(data[i]).utc.iso8601 if column_name == 'TIMESTAMP'

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
  def get_csv_files(query_result_list)
    #todo add try catch
    #todo isolate failes from one another, example large file

    result =[]
    query_result_list.each do |event_log_file|

      # Create Tempfile and write the body of the http_result, which contains the csv data, to a buffer.
      tmp = Tempfile.new('sfdc_elf')

      # Get the path of the CSV file from the LogFile field, then do http get.
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


  private
  def determine_loop_stall(next_schedule_time)
    current_time = Time.now

    @logger.info('MO: time before sleep = ' << current_time.to_s)

    if current_time > next_schedule_time
      @logger.info('MO: missed next schedule time, proceeding to next task without sleeping')
      while current_time > next_schedule_time
        next_schedule_time += @interval
      end
    else
      @logger.info('MO: sleeping for ' << (next_schedule_time - current_time).to_s)
      sleep(next_schedule_time - current_time)
      next_schedule_time += @interval

    end
    @logger.info('MO: time after sleep = ' << Time.now.to_s)

    next_schedule_time
  end # def determine_loop_stall




end # class LogStash::Inputs::File