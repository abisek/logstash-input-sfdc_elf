# encoding: utf-8
require 'logstash/inputs/base'
require 'logstash/namespace'

require 'stud/interval'
require 'csv'
require_relative '../../logstash/Inputs/client_with_streaming_support'

class LogStash::Inputs::SfdcElf < LogStash::Inputs::Base

  # Constants
  SEPARATOR = ','
  QUOTE_CHAR = '"'
  DEFAULT_TIME = '0001-01-01T00:00:00Z'

  config_name 'sfdc_elf'
  default :codec, 'plain'

  config :username, :validate => :string, :required => true
  config :password, :validate => :string, :required => true       # todo might need to change validate to :password
  config :security_token, :validate => :string, :required => true # todo might need to change validate to :password
  config :sfdc_info_path, :validate => :string, :default => Dir.home
  config :poll_interval_in_hours, :validate => :number, :default => 24


  public
  def register

    # Initaialize client
    @client = ClientWithStreamingSupport.new
    @client.client_id = '3MVG9xOCXq4ID1uGlgyzp8E4HEHfzI4iryotXS3FtHQIZ5VYhE8.JPehyksO.uYZmZHct.xlXVxqDCih35j0.'
    @client.client_secret = '7829455833495769170'
    @client.version = '33.0'

    # Authenticate the client
    authenticate

    # Set @sfdc_info_path to home directory if provided path does not exist.
    @sfdc_info_path = Dir.home unless File.directory?(@sfdc_info_path)

    @path = "#{@sfdc_info_path}/.sfdc_info"

    if File.exist?(@path)
      #todo add logging at crital points via info

      @last_read_instant = File.read(@path)
      @logger.error('MO: exist')
    else
      @last_read_instant = DEFAULT_TIME

      @logger.error('MO: does NOT exist')
      f = File.open(@path, 'w')
      f.write(@last_read_instant)
      f.flush
      f.close
    end

  end # def register




  public
  def run(queue)
    begin

      # (0..1).each do

        current_time = Time.now.utc.iso8601.to_s

        # if @last_read_instant < current_time # compare if its the same day or compare against poll_interval_in_hours

          # Grab a list of Sobjects, specifically EventLogFiles.
          query_result = @client.query("SELECT id, eventtype, logfile FROM EventLogFile WHERE LogDate > #{@last_read_instant} ORDER BY LogDate ASC ")
          @last_read_instant = current_time

          if query_result.empty? #TODO Should I logger this?? or move it up for query_result
            @logger.info('MO: query result is empty')
            # next
          end

          f = File.open(@path, 'w')
          f.write(@last_read_instant)
          f.flush
          f.close

          # Grab a list of Tempfiles that contains CSV file data.
          tempfile_list = get_csv_files(query_result)

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
          end

        # end

      # end # do loop


    rescue Databasedotcom::SalesForceError => e

      # Session has expired. Force user logout. Then re-authenticate
      if e.message == 'Session expired or invalid'
        authenticate
      else
        raise e
      end

    end # rescue / begin
  end



  #
  # This helper method is called whenever initaialize the client object or whenever the
  # client token expires.
  #

  private
  def authenticate
    @client.authenticate username: @username, password: @password + @security_token
  end



  #
  # This helper method takes as input a key data and val data that is in CSV format. Using
  # CSV.parse_line we will get back an array for each then one of them. Then create a new
  # Event object where we will place all of the key value pairs into the Event object and then
  # return it.
  #
  # TODO: event.timestamp     = Time.strptime(values[i], 'YYYYMMddHHmmss.SSS').utc.iso8601
  #

  private
  def create_event(column, data)

    # Initaialize event to be used. @timestamp and @version is automatically added
    event = LogStash::Event.new()

    # Add key value pair to event
    data.each_index do |i|

      # Grab current key
      column_name = column[i]

      # Handle when field_name is 'TIMESTAMP', otherwise simply add the key value pair
      # Change the @timestamp field to the actual time on the CSV file, but convert it to iso8601
      event.timestamp = Time.parse(data[i]).utc.iso8601 if column_name == 'TIMESTAMP'

      event[column_name] = data[i]
    end

    event
  end



  #
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
  # Note: for debugging tmp.path will help find the path where the Tempfile is stored.
  #

  private
  def get_csv_files(csv_path_list) #todo add try catch
    #todo isolate failes from one another, example large file

    result =[]
    csv_path_list.each do |csv_path|

      # Create Tempfile and write the body of the http_result, which contains the csv data, to a buffer.
      tmp = Tempfile.new('sfdc_elf')

      # Get the csv_path from the LogFile field, then do http get
      @client.streaming_download(csv_path.LogFile, tmp)

      # Flushing will write the buffer into the Tempfile itself.
      tmp.flush

      # Rewind will move the file pointer from the end to the beginning of the file, so that users can simple call the Read method
      tmp.rewind

      # Append the Tempfile object into the result list
      result << tmp
    end

    # Return the result list
    result

  end

end # class LogStash::Inputs::File