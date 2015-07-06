# encoding: utf-8
require 'logstash/inputs/base'
require 'logstash/namespace'

require 'stud/interval'
require 'databasedotcom'
require 'csv'
# require 'time'
# require 'tempfile'
# require 'digest/md5' #todo why dont i need to require these's?? is it becasuse of :: thingy??

class LogStash::Inputs::Sfdc_elf < LogStash::Inputs::Base

  # Constants
  SEPARATOR = ','
  QUOTE_CHAR = '"'

  config_name 'sfdc_elf'
  default :codec, 'plain'

  config :username, :validate => :string, :required => true
  config :password, :validate => :string, :required => true       # todo need to change validate to :password
  config :security_token, :validate => :string, :required => true # todo need to change validate to :password
  config :poll_interval_in_hours, :validate => :number, :default => 24


  public
  def register

    # Initaialize client
    @client = Databasedotcom::Client.new
    @client.client_id = '3MVG9xOCXq4ID1uGlgyzp8E4HEHfzI4iryotXS3FtHQIZ5VYhE8.JPehyksO.uYZmZHct.xlXVxqDCih35j0.'
    @client.client_secret = '7829455833495769170'
    @client.version = '33.0'

    # Authenticate the client
    authenticate

    # Done list will contain all CSV files that have been read before, prevent duplication of data.
    # This is done using MD5 hash, where it parases a file and generates a unique hexadecimal value for it.
    @done_list = []  #todo load done_list from .db if there is one....

  end # def register

  public
  def run(queue)
    begin

      (0..2).each do

        # Grab a list of Sobjects, specifically EventLogFiles
        query_result = @client.query('select id, eventtype, logfile from EventLogFile')

        # Grab a list of Tempfiles that contains CSV file data
        tempfile_list = get_csv_files(query_result)

        if tempfile_list.empty? #TODO Should I logger this?? or move it up for query_result
          @logger.error('MO: no csv files!!')
        end

        tempfile_list.each do |tmp|
          if !has_been_read_before(tmp)
            # Break CSV file into to two parts, key and value, which is stored in an array
            key_value_arry = tmp.readlines.to_a

            # Close tmp file and unlink it, doing this will delete the actual tempfile
            tmp.close
            tmp.unlink

            # Pass both data to csv_filter and it will return an event
            queue << csv_filter(key_value_arry[0], key_value_arry[1])
          end
        end

      end # do loop


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

  def csv_filter(key_data, val_data)

    # Initaialize event to be used. @timestamp and @version is automatically added
    event = LogStash::Event.new()

    begin
      # Parse the key and value data into an array
      keys    = CSV.parse_line(key_data, :col_sep => SEPARATOR, :quote_char => QUOTE_CHAR)
      values  = CSV.parse_line(val_data, :col_sep => SEPARATOR, :quote_char => QUOTE_CHAR)

      # Add key value pair to event
      values.each_index do |i|

        # Grab current key
        field_name = keys[i]

        # Handle when field_name is 'TIMESTAMP', otherwise simply add the key value pair
        if field_name == 'TIMESTAMP'

          # Add 'TIMESTAMP' key and map to the current value which is the actual time on the CSV file
          # in this format 'YYYYMMddHHmmss.SSS'
          event[field_name]   = values[i]

          # Change the @timestamp field to the actual time on the CSV file, but convert it to iso8601
          event.timestamp     = Time.parse(values[i]).utc.iso8601

        else
          # Add key value to event
          event[field_name] = values[i]
        end
      end

      event

    rescue => e
      event.tag '_csvparsefailure' #TODO: do I need this try catch??
      return
    end # begin
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

  def get_csv_files(csv_path_list)

    result =[]

    csv_path_list.each do |csv_path|

      # Get the csv_path from the LogFile field, then do http get
      http_result = @client.http_get(csv_path.LogFile)

      # Create Tempfile and write the body of the http_result, which contains the csv data, to a buffer.
      tmp = Tempfile.new('sfdc_elf')
      tmp.write(http_result.body)

      # Flushing will write the buffer into the Tempfile itself.
      tmp.flush

      # Rewind will move the file pointer from the end to the beginning of the file, so that users can simple call
      # the Read methods without having to called rewind each time.
      tmp.rewind

      # Append the Tempfile object into the result list
      result << tmp
    end

    # Return the result list
    result

  end


  def has_been_read_before(tmp)
    md5 = Digest::MD5.new
    md5.update(tmp.read)
    hex = md5.hexdigest

    if @done_list.member?(hex)
      @logger.error('MO: this csv file has been read before')
      tmp.close
      tmp.unlink
      true
    else
      @done_list << hex
      tmp.rewind
      false
    end
  end

end # class LogStash::Inputs::File