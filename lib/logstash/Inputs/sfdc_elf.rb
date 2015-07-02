# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"

require "stud/interval"
require "databasedotcom"
require "csv"
require "time"

class LogStash::Inputs::Sfdc_elf < LogStash::Inputs::Base

  # Constants
  VALUE = 'value'
  KEY = 'key'
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

  end # def register

  public
  def run(queue)
    begin

      # List of EventLogFiles
      query_result = @client.query('select id, eventtype, logfile from EventLogFile')

      if !query_result.empty?

        # Get the first LogFile
        log_file_path = query_result.first.LogFile

        # Get csv file from a LogFile, which is located in the body of http request
        http_result = @client.http_get(log_file_path)
        kvArray = http_result.body.lines.to_a

        # Pass data to filter and it will return an event
        queue << csv_filter(kvArray[0], kvArray[1])

      end


    rescue Databasedotcom::SalesForceError => e

      # Session has expired. Force user logout. Then re-authenticate
      if e.message == "Session expired or invalid"
        authenticate
      else
        raise e
      end

    end # rescue / begin
  end # def run



  def authenticate
    @client.authenticate username: @username, password: @password + @security_token
  end # def authenticate



  def csv_filter(key_data, val_data)

    # Initaialize event to be used. @timestamp and @version is automatically added
    event = LogStash::Event.new()

    begin
      # Parse the key and value data into an array
      keys    = CSV.parse_line(key_data, :col_sep => SEPARATOR, :quote_char => QUOTE_CHAR)
      values  = CSV.parse_line(val_data, :col_sep => SEPARATOR, :quote_char => QUOTE_CHAR)

      # Create key value pair
      values.each_index do |i|
        field_name = keys[i]

        # Format @timestamp field to iso8601 when TIMESTAMP is up, otherwise leave everything else as is
        if field_name == 'TIMESTAMP'

          # event.timestamp     = Time.strptime(values[i], 'YYYYMMddHHmmss.SSS').utc.iso8601
          event[field_name]   = Time.parse(values[i]).utc.iso8601
          event[field_name]   = values[i]
        else
          event[field_name] = values[i]
        end
      end

      event

    rescue => e
      event.tag "_csvparsefailure"
      return
    end # begin
  end # def csvFilter



  # def get_csv_files(list)
  #
  #   # Get the first LogFile
  #   log_file_path = list.first.LogFile
  #
  #   # Get csv file from a LogFile, which is located in the body of http request
  #   http_result = @client.http_get(log_file_path)
  #
  #
  # end # def get_csv_files






end # class LogStash::Inputs::File