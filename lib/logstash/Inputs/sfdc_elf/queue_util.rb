# encoding: utf-8
require 'csv'

# Handel parsing data into event objects and then enqueue all of the events to the queue.
class QueueUtil
  # Constants
  LOG_KEY        = 'SFDC - QueueUtil'
  SEPARATOR      = ','
  QUOTE_CHAR     = '"'

  # Zip up the tempfile, which is a CSV file, and the field types, so that when parsing the CSV file we can accurately
  # convert each field to its respective type. Like Integers and Booleans.
  EventLogFile = Struct.new(:field_types, :temp_file)


  def initialize
    @logger = Cabin::Channel.get(LogStash)
  end




  # Given a list of query result's, iterate through it and grab the CSV file associated with it. Then parse the CSV file
  # line by line and generating the event object for it. Then enqueue it.

  public
  def enqueue_events(query_result_list, queue, client)
    @logger.info("#{LOG_KEY}: enqueue events")

    # Grab a list of Tempfiles that contains CSV file data.
    event_log_file_records = get_event_log_file_records(query_result_list, client)

    # Iterate though each record.
    event_log_file_records.each do |elf|
      begin
        # Create local variable to simplify & make code more readable.
        tmp = elf.temp_file

        # Get the schema from the first line in the tempfile. It will be in CSV format so we parse it, and it will
        # return an array.
        schema = CSV.parse_line(tmp.readline, col_sep: SEPARATOR, quote_char: QUOTE_CHAR)

        # Loop through tempfile, line by line.
        tmp.each_line do |line|
          # Parse the current line, it will return an string array.
          string_array = CSV.parse_line(line, col_sep: SEPARATOR, quote_char: QUOTE_CHAR)

          # Convert the string array into its corresponding type array.
          data = string_to_type_array(string_array, elf.field_types)

          # create_event will return a event object.
          queue << create_event(schema, data)
        end
      ensure
        # Close tmp file and unlink it, doing this will delete the actual tempfile.
        tmp.close
        tmp.unlink
      end
    end # do loop, tempfile_list
  end # def create_event_list




  # Convert the given string array to its corresponding type array and return it.

  private
  def string_to_type_array(string_array, field_types)
    data = []

    field_types.each_with_index do |type, i|
      case type
        when 'Number'
          data[i] = (string_array[i].empty?) ? nil : string_array[i].to_f
        when 'Boolean'
          data[i] = (string_array[i].empty?) ? nil : (string_array[i] == '0')
        when 'IP'
          data[i] = (string_array[i].empty? || string_array[i] == 'N/A') ? nil : string_array[i]
        else # 'String', 'Id', 'EscapedString', 'Set'
          data[i] = (string_array[i].empty?) ? nil : string_array[i]
      end
    end # do loop

    data
  end # convert_string_to_type




  # Bases on the schema and data, we create the event object. At any point if the data is nil we simply dont add
  # the data to the event object. Special handling is needed when the schema 'TIMESTAMP' occurs, then the data
  # associated with it needs to be converted into a LogStash::Timestamp.
  #
  # Patch: Whenever USER_AGENT it in the schema I simply dont add it to the event object because right now
  #        it's typing its not standardized to a single type.
  #        TODO: Need a better way to handle user agent typing.

  private
  def create_event(schema, data)
    # Initaialize event to be used. @timestamp and @version is automatically added
    event = LogStash::Event.new

    # Add column data pair to event.
    data.each_index do |i|
      # Grab current key.
      schema_name = schema[i]

      # Handle when field_name is 'TIMESTAMP', Change the @timestamp field to the actual time on the CSV file,
      # but convert it to iso8601.
      if schema_name == 'TIMESTAMP'
        epochmillis = DateTime.parse(data[i]).to_time.to_f
        event.timestamp = LogStash::Timestamp.at(epochmillis)
      end

      # Add the schema data pair to event object.
      if data[i] != nil && schema[i] != 'USER_AGENT'
        event[schema_name] = data[i]
      end
    end

    # Return the event
    event
  end # def create_event




  # This helper method takes as input a list/collection of SObjects which each contains a path to their respective CSV
  # files. The path is stored in the LogFile field. Using that path, we are able to grab the actual CSV file via
  # @client.http_get method.
  #
  # After grabbing the CSV file we then store them using the standard Tempfile library. Tempfile will create a unique
  # file each time using 'sfdc_elf_tempfile' as the prefix and finally we will be returning a list of Tempfile object,
  # where the user can read the Tempfile and then close it and unlink it, which will delete the file.

  public
  def get_event_log_file_records(query_result_list, client)
    @logger.info("#{LOG_KEY}: generating tempfile list")
    result = []
    query_result_list.each do |event_log_file|
      # Get the path of the CSV file from the LogFile field, then stream the data to the .write method of the Tempfile
      tmp = Tempfile.new('sfdc_elf_tempfile')
      client.streaming_download(event_log_file.LogFile, tmp)

      # Flushing will write the buffer into the Tempfile itself.
      tmp.flush

      # Rewind will move the file pointer from the end to the beginning of the file, so that users can simple
      # call the Read method.
      tmp.rewind

      # Append the EventLogFile object into the result list
      field_types = event_log_file.LogFileFieldTypes.split(',')
      result << EventLogFile.new(field_types, tmp)

      # Log the info from event_log_file object.
      @logger.info("  #{LOG_KEY}: Id = #{event_log_file.Id}")
      @logger.info("  #{LOG_KEY}: EventType = #{event_log_file.EventType}")
      @logger.info("  #{LOG_KEY}: LogFile = #{event_log_file.LogFile}")
      @logger.info("  #{LOG_KEY}: LogDate = #{event_log_file.LogDate}")
      @logger.info("  #{LOG_KEY}: LogFileLength = #{event_log_file.LogFileLength}")
      @logger.info("  #{LOG_KEY}: LogFileFieldTypes = #{event_log_file.LogFileFieldTypes}")
      @logger.info('  ......................................')
    end
    result
  end # def get_event_log_file_records
end # QueueUtil
