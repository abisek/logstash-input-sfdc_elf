# encoding: utf-8
require 'csv'

# Handel parsing data into event objects and then enqueue all of the events to the queue.
class QueueUtil
  # Constants
  LOG_KEY        = 'SFDC - QueueUtil'
  SEPARATOR      = ','
  QUOTE_CHAR     = '"'

  # TODO: comment
  EventLogFile = Struct.new(:field_types, :temp_file)

  def initialize
    @logger = Cabin::Channel.get(LogStash)
  end




  # Given a list of query result that are SObjects, iterate through the list and grab all the CSV files that each
  # SObject points to via get_csv_tempfile_list(). Once that is done we save the first LogDate in the list to the @path
  # file. After that we parse the CSV files parse it line by line and generate the events for the parsed CSV line,
  # append it to the queue.
  #
  # Note: when grabbing the CSV files, they are stored as Tempfiles and deleted after parsed.

  public
  def enqueue_events(query_result_list, queue, client)
    @logger.info("#{LOG_KEY}: enqueue events")

    # Grab a list of Tempfiles that contains CSV file data.
    tempfile_list = get_csv_tempfile_list(query_result_list, client)

    # Loop though each tempfile.
    tempfile_list.each do |elf|
      begin
        tmp = elf.temp_file
        # Get the column from Tempfile, which is in the first line and in CSV format, then parse it.
        # It will return an array.
        column = CSV.parse_line(tmp.readline, col_sep: SEPARATOR, quote_char: QUOTE_CHAR)

        # Loop through tempfile, line by line.
        tmp.each_line do |line|
          # Parse the current line, it will return an string array.
          string_array = CSV.parse_line(line, col_sep: SEPARATOR, quote_char: QUOTE_CHAR)

          data = string_to_type_array(string_array, elf.field_types)
          # create_event will return a event object.
          queue << create_event(column, data)
        end
      ensure
        # Close tmp file and unlink it, doing this will delete the actual tempfile.
        tmp.close
        tmp.unlink
      end
    end # do loop, tempfile_list
  end # def create_event_list



  private
  def string_to_type_array(string_array, field_types)
    data = []

    field_types.each_with_index do |type, i|
      case type
        when 'Number'
          data[i] = (string_array[i].empty?) ? nil : string_array[i].to_f
        when 'Boolean'
          data[i] = (string_array[i].empty?) ? nil : (string_array[i] == '0')
        else # 'String', 'Id', 'EscapedString', 'Set', 'IP'
          data[i] = (string_array[i].empty?) ? nil : string_array[i]
          # data << string_array[i]
      end
    end # do loop

    data
  end # convert_string_to_type


  # This helper method takes as input a key data and val data that is in CSV format. Using CSV.parse_line we will get
  # back an array for each then one of them. Then create a new Event object where we will place all of the key value
  # pairs into the Event object and then return it.

  private
  def create_event(column, data) # TODO: change to schema, data
    # Initaialize event to be used. @timestamp and @version is automatically added
    event = LogStash::Event.new

    # Add column data pair to event.
    data.each_index do |i|
      # Grab current key.
      column_name = column[i]

      # Handle when field_name is 'TIMESTAMP', Change the @timestamp field to the actual time on the CSV file,
      # but convert it to iso8601.
      if column_name == 'TIMESTAMP'
        epochmillis = DateTime.parse(data[i]).to_time.to_f
        event.timestamp = LogStash::Timestamp.at(epochmillis)
      end

      # Add the column data pair to event object.
      if data[i] != nil
        event[column_name] = data[i]
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
  #
  # Note: for debugging tmp.path will help find the path where the Tempfile is stored.

  public
  def get_csv_tempfile_list(query_result_list, client)
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

      # Append the Tempfile object into the result list
      # result << tmp
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
  end # def get_csv_files
end # QueueUtil
