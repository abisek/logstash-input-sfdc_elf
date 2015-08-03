# Handel what the next procedure should be based on the .sfdc_info_logstash file. States proceed via reading and
# writing LogDates to the .sfdc_info_logstash file.

class StatePersistor

  LOG_KEY        = 'SFDC - StatePersistor'
  FILE_PREFIX    = 'sfdc_info_logstash'
  DEFAULT_TIME   = '0001-01-01T00:00:00Z'

  def initialize(base_path, org_id) #base_path
    @logger = Cabin::Channel.get(LogStash)
    @path_with_file_name = "#{base_path}/.#{FILE_PREFIX}_#{org_id}"
  end


  # Read the last indexed LogDate from .sfdc_info_logstash file and return it. If the .sfdc_info_logstash file does
  # not exist then create the file and write DEFAULT_TIME to it using update_last_indexed_log_date() method.

  public
  def get_last_indexed_log_date
    # Read from .sfdc_info_logstash if it exists, otherwise load @last_read_log_date with DEFAULT_TIME.
    if File.exist?(@path_with_file_name)
      # Load last read LogDate from .sfdc_info_logstash.
      @logger.info("#{LOG_KEY}: .#{@path_with_file_name} does exist, read time from it and stored it in @last_read_instant")
      File.read(@path_with_file_name)
    else
      # Load default time to ensure getting all possible EventLogFiles from oldest to current. Also
      # create .sfdc_info_logstash file
      @logger.info("#{LOG_KEY}: .sfdc_info_logstash does not exist and loaded DEFAULT_TIME to @last_read_instant")
      update_last_indexed_log_date(DEFAULT_TIME)
      DEFAULT_TIME
    end
  end




  # Take as input a date sting that is in iso8601 format, then overwrite .sfdc_info_logstash with the date string,
  # because of the 'w' flag used with the File class.

  public
  def update_last_indexed_log_date(date)
    @logger.info("#{LOG_KEY}: overwriting #{@path_with_file_name} with #{date}")
    f = File.open(@path_with_file_name, 'w')
    f.write(date)
    f.flush
    f.close
  end
end