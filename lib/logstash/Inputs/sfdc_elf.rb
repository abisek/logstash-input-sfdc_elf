# encoding: utf-8
require 'logstash/inputs/base'
require 'logstash/namespace'
require_relative 'sfdc_elf/client_with_streaming_support'
require_relative 'sfdc_elf/queue_util'
require_relative 'sfdc_elf/state_persistor'
require_relative 'sfdc_elf/scheduler'

# This plugin enables Salesforce customers to load EventLogFile(ELF) data from their Force.com orgs. The plugin will
# handle downloading ELF CSV file, parsing them, and handling any schema changes transparently.
class LogStash::Inputs::SfdcElf < LogStash::Inputs::Base
  LOG_KEY        = 'SFDC'
  RETRY_ATTEMPTS = 3

  config_name 'sfdc_elf'
  default :codec, 'plain'

  # TODO: Publish https://www.elastic.co/guide/en/logstash/current/_how_to_write_a_logstash_input_plugin.html#_publishing_to_ulink_url_http_rubygems_org_rubygems_org_ulink?q=publish
  # Username to your Force.com organization.
  config :username, validate: :string, required: true

  # Password to your Force.com organization.
  config :password, validate: :password, required: true

  # Security token to you Force.com organization, can be found in  My Settings > Personal > Reset My Security Token.
  # Then it will take you to "Reset My Security Token" page, and click on the "Reset Security Token" button. The token
  # will be emailed to you.
  # TODO: make make a simple .gif that shows this process? and attach it?
  config :security_token, validate: :password, required: true

  # The path to be use to store the .sfdc_info_logstash file. You set the path like so, `/var/log` Paths must be
  # absolute and cannot be relative.
  config :path, validate: :string, default: Dir.home

  # How often this plugin should grab new data.
  config :poll_interval_in_hours, validate: [*6..24], default: 24 # TODO: defualt is 24hours and do validation in regiter, fail fast if not a positive interger units to mintures


  # The first part of logstash pipeline is register, where all instance variables are initialized.

  public
  def register
    # Do not change id and secret. Currently pointing to "Event Log File Logstash Plugin," a long running app for this
    # plugin.
    @client = ClientWithStreamingSupport.new
    @client.client_id = '3MVG9xOCXq4ID1uGlgyzp8E4HENTnwB05RL1qOmas88eMfE0mk7h0duhs3EnEY2v7Khs9aUXQnrUdB_wm.yJx'
    @client.client_secret = '5847713965780458928'
    @client.version = '33.0'

    # Authenticate the client
    @logger.info("#{LOG_KEY}: tyring to authenticate client")
    @client.retryable_authenticate(username: @username,
                                   password: @password.value + @security_token.value,
                                   retry_attempts: RETRY_ATTEMPTS)
    @logger.info("#{LOG_KEY}: authenticating succeeded") # TODO: move to streaming support class

    # Save org id to distinguish between multiple orgs.
    # @org_id = @client.org_id # TODO: (mo) why doesnt this work???
    @org_id = @client.query('select id from Organization')[0]['Id']

    # Set up time interval for forever while loop.
    @poll_interval_in_hours = @poll_interval_in_hours * 3600
    # @poll_interval_in_hours = 10

    # Handel the @path config passed by the user. If path does not exist then set @path to home directory.
    verify_path

    # Handel parsing the data into event objects and enqueue it to the queue.
    @queue_util = QueueUtil.new()

    # Handel when to schedule the next process based on the @poll_interval_in_hours config.
    @scheduler = Scheduler.new(@poll_interval_in_hours)

    # Handel state of the plugin based on the read and writes of LogDates to the .sdfc_info_logstash file.
    @state_persistor = StatePersistor.new(@path,@org_id)

    # Grab the last indexed log date.
    @last_indexed_log_date = @state_persistor.get_last_indexed_log_date
    @logger.info("#{LOG_KEY}: @last_indexed_log_date =  #{@last_indexed_log_date}")
  end  # def register




  # The second stage of Logstash pipeline is run, where it expects to parse your data into event objects and then pass
  # it into the queue to be used in the rest of the pipeline.

  public
  def run(queue)
    @scheduler.schedule do
      # Line for readable log statements.
      @logger.info('---------------------------------------------------')

      # Grab a list of SObjects, specifically EventLogFiles.
      soql_expr = "SELECT Id, EventType, Logfile, LogDate, LogFileLength
                   FROM EventLogFile
                   WHERE LogDate > #{@last_indexed_log_date} ORDER BY LogDate DESC "

      query_result_list = @client.retryable_query(username: @username,
                                                  password: @password.value + @security_token.value,
                                                  retry_attempts: RETRY_ATTEMPTS,
                                                  soql_expr: soql_expr)

      @logger.info("#{LOG_KEY}: query result size = #{query_result_list.size}")

      if !query_result_list.empty?
        # query_result_list is in descending order based on the LogDate, so grab the first one of the list and save the
        # LogDate to @last_read_log_date and .sfdc_info_logstash
        @last_indexed_log_date = query_result_list.first.LogDate.strftime('%FT%T.%LZ')

        # TODO: grab tempfiles here!!

        # Overwrite the .sfdc_info_logstash file with the @last_read_log_date.
        # Note: we currently do not support deduplication, but will implement it soon. TODO: need to implement deduplication
        # TODO: might have to move this after enqueue_events(), in case of a crash in between.
        @state_persistor.update_last_indexed_log_date(@last_indexed_log_date) # TODO: can do all @state_persistor calls after the if statement

        # Creates events from query_result_list, then simply append the events to the queue.
        @queue_util.enqueue_events(query_result_list, queue, @client)
      else
        # Make sure to save the last read LogDate even when query_result_list is empty
        @state_persistor.update_last_indexed_log_date(DateTime.now.new_offset(0).strftime('%FT%T.%LZ'))
      end

    end # do loop
  end # def run




  # Handel the @path variable passed by the user. If path does not exist then set @path to home directory.

  private
  def verify_path
    # Check if the path exist, if not then set @path to home directory.
    unless File.directory?(@path)
      @logger.warn("#{LOG_KEY}: provided path does not exist or is invalid. path=#{@path}")
      @path = Dir.home
    end
    @logger.info("#{LOG_KEY}: path = #{@path}")
  end

end # class LogStash::inputs::File
