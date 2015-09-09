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

  # Username to your Force.com organization.
  config :username, validate: :string, required: true

  # Password to your Force.com organization.
  config :password, validate: :password, required: true

  # Client id to your Force.com organization.
  config :client_id, validate: :password, required: true

  # Client secret to your Force.com organization.
  config :client_secret, validate: :password, required: true

  # The host to use for OAuth2 authentication.
  config :host, validate: :string, default: 'login.salesforce.com'

  # Only needed when your Force.com organization requires it.
  # Security token to you Force.com organization, can be found in  My Settings > Personal > Reset My Security Token.
  # Then it will take you to "Reset My Security Token" page, and click on the "Reset Security Token" button. The token
  # will be emailed to you.
  config :security_token, validate: :password, default: ''

  # The path to be use to store the .sfdc_info_logstash state persistor file. You set the path like so, `~/SomeDirectory` Paths must be
  # absolute and cannot be relative.
  config :path, validate: :string, default: Dir.home

  # Specify how often the plugin should grab new data in terms of minutes.
  config :poll_interval_in_minutes, validate: [*1..(24 * 60)], default: (24 * 60)


  # The first part of logstash pipeline is register, where all instance variables are initialized.

  public
  def register
    # Initialize the client.
    @client = ClientWithStreamingSupport.new
    @client.client_id     = @client_id.value
    @client.client_secret = @client_secret.value
    @client.host          = @host
    @client.version       = '33.0'

    # Authenticate the client
    @logger.info("#{LOG_KEY}: tyring to authenticate client")
    @client.retryable_authenticate(username: @username,
                                   password: @password.value + @security_token.value,
                                   retry_attempts: RETRY_ATTEMPTS)
    @logger.info("#{LOG_KEY}: authenticating succeeded")

    # Save org id to distinguish between multiple orgs.
    @org_id = @client.query('select id from Organization')[0]['Id']

    # Set up time interval for forever while loop.
    @poll_interval_in_seconds = @poll_interval_in_minutes * 60

    # Handel the @path config passed by the user. If path does not exist then set @path to home directory.
    verify_path

    # Handel parsing the data into event objects and enqueue it to the queue.
    @queue_util = QueueUtil.new

    # Handel when to schedule the next process based on the @poll_interval_in_hours config.
    @scheduler = Scheduler.new(@poll_interval_in_seconds)

    # Handel state of the plugin based on the read and writes of LogDates to the .sdfc_info_logstash file.
    @state_persistor = StatePersistor.new(@path, @org_id)

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
      soql_expr = "SELECT Id, EventType, Logfile, LogDate, LogFileLength, LogFileFieldTypes
                   FROM EventLogFile
                   WHERE LogDate > #{@last_indexed_log_date} ORDER BY LogDate ASC "


      query_result_list = @client.retryable_query(username: @username,
                                                  password: @password.value + @security_token.value,
                                                  retry_attempts: RETRY_ATTEMPTS,
                                                  soql_expr: soql_expr)

      @logger.info("#{LOG_KEY}: query result size = #{query_result_list.size}")

      if !query_result_list.empty?
        # query_result_list is in ascending order based on the LogDate, so grab the last one of the list and save the
        # LogDate to @last_read_log_date and .sfdc_info_logstash
        @last_indexed_log_date = query_result_list.last.LogDate.strftime('%FT%T.%LZ')

        # TODO: grab tempfiles here!!

        # Overwrite the .sfdc_info_logstash file with the @last_read_log_date.
        # Note: we currently do not support deduplication, but will implement it soon.
        # TODO: need to implement deduplication
        # TODO: might have to move this after enqueue_events(), in case of a crash in between.
        # TODO: can do all @state_persistor calls after the if statement
        @state_persistor.update_last_indexed_log_date(@last_indexed_log_date)

        # Creates events from query_result_list, then simply append the events to the queue.
        @queue_util.enqueue_events(query_result_list, queue, @client)
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
