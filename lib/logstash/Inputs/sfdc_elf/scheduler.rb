# encoding: utf-8

# Handel when to schedule the next process based on the poll interval specified. The poll interval provided has to be
# in seconds.
class Scheduler
  LOG_KEY = 'SFDC - Scheduler'

  def initialize(poll_interval_in_seconds)
    @logger = Cabin::Channel.get(LogStash)
    @poll_interval_in_seconds = poll_interval_in_seconds
  end




  # In a forever loop, run the block provided then sleep based on the poll interval and repeat.

  public
  def schedule(&block)
    # Grab the current time and one @interval to it so that the while loop knows when it need to compute again.
    next_schedule_time = Time.now + @poll_interval_in_seconds

    # sleep until start time
    loop do
      block.call

      # Depending on the next_schedule_time and the time taking the compute the code above,
      # sleep this loop and adjust the next_schedule_time.
      @logger.info("#{LOG_KEY}: next_schedule_time = #{next_schedule_time}")
      next_schedule_time = stall_schedule(next_schedule_time)
    end
  end




  # Given as input the next schedule time, stall_schedule() will decide if we need to sleep until the next
  # schedule time or skip sleeping because of missing the next schedule time.
  #
  # For both examples, the time interval is 1 hour.
  # Example 1:
  # started time       = 1:00pm
  # next_schedule_time = 2:00pm
  # current_time       = 1:30pm
  # In this example you will need to sleep for 30 mins, so you will be on schedule.
  #
  # Example 2:
  # started time       = 1:00pm
  # next_schedule_time = 2:00pm
  # current_time       = 2:30pm
  # In this example you will not be allowed to sleep, and will proceed to compute again since you missed the
  # schedule time.

  public
  def stall_schedule(next_schedule_time)
    current_time = Time.now
    @logger.info("#{LOG_KEY}: time before sleep  = #{current_time}")

    # Example 2 case from above.
    if current_time > next_schedule_time
      @logger.info("#{LOG_KEY}: missed next schedule time, proceeding to next task without sleeping")
      next_schedule_time += @poll_interval_in_seconds while current_time > next_schedule_time

      # Example 1 case from above.
    else
      @logger.info("#{LOG_KEY}: sleeping for #{(next_schedule_time - current_time)} seconds")
      sleep(next_schedule_time - current_time)
      next_schedule_time += @poll_interval_in_seconds
    end
    @logger.info("#{LOG_KEY} time after sleep   = #{Time.now}")
    next_schedule_time
  end # def determine_loop_stall
end # Scheduler
