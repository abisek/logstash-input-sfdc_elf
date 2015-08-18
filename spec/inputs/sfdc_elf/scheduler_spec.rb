require_relative '../../spec_helper'

describe Scheduler do
  let(:hour_interval) { hour_interval = 3600 }

  after do
    Timecop.return
  end

  describe '#stall_schedule' do
    # Precondition:
    #               - none
    it 'Missed the next scheduled time' do
      # Start time is 1:00pm.
      start_time = Time.local(2015, 9, 1, 13, 00, 0)

      # Freeze the time at the current time, which is 2:30pm.
      current_time = Time.local(2015, 9, 1, 14, 30, 0)
      Timecop.freeze(current_time)

      # Set the interval in to every hour.
      scheduler = Scheduler.new(hour_interval)

      # The next schedule time should be based on the start time + the interval, which would be 2:00pm.
      next_schedule_time = start_time + hour_interval

      # Since the start time is 1:00pm and next schedule time is 2:00pm, but the current time is 2:30pm we missed the
      # schedule time. So then we expect the there is no sleep and the next schedule is 3:00pm.
      next_schedule_time = scheduler.stall_schedule(next_schedule_time)
      expect(next_schedule_time).to eq Time.local(2015, 9, 1, 15, 00, 0)
    end

    # Precondition:
    #               - none
    it 'Within the next scheduled time' do
      # Start time is 1:00pm.
      start_time = Time.local(2015, 9, 1, 13, 00, 0)

      # Freeze the time at the current time, which is 1:30pm.
      current_time = Time.local(2015, 9, 1, 13, 59, 55)
      Timecop.freeze(current_time)

      # Set the interval in to every hour.
      scheduler = Scheduler.new(hour_interval)

      # The next schedule time should be based on the start time + the interval, which would be 2:00pm.
      next_schedule_time = start_time + hour_interval

      # Since the start time is 1:00pm and next schedule time is 2:00pm, but the current time is 1:59:55pm, so we are
      # within the scheduled time. So then we expect to sleep for 5 seconds and the next schedule is 3:00pm.
      next_schedule_time = scheduler.stall_schedule(next_schedule_time)

      expect(next_schedule_time).to eq Time.local(2015, 9, 1, 15, 00, 0)
    end
  end # stall_schedule
end # describe Scheduler
