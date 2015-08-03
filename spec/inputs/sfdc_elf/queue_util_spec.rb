require_relative '../../spec_helper'
require 'csv'

describe QueueUtil do

  before do
    # Stub authentication
    stub_request(:post, /login.salesforce.com/).
        with(:headers => {'Accept'=>'*/*', 'User-Agent'=>'Ruby'}).
        to_return(:status => 200, body: fixture('auth_success_response.json'), :headers => {})

    # Stub getting EventLogFile description
    stub_request(:get, 'https://na1.salesforce.com/services/data/v22.0/sobjects/EventLogFile/describe').
        with(:headers => {'Accept'=>'*/*', 'Accept-Encoding'=>'gzip;q=1.0,deflate;q=0.6,identity;q=0.3', 'Authorization'=>'OAuth access_token', 'User-Agent'=>'Ruby'}).
        to_return(:status => 200, body: fixture('queue_util/eventlogfile_describe.json'), :headers => {})
  end


  describe '#enqueue_events' do
    # Apply this stub_request to all test cases in this describe block because ...
    before do
      # Stub getting list of EventLogFiles.
      stub_request(:get, 'https://na1.salesforce.com/services/data/v22.0/query?q=SELECT%20Id,%20EventType,%20Logfile,%20LogDate,%20LogFileLength%20FROM%20EventLogFile').
          with(:headers => {'Accept'=>'*/*', 'Authorization'=>'OAuth access_token', 'User-Agent'=>'Ruby'}).
          to_return(:status => 200, body: fixture('queue_util/eventlogfile_list.json'), :headers => {})

      # Stub in all of the require EventLogFiles from sample_data*
      stub_request(:get, 'https://na1.salesforce.com/services/data/v33.0/sobjects/EventLogFile/sample_data1').
          with(:headers => {'Accept'=>'*/*', 'Authorization'=>'OAuth access_token', 'User-Agent'=>'Ruby'}).
          to_return(:status => 200, body: fixture('queue_util/sample_data1.csv'), :headers => {})

      stub_request(:get, 'https://na1.salesforce.com/services/data/v33.0/sobjects/EventLogFile/sample_data2').
          with(:headers => {'Accept'=>'*/*', 'Authorization'=>'OAuth access_token', 'User-Agent'=>'Ruby'}).
          to_return(:status => 200, body: fixture('queue_util/sample_data2.csv'), :headers => {})

      stub_request(:get, 'https://na1.salesforce.com/services/data/v33.0/sobjects/EventLogFile/sample_data3').
          with(:headers => {'Accept'=>'*/*', 'Authorization'=>'OAuth access_token', 'User-Agent'=>'Ruby'}).
          to_return(:status => 200, body: fixture('queue_util/sample_data3.csv'), :headers => {})

      stub_request(:get, 'https://na1.salesforce.com/services/data/v33.0/sobjects/EventLogFile/sample_data4').
          with(:headers => {'Accept'=>'*/*', 'Authorization'=>'OAuth access_token', 'User-Agent'=>'Ruby'}).
          to_return(:status => 200, body: fixture('queue_util/sample_data4.csv'), :headers => {})

      stub_request(:get, 'https://na1.salesforce.com/services/data/v33.0/sobjects/EventLogFile/sample_data5').
          with(:headers => {'Accept'=>'*/*', 'Authorization'=>'OAuth access_token', 'User-Agent'=>'Ruby'}).
          to_return(:status => 200, body: fixture('queue_util/sample_data5.csv'), :headers => {})

    end

    # /services/data/v33.0/query?q=select+id,logfile,logdate,logfilelength,eventtype+from+eventlogfile+where+logdate+%3e+0001-01-01T00:00:00Z+order+by+logdate+desc

    # Precondition:
    #               - None.
    it 'Enqueues events from a SOQL query list size of 5 EventLogFiles' do
      # Create a client object to grab stubbed EventLogFiles and their CSV files associated with them.
      client = ClientWithStreamingSupport.new
      client.authenticate(username: 'me@example.com', password: 'passwordWithToken')
      soql_expr = 'SELECT Id, EventType, Logfile, LogDate, LogFileLength FROM EventLogFile'
      query_result_list = client.query(soql_expr)

      queue_util = QueueUtil.new
      queue = Queue.new

      # Check if enqueue_events() parses all of the data and gets a queue of size 1783.
      queue_util.enqueue_events(query_result_list, queue, client)
      expect(queue.size).to eq 1783
    end
  end





  describe '#create_event' do

    before do
      # Stub getting list of EventLogFiles.
      stub_request(:get, 'https://na1.salesforce.com/services/data/v22.0/query?q=GiveMe:create_event_ELF_list1.json').
          with(:headers => {'Accept'=>'*/*', 'Authorization'=>'OAuth access_token', 'User-Agent'=>'Ruby'}).
          to_return(:status => 200, body: fixture('queue_util/create_event_ELF_list1.json'), :headers => {})

      # Stub in all of the require EventLogFiles from sample_data
      stub_request(:get, 'https://na1.salesforce.com/services/data/v33.0/sobjects/EventLogFile/create_event_sampledata1').
          with(:headers => {'Accept'=>'*/*', 'Authorization'=>'OAuth access_token', 'User-Agent'=>'Ruby'}).
          to_return(:status => 200, body: fixture('queue_util/create_event_sampledata1.csv'), :headers => {})



      stub_request(:get, 'https://na1.salesforce.com/services/data/v22.0/query?q=GiveMe:create_event_ELF_list2.json').
          with(:headers => {'Accept'=>'*/*', 'Authorization'=>'OAuth access_token', 'User-Agent'=>'Ruby'}).
          to_return(:status => 200, body: fixture('queue_util/create_event_ELF_list2.json'), :headers => {})

      # Stub in all of the require EventLogFiles from sample_data
      stub_request(:get, 'https://na1.salesforce.com/services/data/v33.0/sobjects/EventLogFile/create_event_sampledata2').
          with(:headers => {'Accept'=>'*/*', 'Authorization'=>'OAuth access_token', 'User-Agent'=>'Ruby'}).
          to_return(:status => 200, body: fixture('queue_util/create_event_sampledata2.csv'), :headers => {})
    end

    # Precondition:
    #               - None.
    it 'Correctly converted salesforce time to iso8601 via LogStash::Timestamp' do
      # Create a client object to grab stubbed EventLogFiles and their CSV files associated with them.
      client = ClientWithStreamingSupport.new
      client.authenticate(username: 'me@example.com', password: 'passwordWithToken')
      soql_expr = 'GiveMe:create_event_ELF_list1.json'
      query_result_list = client.query(soql_expr)

      queue_util = QueueUtil.new
      queue = Queue.new
      queue_util.enqueue_events(query_result_list, queue, client)
      event = queue.pop

      # Check if the time was converted correctly.
      expect(event.timestamp.to_s).to eq '2015-07-26T22:24:19.438Z'

      # Makes sure that the object stored in event.timestamp is of the class LogStash::Timestamp because ElasticSearch
      # expects it to be so and anything else doesnt work.
      # Things that dont work: DateTime, Time, and Strings in iso8601 format.
      expect(event.timestamp.class).to eq LogStash::Timestamp
    end



    # Precondition:
    #               - None.
    it 'Creates events based on the header to handle version and schema changes' do
      # Create a client object to grab stubbed EventLogFiles and their CSV files associated with them.
      client = ClientWithStreamingSupport.new
      client.authenticate(username: 'me@example.com', password: 'passwordWithToken')
      soql_expr = 'GiveMe:create_event_ELF_list2.json'
      query_result_list = client.query(soql_expr)

      queue_util = QueueUtil.new
      queue = Queue.new
      queue_util.enqueue_events(query_result_list, queue, client)
      event1 = queue.pop
      event2 = queue.pop

      # I expect event1 and event2 to have different keys, which are bases on the CSV files header.
      expect(event1.to_hash.keys).not_to eq (event2.to_hash.keys)
    end
  end #create_event




  describe '#get_csv_tempfile_list' do
    before do
      # Stub getting list of EventLogFiles.
      stub_request(:get, 'https://na1.salesforce.com/services/data/v22.0/query?q=GiveMe:create_event_ELF_list3.json').
          with(:headers => {'Accept'=>'*/*', 'Authorization'=>'OAuth access_token', 'User-Agent'=>'Ruby'}).
          to_return(:status => 200, body: fixture('queue_util/create_event_ELF_list3.json'), :headers => {})

      # Stub in all of the require EventLogFiles from sample_data
      stub_request(:get, 'https://na1.salesforce.com/services/data/v33.0/sobjects/EventLogFile/create_event_sampledata3').
          with(:headers => {'Accept'=>'*/*', 'Authorization'=>'OAuth access_token', 'User-Agent'=>'Ruby'}).
          to_return(:status => 200, body: fixture('queue_util/create_event_sampledata3.csv'), :headers => {})
    end

    it 'returns a list of tempfiles that are pointing to the beginning of the file' do
      # Create a client object to grab stubbed EventLogFiles and their CSV files associated with them.
      client = ClientWithStreamingSupport.new
      client.authenticate(username: 'me@example.com', password: 'passwordWithToken')
      soql_expr = 'GiveMe:create_event_ELF_list3.json'
      query_result_list = client.query(soql_expr)

      queue_util = QueueUtil.new
      tempfile_list = queue_util.get_csv_tempfile_list(query_result_list, client)

      # Loop though each tempfile.
      tempfile_list.each do |tmp|

        expect(tmp.readline).to eq "Beginning of file\n"

        expect(tmp.readline).to eq 'End of file'

        # Close tmp file and unlink it, doing this will delete the actual tempfile.
        tmp.close
        tmp.unlink
      end # do loop
    end

    # TODO: test token expires when trying to do streaming download
  end #get_csv_tempfile_list

end # describe SfdcElf
