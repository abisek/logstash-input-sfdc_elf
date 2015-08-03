require_relative '../spec_helper'

describe LogStash::Inputs::SfdcElf do

  describe 'Path config' do
    let(:provided_path_with_file) { provided_path_with_file =  "#{Dir.home}/.sfdc_info_logstash_ThisIsATestID00000" }

    # Apply this stub_request to all test cases in this describe block because the suffix for the .sfdc_info_logstash
    # is based on the the client org id, so a successful login is needed.
    before do
      # Stub authentication
      stub_request(:post, /login.salesforce.com/).
          with(:headers => {'Accept'=>'*/*', 'User-Agent'=>'Ruby'}).
          to_return(:status => 200, body: fixture('auth_success_response.json'), :headers => {})

      # Stub organization query
      stub_request(:get, "https://na1.salesforce.com/services/data/v33.0/query?q=select%20id%20from%20Organization").
          with(:headers => {'Accept'=>'*/*', 'Authorization'=>'OAuth access_token', 'User-Agent'=>'Ruby'}).
          to_return(:status => 200, body: fixture('org_query_response.json'), :headers => {})

      # Stub describe query
      # TODO: Not sure why org query calls describe query, and the decribe file is huge!!
      stub_request(:get, "https://na1.salesforce.com/services/data/v33.0/sobjects/Organization/describe").
          with(:headers => {'Accept'=>'*/*', 'Authorization'=>'OAuth access_token', 'User-Agent'=>'Ruby'}).
          to_return(:status => 200, body: fixture('describe.json'), :headers => {})
    end




    # Precondition:
    #               - .sfdc_info_logstash file does not exist in the home directory.
    #               - Successful client login.
    it 'sets .sfdc_info_logstash file in the home directory because no path was specified' do
      config =
        {
          'username' => 'me@example.com',
          'password' => 'password',
          'security_token' => 'security_token',
        }

      # Push config though the plugin life cycle of register and teardown only.
      plugin = LogStash::Inputs::SfdcElf.new(config)
      plugin.register
      expect(plugin.path).to eq Dir.home
      plugin.teardown # TODO: Move teardown to after block. See https://www.relishapp.com/rspec/rspec-core/v/2-2/docs/hooks/before-and-after-hooks

      # Delete the .sfdc_info_logstash file.
      File.delete(provided_path_with_file)
    end




    # Precondition:
    #               - .sfdc_info_logstash file does not exist in the home directory.
    #               - Successful client login.
    it 'sets .sfdc_info_logstash file in the home directory because the provided path is does not exist' do
      config =
        {
          'username' => 'me@example.com',
          'password' => 'password',
          'security_token' => 'security_token',
          'path' => 'This/is/an/incorrect/path'
        }

      # Push config though the plugin life cycle of register and teardown only.
      plugin = LogStash::Inputs::SfdcElf.new(config)
      plugin.register
      expect(plugin.path).to eq Dir.home
      plugin.teardown

      # Delete the .sfdc_info_logstash file.
      File.delete(provided_path_with_file)
    end




    # Precondition:
    #               - .sfdc_info_logstash file does not exist in the provided directory.
    #               - Successful client login
    it 'sets sfdc_info_path to the provided path' do
      provided_path = Dir.tmpdir

      config =
        {
          'username' => 'me@example.com',
          'password' => 'password',
          'security_token' => 'security_token',
          'path' => provided_path
        }

      plugin = LogStash::Inputs::SfdcElf.new(config)
      plugin.register
      expect(plugin.path).to eq provided_path
      plugin.teardown

      # Delete the .sfdc_info_logstash file.
      File.delete("#{provided_path}/.sfdc_info_logstash_ThisIsATestID00000")
    end
  end #Path for .sfdc_info_logstash


end # describe SfdcElf
