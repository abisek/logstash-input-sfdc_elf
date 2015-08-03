require_relative '../../spec_helper'

describe StatePersistor do
  let(:provided_path) { provided_path =  Dir.tmpdir }
  let(:provided_org_id) { provided_org_id =  'some_org_id' }
  let(:provided_path_with_file) { provided_path_with_file =  "#{provided_path}/.sfdc_info_logstash_#{provided_org_id}" }

  after do
    # Delete the .sfdc_info_logstash file.
    File.delete(provided_path_with_file) if File.exist?(provided_path_with_file)
  end



  describe '#get_last_indexed_log_date' do

    # Precondition:
    #               - .sfdc_info_logstash file does not exist in the system temp directory.
    it 'creates .sdfc_info_logstash file because it does not exist' do

      # I expect the sfdc_info_logstash file to not exist.
      expect(File.exist?(provided_path_with_file)).to eq false

      # I expect the file to exist now.
      state_persistor = StatePersistor.new(provided_path, provided_org_id)
      state_persistor.get_last_indexed_log_date
      expect(File.exist?(provided_path_with_file)).to eq true
    end


    # Precondition:
    #               - .sfdc_info_logstash file exist in the provided directory and with the default date in it which is
    #                 created in this IT block.
    it 'read from .sdfc_info_logstash file which as the default date in it' do
      state_persistor = StatePersistor.new(provided_path, provided_org_id)

      # I expect the .sfdc_info_logstash file to not exist with default date in it.
      expect(state_persistor.get_last_indexed_log_date).to eq '0001-01-01T00:00:00Z'
    end
  end #get_last_indexed_log_date




  describe '#update_last_indexed_log_date' do
    # Precondition:
    #               - .sfdc_info_logstash file exist in the provided directory and with the default date in it which is
    #                 created in this IT block.
    it 'updates the last indexed LogDate on the .sdfc_info_logstash file' do
      state_persistor = StatePersistor.new(provided_path, provided_org_id)

      # I expect the .sfdc_info_logstash file having default date in it.
      expect(state_persistor.get_last_indexed_log_date).to eq '0001-01-01T00:00:00Z'

      # I expect the .sfdc_info_logstash file having the new date.
      state_persistor.update_last_indexed_log_date('3672-21-11T23:59:342Z')
      expect(state_persistor.get_last_indexed_log_date).to eq '3672-21-11T23:59:342Z'
    end
  end


end # describe StatePersistor
