# encoding: utf-8
require 'databasedotcom'

class ClientWithStreamingSupport < Databasedotcom::Client

  def streaming_download(path, output_stream)
    connection = Net::HTTP.new(URI.parse(self.instance_url).host, 443)
    connection.use_ssl = true
    encoded_path = URI.escape(path)

    req = Net::HTTP::Get.new(encoded_path, {"Authorization" => "OAuth #{self.oauth_token}"})
    connection.request(req) do |response|
      raise SalesForceError.new(response) unless response.is_a?(Net::HTTPSuccess)
      response.read_body do |chunk|
        output_stream.write chunk
      end
    end
  end

  # This helper method is called whenever we need to initaialize the client object or whenever the
  # client token expires. It will attempt 3 times with a 30 second delay between each retry. On the 3th try, if it
  # fails the exception will be raised.

  # todo move to streamign client
  def retryable_authenticate(options={})
    1.upto(options[:retry_attempts]) do |count|
      begin
        # If exception is not thrown, then break out of loop.
        self.authenticate(username: options[:username], password: options[:password])
        break
      rescue Exception => e
        # Sleep for 30 seconds 2 times. On the 3th time if it fails raise the exception without sleeping.
        if (count == options[:retry_attempts])
          raise e
        else
          sleep(30)
        end
      end
    end
  end # def authenticate



  def retryable_query(options={})
    begin
      self.query(options[:soql_expr])
    rescue Databasedotcom::SalesForceError => e
      # Session has expired. Force user logout, then re-authenticate.
      if e.message == 'Session expired or invalid'
        self.retryable_authenticate(options)
      else
        raise e
      end

    end # rescue / begin
  end # def retryable_query

end # ClientWithStreamingSupport