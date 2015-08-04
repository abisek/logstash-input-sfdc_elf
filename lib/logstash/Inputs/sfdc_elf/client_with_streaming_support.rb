# encoding: utf-8
require 'databasedotcom'

# This class subclasses Databasedotcom Client object and added steaming
# downloading and retryable authentication and retryable query.
class ClientWithStreamingSupport < Databasedotcom::Client
  # Constants
  # LOG_KEY        = 'SFDC - ClientWithStreamingSupport'
  #
  # def initialize
  #   @logger = Cabin::Channel.get(LogStash)
  # end

  def streaming_download(path, output_stream)
    connection = Net::HTTP.new(URI.parse(instance_url).host, 443)
    connection.use_ssl = true
    encoded_path = URI.escape(path)

    req = Net::HTTP::Get.new(encoded_path, 'Authorization' => "OAuth #{oauth_token}")
    connection.request(req) do |response|
      raise SalesForceError.new(response) unless response.is_a?(Net::HTTPSuccess)
      response.read_body do |chunk|
        output_stream.write chunk
      end
    end
  end

  # This helper method is called whenever we need to initaialize the client
  # object or whenever the client token expires. It will attempt 3 times
  # with a 30 second delay between each retry. On the 3th try, if it fails the
  # exception will be raised.

  def retryable_authenticate(options = {})
    1.upto(options[:retry_attempts]) do |count|
      begin
        # If exception is not thrown, then break out of loop.
        authenticate(username: options[:username], password: options[:password])
        break
      rescue StandardError => e
        # Sleep for 30 seconds 2 times. On the 3th time if it fails raise the exception without sleeping.
        if (count == options[:retry_attempts])
          raise e
        else
          sleep(30)
        end
      end
    end
  end # def authenticate


  def retryable_query(options = {})
    query(options[:soql_expr])
  rescue Databasedotcom::SalesForceError => e
    # Session has expired. Force user logout, then re-authenticate.
    if e.message == 'Session expired or invalid'
      retryable_authenticate(options)
    else
      raise e
    end
  end # def retryable_query
end # ClientWithStreamingSupport
