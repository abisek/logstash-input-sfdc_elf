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
end