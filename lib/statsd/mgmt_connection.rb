module Statsd
  class MgmtConnection < EM::Connection
    
    attr_accessor :server    
    
    def receive_data(msg)
      response = server.on_mgmt_message(msg)
      send_data response
      close_connection_after_writing
    end
    
  end
end