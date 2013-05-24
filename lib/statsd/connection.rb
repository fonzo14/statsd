module Statsd
  class Connection < EM::Connection
    
    attr_accessor :server
    
    def receive_data(msg)
      server.on_message(msg)
    end
    
  end
end