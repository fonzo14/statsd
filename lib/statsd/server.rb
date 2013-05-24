module Statsd
  class Server
    include Aggregator

    attr_reader :config, :counters, :timers

    def initialize(config)
      @config     = config
      @timers     = {}
      @counters   = {}
      @gauges     = {}
      @timestamps = {}
    end
    
    def start
      EM::run do
        EM::start_server(@config[:host], @config[:mgmt_port], MgmtConnection) do |conn|
          conn.server = self 
        end
        
        EM::open_datagram_socket(@config[:host], @config[:port], Connection) do |conn|
          conn.server = self
          puts "Now accepting connections on address #{config[:host]}, port #{config[:port]}..." 
        end

        EM::add_periodic_timer(config[:purge_interval]) do
          purge!
        end
         
      end
    end
    
    def on_mgmt_message(msg)
      # msg example: stat:<stat_name>
      # gauges
      q, stat  = msg.strip.split(':')
      if (stat)
        puts msg if (@config[:debug])
        stat_key = stat.gsub(/\s+/, '_').gsub(/\//, '-').gsub(/[^a-zA-Z_\-0-9\.]/, '')
        aggregate!(stat_key)
      elsif q == 'gauges'
        @gauges.to_a.map { |pair| pair.join(':') }.join('|')
      end
    end
    
    def on_message(msg)

      puts msg if (@config[:debug])

      bits = msg.split(':')
      key  = bits.first.gsub(/\s+/, '_').gsub(/\//, '-').gsub(/[^a-zA-Z_\-0-9\.]/, '')
      
      bits << '1' if bits.empty?

      bits.each do |b|
        next unless b.include? '|'

        sample_rate = 1
        fields      = b.split('|')

        if fields[1]
          case fields[1].strip
          when 'ms' #timer ex : glork:320|ms
            @timers[key] ||= []
            @timers[key] << fields[0].to_f || 0
          when 'c' #counter ex : gorets:1|c
            /^@([\d\.]+)/.match(fields[2]) {|m| sample_rate = m[1].to_f }

            @counters[key] ||= 0
            @counters[key] += (fields[0].to_f || 1) * (1/sample_rate)
          when 'g' #gauge ex : gaugor:333|g
            @gauges[key] = fields[0].to_f || 0
          else
            # do nothing
            puts "Unsupported type: #{msg}" if (@config[:debug])
          end
        else
          puts "Invalid line: #{msg}" if (@config[:debug])
        end
      end

    end

  end
end
