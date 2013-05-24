module Statsd
  class Runner

    def self.default_config
      {
        :host            => "0.0.0.0",
        :port            => 8125,
        :mgmt_port       => 8126,
        :debug           => false,
        :purge_interval  => 60,
        :threshold_purge => 30*60,
        :threshold_pct   => 90,
      }
    end

    def self.run(opts = {})
      config = self.default_config.merge(opts)
      
      server = Server.new config
      server.start
    end

  end
end