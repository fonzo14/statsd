module Statsd
  module Aggregator

    def aggregate!(stat_key)
      stat              = ''
      ts                = $TESTING ? 0 : Time.new.to_i
      previous_ts       = @timestamps[stat_key]
      @timestamps[stat_key] = ts

      stat = aggregate_counter!(stat_key,@counters[stat_key],ts,previous_ts) if @counters.key?(stat_key)
      stat = aggregate_timer!(stat_key,@timers[stat_key],ts,previous_ts)     if @timers.key?(stat_key)        
      stat = aggregate_gauge!(stat_key,@gauges[stat_key],ts,previous_ts)     if @gauges.key?(stat_key)

      stat
    end
    
    def purge!
      puts "purging statsd" if (@config[:debug])

      ts = Time.new.to_i
      @counters.each do |k,v|
        previous_ts  = @timestamps[k]
        @counters[k] = 0 if (previous_ts.nil? || (ts - previous_ts > @config[:threshold_purge]))
      end
      @timers.each do |k,v|
        previous_ts = @timestamps[k]
        @timers[k]  = [] if (previous_ts.nil? || (ts - previous_ts > @config[:threshold_purge]))
      end
      @gauges.each do |k,v|
        previous_ts = @timestamps[k]
        @gauges[k] = 0 if (previous_ts.nil? || (ts - previous_ts > @config[:threshold_purge]))
      end
    end
    
    private
    def aggregate_gauge!(k,v,ts,previous_ts)
      #@gauges[k] = 0

      ['g',v.to_f].join('|')
    end

    def aggregate_counter!(k,v,ts,previous_ts)
      stat         = nil
      @counters[k] = 0
      
      if previous_ts && (ts - previous_ts > 0)
        val = v / (ts - previous_ts)
        stat = ['c',val,v.to_i,ts].join('|')
      end
      stat
    end
    
    def aggregate_timer!(k,v,ts,previous_ts)
      stat       = nil
      @timers[k] = []
      
      if previous_ts && (ts - previous_ts > 0)
        pct_thresh  = @config[:threshold_pct]
        values      = v.sort { |a,b| a-b }
        min         = values.first
        max         = values.last

        mean          = min
        max_at_thresh = max

        if values.size > 1
          thresh_idx    = (((100-pct_thresh)/100) * values.length).round
          num_in_thresh = values.length - thresh_idx

          values = values.slice(0, num_in_thresh)
          max_at_thresh = values.last

          # avg remaining times
          mean = values.reduce(0, :+) / num_in_thresh
        end

        count = values.length

        stat = ['t',mean,max,max_at_thresh,min,count.to_i,count.to_f / (ts - previous_ts),ts].join('|')
      end
      stat
    end
    
  end

end
