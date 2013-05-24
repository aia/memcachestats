require 'logger'

class MemcacheStats
  
  attr_accessor :config, :connection, :host, :log
  
  attr_accessor :gauges, :counters, :metrics, :stats
  
  def initialize(config, logger = nil)
    @config = config
    
    initialize_log unless logger
    @log = logger if logger
    @log.error("Logging started")
    
    initialize_metrics
  end
  
  def initialize_log
    @config['log'] = {
       'file' => STDOUT,
       'level' => 'INFO'
     }.merge(@config['log'] || {})

     log_initialize = [@config['log']['file']]
     log_initialize << @config['log']['shift_age'] if @config['log']['shift_age']
     log_initialize << @config['log']['shift_size'] if @config['log']['shift_size']

     begin
       @log = Logger.new(*log_initialize)
       @log.level = Logger.const_get(@config['log']['level'])
     rescue Exception => e
       @config['log'] = {
         'file' => STDOUT,
         'level' => 'INFO'
       }
       @log = Logger.new(@config['log']['file'])
       @log.level = Logger.const_get(@config['log']['level'])
       @log.error("Caught a problem with log settings")
       @log.error("#{e.message}")
       @log.error("Setting log settings to defaults")
     end
  end
  
  def initialize_metrics
    @gauges = {
      'curr_connections' => {
        'units' => 'conn',
        'type' => 'uint32'
      },
      'curr_items' => {
        'units' => 'keys',
        'type' => 'double'
      },
      'threads' => {
        'units' => 'threads',
        'type' => 'uint32'
      },
      'bytes' => {
        'units' => 'bytes',
        'publish' => 'no'
      },
      'limit_maxbytes' => {
        'units' => 'bytes',
        'publish' => 'no'
      },
      'total_connections_total' => {
        'units' => 'conn',
        'publish' => 'no'
      },
      'total_items_total' => {
        'units' => 'key',
        'publish' => 'no'
      },
      'get_hits_total' => {
        'units' => 'r',
        'publish' => 'no'
      },
      'get_misses_total' => {
        'units' => 'r',
        'publish' => 'no'
      },
      'delete_misses_total' => {
        'units' => 'r',
        'publish' => 'no'
      },
      'delete_hits_total' => {
        'units' => 'r',
        'publish' => 'no'
      },
      'bytes_read_total' => {
        'units' => 'bytes',
        'publish' => 'no'
      },
      'bytes_written_total' => {
        'units' => 'bytes',
        'publish' => 'no'
      },
      "get_hit_percentage" => {
        'units' => '%',
        'type' => 'double'
      },
      "get_miss_percentage" => {
        'units' => '%',
        'type' => 'double'
      },
      "delete_hit_percentage" => {
        'units' => '%',
        'type' => 'double'
      },
      "delete_miss_percentage" => {
        'units' => '%',
        'type' => 'double'
      },
      "used_percentage" => {
        'units' => '%',
        'type' => 'double'
      }
    }
    
    @counters = {
      'total_connections' => {
        'units' => 'conn/s',
        'type' => 'uint32'
      },
      'total_items' => {
        'units' => 'keys/s',
        'type' => 'uint32'
      },
      'get_hits' => {
        'units' => 'r/s',
        'type' => 'uint32'
      },
      'get_misses' => {
        'units' => 'r/s',
        'type' => 'uint32'
      },
      'delete_misses' => {
        'units' => 'r/s',
        'type' => 'uint32'
      },
      'delete_hits' => {
        'units' => 'r/s',
        'type' => 'uint32'
      },
      'bytes_read' => {
        'units' => 'bytes/s',
        'type' => 'double'
      },
      'bytes_written' => {
        'units' => 'bytes/s',
        'type' => 'double'
      }
    }
    
    @metrics = @counters.merge(@gauges)
  end
  
  def connect(host, port = '11211')
    return if @connection and ((@host == host) && (@port == port))
    
    @connection = TCPSocket.new(host, port)
    @host = host
    @port = port
  end
  
  def get_memcached_stats(host, port = '11211')
    @stats = {}
    matched = {}

    begin
      connect(host, port)

      @connection.puts("stats")

      while ((line = @connection.gets.chomp) != "END")
        if (matched = /STAT (?<key>\w+) (?<value>\d+)/.match(line))
          @stats[matched['key']] = matched['value']
          @stats["#{matched['key']}_total"] = matched['value'] if @gauges["#{matched['key']}_total"]
        end
      end
    rescue Exception => e
      pp ["ex", e]
    end
  end
  
  def add_derived_metrics(stats)
    stats['get_hit_percentage'] = stats['get_hits_total'].to_f / (stats['get_hits_total'] + stats['get_misses_total'] + 1)
    stats['get_miss_percentage'] = stats['get_misses_total'].to_f / (stats['get_hits_total'] + stats['get_misses_total'] + 1)
    stats['delete_hit_percentage'] = stats['delete_hits_total'].to_f / (stats['delete_hits_total'] + stats['delete_misses_total'] + 1)
    stats['delete_miss_percentage'] = stats['delete_misses_total'].to_f / (stats['delete_hits_total'] + stats['delete_misses_total'] + 1)
    stats['used_percentage'] = stats['bytes'].to_i / (stats['limit_maxbytes'].to_f + 1)
  end
end