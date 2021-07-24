# frozen_string_literal: true

require 'mysql2/aurora/version'
require 'mysql2'

module Mysql2
  # mysql2 aurora module
  # @note This module patch Mysql2::Client
  module Aurora
    ORIGINAL_CLIENT_CLASS = ::Mysql2.send(:remove_const, :Client)

    # Implement client patch
    class Client
      attr_reader :client

      AuroraReadOnlyError = Class.new(StandardError) do
        attr_reader :read_only_value

        def initialize(read_only_value)
          @read_only_value = read_only_value
          super("read_only_value was '#{read_only_value}', expected: 'OFF'")
        end
      end

      AURORA_READONLY_ERROR = /(read-only|READ ONLY)/i
      AURORA_READONLY_CHECK_QUERY = \
        "SHOW GLOBAL VARIABLES LIKE '%s';"

      AURORA_CONNECTION_ERRORS = [
        'client is not connected',
        'Lost connection to MySQL server',
        "Can't connect to MySQL",
        'Server shutdown in progress'
      ].freeze

      # Initialize class
      # @note [Override] with reconnect options
      # @param [Hash] opts Options
      # @option opts [Integer] aurora_max_retry Max retry count, when failover. (Default: 5)
      # @option opts [Bool] aurora_disconnect_on_readonly, when readonly exception hit terminate the connection (Default: false)
      def initialize(opts)
        @opts = Mysql2::Util.key_hash_as_symbols(opts)
        @max_retry = @opts.delete(:aurora_max_retry) || 5
        @disconnect_only = @opts.delete(:aurora_disconnect_on_readonly) || false
        reconnect!
      end

      # Execute query with reconnect
      # @note [Override] with reconnect.
      def query(*args)
        aurora_reconnect! if client.closed?
        begin
          client.query(*args)
        rescue Mysql2::Error => e
          raise e unless e.message&.include?('--read-only')

          try_count += 1

          if @disconnect_only
            warn '[mysql2-aurora] Database is readonly, Aurora failover event likely occured, closing database connection'
            disconnect!
          elsif try_count <= @max_retry
            retry_interval_seconds = [1.5 * (try_count - 1), 10].min

            warn "[mysql2-aurora] Database is readonly. Retry after #{retry_interval_seconds}seconds"
            sleep retry_interval_seconds
            reconnect!
            retry
          end

          raise e
        end
      end

      # Reconnect to database and Set `@client`
      # @note If client is not connected, Connect to database.
      def reconnect!
        query_options = (@client&.query_options&.dup || {})

        disconnect!

        warn "[mysql2-aurora] auto-reconnect success"
      end

      # Close connection to database server
      def disconnect!
        @client&.close
      rescue StandardError
        nil
      end

      # Delegate method call to client.
      # @param [String] name  Method name
      # @param [Array]  args  Method arguments
      # @param [Proc]   block Method block
      def method_missing(name, *args, &block) # rubocop:disable Style/MethodMissingSuper, Style/MissingRespondToMissing
        @client.__send__(name, *args, &block)
      end

      # Delegate method call to Mysql2::Client.
      # @param [String] name  Method name
      # @param [Array]  args  Method arguments
      # @param [Proc]   block Method block
      def self.method_missing(name, *args, &block) # rubocop:disable Style/MethodMissingSuper, Style/MissingRespondToMissing
        ::Mysql2::Aurora::ORIGINAL_CLIENT_CLASS.__send__(
          name, *args, &block
        )
      end

      # Delegate const reference to class.
      # @param [Symbol] name Const name
      def self.const_missing(name)
        ::Mysql2::Aurora::ORIGINAL_CLIENT_CLASS.const_get(name)
      end
    end

    module_function
    def read_only_variable=(arg)
      @read_only_variable = arg.to_s
    end

    def read_only_variable
      @read_only_variable ||= 'read_only'
    end
  end
end

Mysql2.const_set(:Client, ::Mysql2::Aurora::Client)
