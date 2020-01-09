# frozen_string_literal: true

require 'mysql2/aurora/version'
require 'mysql2'

module Mysql2
  # mysql2 aurora module
  # @note This module patch Mysql2::Client
  module Aurora
    # Implement client patch
    class Client
      attr_reader :client

      AuroraReadOnlyError = Class.new(StandardError) do
        attr_reader :innodb_read_only_value

        def initialize(innodb_read_only_value)
          @innodb_read_only_value = innodb_read_only_value
          super("innodb_read_only_value was '#{innodb_read_only_value}', expected: 'OFF'")
        end
      end

      AURORA_READONLY_ERROR = '--read-only'
      AURORA_READONLY_CHECK_QUERY = \
        "SHOW GLOBAL VARIABLES LIKE 'innodb_read_only';"

      AURORA_RECONNECT_ERROR_MESSAGES = [
        AURORA_READONLY_ERROR,
        'client is not connected',
        'Lost connection to MySQL server',
        "Can't connect to MySQL",
        'Server shutdown in progress'
      ].freeze

      # Initialize class
      # @note [Override] with reconnect options
      # @param [Hash] opts Options
      # @option opts [Integer] aurora_max_retry Max retry count, when failover. (Default: 5)
      def initialize(opts)
        @opts      = Mysql2::Util.key_hash_as_symbols(opts)
        @max_retry = (@opts.delete(:aurora_max_retry) || 5).to_i
        reconnect!
      end

      # Execute query with reconnect
      # @note [Override] with reconnect.
      def query(*args)
        begin
          client.query(*args)
        rescue Mysql2::Error => e
          aurora_auto_reconnect!(e) if aurora_reconnect_error?(e.message.to_s)

          raise e
        end
      end

      def aurora_readonly_error?(message)
        message.include?(AURORA_READONLY_ERROR)
      end

      def aurora_reconnect_error?(message)
        AURORA_RECONNECT_ERROR_MESSAGES.any? do |matching_str|
          message.include?(matching_str)
        end
      end

      # Reconnect to database and Set `@client`
      # @note If client is not connected, Connect to database.
      def reconnect!
        query_options = (@client&.query_options&.dup || {})

        begin
          @client&.close
        rescue StandardError
          nil
        end

        @client = Mysql2::Aurora::ORIGINAL_CLIENT_CLASS.new(@opts)
        @client.query_options.merge!(query_options)
      end

      def aurora_auto_reconnect!(error)
        warn "[mysql2-aurora] Auto-reconnect on error, max retries: #{@max_retry}"
        try_count = 0
        begin
          retry_interval_seconds = [1.5 * (try_count - 1), 10].min

          warn "[mysql2-aurora] Database is readonly. Retry after #{retry_interval_seconds}seconds"
          sleep retry_interval_seconds
          reconnect!

          return unless aurora_readonly_error?(error.message)

          # if error was a readonly type, it must check that we are not being
          # reconnected to a slave, so we ensure that we are connected to a
          # master node by checking 'innodb_read_only' MySQL's variable
          # it must be set to 'OFF' if we are on the master
          innodb_readonly_result = client.query(
            AURORA_READONLY_CHECK_QUERY
          ).to_a.first

          if innodb_readonly_result.nil? ||
             innodb_readonly_result.fetch('Value', '').to_s.upcase != 'OFF'
            raise AuroraReadOnlyError
          end
        rescue => e
          warn "[mysql2-aurora] auto reconnect error: #{e.message}"
          try_count += 1
          raise e if try_count > @max_retry

          retry
        end
      end

      # Delegate method call to client.
      # @param [String] name  Method name
      # @param [Array]  args  Method arguments
      # @param [Proc]   block Method block
      def method_missing(name, *args, &block) # rubocop:disable Style/MethodMissingSuper, Style/MissingRespondToMissing
        client.public_send(name, *args, &block)
      end

      # Delegate method call to Mysql2::Client.
      # @param [String] name  Method name
      # @param [Array]  args  Method arguments
      # @param [Proc]   block Method block
      def self.method_missing(name, *args, &block) # rubocop:disable Style/MethodMissingSuper, Style/MissingRespondToMissing
        Mysql2::Aurora::ORIGINAL_CLIENT_CLASS.public_send(name, *args, &block)
      end

      # Delegate const reference to class.
      # @param [Symbol] name Const name
      def self.const_missing(name)
        Mysql2::Aurora::ORIGINAL_CLIENT_CLASS.const_get(name)
      end
    end

    # Swap Mysql2::Client
    ORIGINAL_CLIENT_CLASS = Mysql2.send(:remove_const, :Client)
    Mysql2.const_set(:Client, Mysql2::Aurora::Client)
  end
end
