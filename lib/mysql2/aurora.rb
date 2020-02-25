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

      AURORA_READONLY_ERROR = 'read-only'
      AURORA_READONLY_CHECK_QUERY = \
        "SHOW GLOBAL VARIABLES LIKE '%s';"

      AURORA_CONNECTION_ERRORS = [
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
        aurora_reconnect!
      end

      def aurora_reconnect!
        query_options = {}
        unless @client.nil?
          begin
            @client.close
            query_options = (@client.query_options&.dup || {})
          rescue => e
            warn "[mysql2-aurora] reconnect! error: #{e.message} (#{e.class})"
          end
        end

        @client = ::Mysql2::Aurora::ORIGINAL_CLIENT_CLASS.new(@opts)
        @client.query_options.merge!(query_options)
      end

      # Execute query with reconnect
      # @note [Override] with reconnect.
      def query(*args)
        aurora_reconnect! if client.closed?
        begin
          client.query(*args)
        rescue ::Mysql2::Error => e
          if aurora_connection_error?(e.message)
            aurora_wait_for_availability_after(e)
            aurora_reconnect!
          end

          raise e
        end
      end

      def aurora_readonly_error?(message)
        message.include?(AURORA_READONLY_ERROR)
      end

      def aurora_connection_error?(message)
        AURORA_CONNECTION_ERRORS.any? do |matching_str|
          message.include?(matching_str)
        end
      end

      def aurora_wait_for_availability_after(error)
        warn "[mysql2-aurora] auto reconnect origin error: #{error.message}, max retries: #{@max_retry}"
        try_count = 1
        begin
          retry_interval_seconds = [1.5 * (try_count - 1), 10].min

          warn "[mysql2-aurora] Retry after #{retry_interval_seconds}seconds"
          sleep retry_interval_seconds
          check_client = ::Mysql2::Aurora::ORIGINAL_CLIENT_CLASS.new(@opts)

          if aurora_readonly_error?(error.message)
            # if error was a readonly type, it must check that we are not being
            # reconnected to a slave, so we ensure that we are connected to a
            # master node by checking 'innodb_read_only' MySQL's variable
            # it must be set to 'OFF' if we are on the master
            show_variables_res = check_client.query(
              format(
                AURORA_READONLY_CHECK_QUERY, ::Mysql2::Aurora.read_only_variable
              ),
              as: :array
            ).to_a

            if show_variables_res.length == 0 ||
              show_variables_res[0][1].to_s.upcase != 'OFF'
              raise AuroraReadOnlyError, show_variables_res[0][1]
            end
          else
            check_client.ping
          end
        rescue ::Mysql2::Error, AuroraReadOnlyError => e
          warn "[mysql2-aurora] auto reconnect error: #{e.message}"
          try_count += 1
          retry if try_count <= @max_retry
        end

        warn "[mysql2-aurora] auto-reconnect success"
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
