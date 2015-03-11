# Copyright (C) 2014-2015 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo
  module Operation
    module Write
      class BulkDelete

        # Defines custom behaviour of results when deleting.
        #
        # @since 2.0.0
        class Result < Operation::Result

          # The aggregate number of deleted docs reported in the replies.
          #
          # @since 2.0.0
          REMOVED = 'nRemoved'.freeze

          # Gets the number of documents deleted.
          #
          # @example Get the deleted count.
          #   result.n_removed
          #
          # @return [ Integer ] The number of documents deleted.
          #
          # @since 2.0.0
          def n_removed
            return 0 unless acknowledged?
            @replies.reduce(0) do |n, reply|
              n += reply.documents.first[N]
            end
          end

          # Aggregate the write errors returned from this result.
          #
          # @example Aggregate the write errors.
          #   result.aggregate_write_errors
          #
          # @return [ Array ] The aggregate write errors.
          #
          # @since 2.0.0
          def aggregate_write_errors(indexes)
            @replies.reduce(nil) do |errors, reply|
              if reply.documents.first['writeErrors']
                write_errors = reply.documents.first['writeErrors'].collect do |we|
                  we.merge!('index' => indexes[we['index']])
                end
                (errors || []) << write_errors if write_errors
              end
            end
          end

          # Aggregate the write concern errors returned from this result.
          #
          # @example Aggregate the write concern errors.
          #   result.aggregate_write_concern_errors
          #
          # @return [ Array ] The aggregate write concern errors.
          #
          # @since 2.0.0
          def aggregate_write_concern_errors(indexes)
            @replies.each_with_index.reduce(nil) do |errors, (reply, i)|
              if write_concern_errors = reply.documents.first['writeConcernErrors']
                (errors || []) << write_concern_errors.reduce(nil) do |errs, wce|
                    wce.merge!('index' => indexes[wce['index']])
                    (errs || []) << write_concern_error
                end
              end
            end
          end
        end

        # Defines custom behaviour of results when deleting.
        # For server versions < 2.5.5 (that don't use write commands).
        #
        # @since 2.0.0
        class LegacyResult < Operation::Result

          # Gets the number of documents deleted.
          #
          # @example Get the deleted count.
          #   result.n_removed
          #
          # @return [ Integer ] The number of documents deleted.
          #
          # @since 2.0.0
          def n_removed
            return 0 unless acknowledged?
            @replies.reduce(0) do |n, reply|
              n += reply.documents.first[N]
            end
          end

          # Aggregate the write errors returned from this result.
          #
          # @example Aggregate the write errors.
          #   result.aggregate_write_errors
          #
          # @return [ Array ] The aggregate write errors.
          #
          # @since 2.0.0
          def aggregate_write_errors(indexes)
            @replies.each_with_index.reduce(nil) do |errors, (reply, i)|
              if reply_write_errors?(reply)
                errors ||= []
                errors << { 'errmsg' => reply.documents.first[Error::ERROR],
                            'index' => indexes[i],
                            'code' => reply.documents.first[Error::CODE] }
              end
              errors
            end
          end

          # Aggregate the write concern errors returned from this result.
          #
          # @example Aggregate the write concern errors.
          #   result.aggregate_write_concern_errors
          #
          # @return [ Array ] The aggregate write concern errors.
          #
          # @since 2.0.0
          def aggregate_write_concern_errors(indexes)
            @replies.each_with_index.find do |reply, i|
              if error = reply_write_errors?(reply)
                if note = reply.documents.first['wnote'] || reply.documents.first['jnote']
                  code = reply.documents.first['code'] || "bad value constant"
                  error_string = "#{code}: #{note}"
                elsif error == 'timeout'
                  code = reply.documents.first['code'] || "unknown error constant"
                  error_string = "#{code}: #{error}"
                end
                { 'errmsg' => error_string,
                  'index' => indexes[i],
                  'code' => code } if error_string
              end
            end
          end

          private

          def reply_write_errors?(reply)
            reply.documents.first[Error::ERROR] ||
              reply.documents.first[Error::ERRMSG]
          end
        end
      end
    end
  end
end
