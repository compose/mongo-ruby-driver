# Copyright (C) 2009-2014 MongoDB, Inc.
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
  class Cluster
    module Topology
      class DetectError < StandardError

      end

      # Defines behaviour when a cluster is in replica set topology.
      #
      # @since 2.0.0
      class Detect

        # Select appropriate servers for this topology.
        #
        # @example Select the servers.
        #   ReplicaSet.servers(servers, 'test')
        #
        # @param [ Array<Server> ] servers The known servers.
        # @param [ String ] replica_set_name The name of the replica set.
        #
        # @return [ Array<Server> ] The servers in the replica set.
        #
        # @since 2.0.0
        def self.servers(servers, name = nil)
          if servers.length == 1 && servers.first.standalone?
            return servers
          end

          rs = name
          standalone_count = 0
          mongos_count = 0
          other_count = 0
          total_count = 0

          servers.select do |server|
            if rs && server.replica_set_name != rs
              raise DetectError.new("Servers have non-matching replica set names")
            end

            rs ||= server.replica_set_name

            total_count += 1
            if server.standalone?
              standalone_count += 1

              if standalone_count > total_count
                raise DetectError.new("Standalone server detected, but other replica set or mongos servers in list")
              end
            elsif server.mongos?
              mongos_count += 1
              puts "Detected mongos"
              
              if mongos_count > total_count
                raise DetectError.new("Mongos detected, but replica set or standalone server exists in list")
              end
            else
              other_count += 1

              if mongos_count > total_count
                raise DetectError.new("Mongos detected, but replica set or standalone server exists in list")
              end
            end

            server.primary? || server.secondary? || server.mongos? || server.standalone?
          end

        end
      end
    end
  end
end
