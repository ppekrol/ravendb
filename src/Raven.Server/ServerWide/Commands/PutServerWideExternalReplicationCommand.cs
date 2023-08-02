﻿using System;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.ServerWide.Operations.OngoingTasks;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    internal sealed class PutServerWideExternalReplicationCommand : UpdateValueCommand<ServerWideExternalReplication>
    {
        public PutServerWideExternalReplicationCommand()
        {
            // for deserialization
        }

        public PutServerWideExternalReplicationCommand(ServerWideExternalReplication configuration, string uniqueRequestId) : base(uniqueRequestId)
        {
            Name = ClusterStateMachine.ServerWideConfigurationKey.ExternalReplication;
            Value = configuration;
        }

        public override object ValueToJson()
        {
            return Value.ToJson();
        }

        public override BlittableJsonReaderObject GetUpdatedValue(JsonOperationContext context, BlittableJsonReaderObject previousValue, long index)
        {
            if (string.IsNullOrWhiteSpace(Value.Name))
                Value.Name = GenerateTaskName(previousValue);

            if (Value.ExcludedDatabases != null &&
                Value.ExcludedDatabases.Any(string.IsNullOrWhiteSpace))
                throw new RachisApplyException($"{nameof(ServerWideExternalReplication.ExcludedDatabases)} cannot contain null or empty database names");

            var originTaskId = Value.TaskId; 
            Value.TaskId = index;

            if (previousValue != null)
            {
                var lazy = context.GetLazyString(Value.Name);
                if (previousValue.Contains(lazy) == false)
                {
                    //The name have might modified so we search by index/TaskId
                    foreach (var propertyName in previousValue.GetPropertyNames())
                    {
                        var value = previousValue[propertyName] as BlittableJsonReaderObject;
                        Debug.Assert(value != null);
                        if (value.TryGet(nameof(ServerWideExternalReplication.TaskId), out long taskId) && taskId == originTaskId)
                        {
                            previousValue.Modifications = new DynamicJsonValue(previousValue);
                            previousValue.Modifications.Remove(propertyName);
                            break;
                        }
                    }
                }
                previousValue.Modifications ??= new DynamicJsonValue();
                previousValue.Modifications[Value.Name] = Value.ToJson();
                
                return context.ReadObject(previousValue, Name);
            }

            var djv = new DynamicJsonValue
            {
                [Value.Name] = Value.ToJson()
            };

            return context.ReadObject(djv, Name);
        }

        private string GenerateTaskName(BlittableJsonReaderObject previousValue)
        {
            var baseTaskName = Value.GetDefaultTaskName();
            if (previousValue == null)
                return baseTaskName;

            long i = 1;
            var taskName = baseTaskName;
            var allTaskNames = previousValue.GetPropertyNames();
            while (allTaskNames.Contains(taskName, StringComparer.OrdinalIgnoreCase))
            {
                taskName += $" #{++i}";
            }

            return taskName;
        }

        public override void VerifyCanExecuteCommand(ServerStore store, TransactionOperationContext context, bool isClusterAdmin)
        {
            AssertClusterAdmin(isClusterAdmin);
        }

        public static string GetTaskName(string serverWideName)
        {
            return $"{ServerWideExternalReplication.NamePrefix}, {serverWideName}";
        }

        public static string GetRavenConnectionStringName(string serverWideName)
        {
            return $"{ServerWideExternalReplication.RavenConnectionStringPrefix} for {serverWideName}";
        }

        public static RavenConnectionString UpdateExternalReplicationTemplateForDatabase(ExternalReplication configuration, string databaseName, string[] topologyDiscoveryUrls)
        {
            var serverWideName = configuration.Name;
            configuration.Name = GetTaskName(serverWideName);
            configuration.Database = databaseName;
            configuration.ConnectionStringName = GetRavenConnectionStringName(serverWideName);

            return new RavenConnectionString
            {
                Name = configuration.ConnectionStringName,
                Database = databaseName,
                TopologyDiscoveryUrls = topologyDiscoveryUrls
            };
        }
    }
}
