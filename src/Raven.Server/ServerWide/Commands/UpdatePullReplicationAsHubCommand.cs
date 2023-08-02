﻿using System.Collections.Generic;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    internal sealed class UpdatePullReplicationAsHubCommand : UpdateDatabaseCommand
    {
        public PullReplicationDefinition Definition;

        public UpdatePullReplicationAsHubCommand() { }

        public UpdatePullReplicationAsHubCommand(string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (Definition.TaskId == 0)
            {
                Definition.TaskId = etag;
            }
            else
            {
                foreach (var task in record.HubPullReplications)
                {
                    if (task.TaskId != Definition.TaskId)
                        continue;
                    record.HubPullReplications.Remove(task);
                    break;
                }
            }
            
            record.EnsureTaskNameIsNotUsed(Definition.Name);
            record.HubPullReplications.Add(Definition);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Definition)] = Definition.ToJson();
        }
    }
}
