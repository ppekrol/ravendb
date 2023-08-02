﻿using System.Collections.Generic;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Commands.Indexes;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    internal sealed class RemoveNodeFromDatabaseCommand : UpdateDatabaseCommand
    {
        public string NodeTag;
        public string DatabaseId;

        public RemoveNodeFromDatabaseCommand()
        {
        }

        public RemoveNodeFromDatabaseCommand(string databaseName, string databaseId, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            DatabaseId = databaseId;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            DeletionInProgressStatus deletionStatus = DeletionInProgressStatus.No;
            record.DeletionInProgress?.TryGetValue(NodeTag, out deletionStatus);
            if (deletionStatus == DeletionInProgressStatus.No)
                return;

            record.Topology.RemoveFromTopology(NodeTag);
            record.DeletionInProgress?.Remove(NodeTag);

            if (DatabaseId == null)
                return;

            if (deletionStatus == DeletionInProgressStatus.HardDelete)
            {
                if (record.UnusedDatabaseIds == null)
                    record.UnusedDatabaseIds = new HashSet<string>();

                record.UnusedDatabaseIds.Add(DatabaseId);
            }

            if (record.RollingIndexes == null)
                return;

            foreach (var rollingIndex in record.RollingIndexes)
            {
                if (rollingIndex.Value.ActiveDeployments.ContainsKey(NodeTag))
                {
                    // we use a dummy command to update the record as if the indexing on the removed node was completed
                    var dummy = new PutRollingIndexCommand(DatabaseName, rollingIndex.Key, NodeTag, finishedAt: null, "dummy update");
                    dummy.UpdateDatabaseRecord(record, etag);
                    rollingIndex.Value.ActiveDeployments.Remove(NodeTag);
                }
            }
        }

        public string UpdateShardedDatabaseRecord(DatabaseRecord record, int shardNumber, long etag)
        {
            record.Sharding.Shards[shardNumber].RemoveFromTopology(NodeTag);
            record.DeletionInProgress?.Remove(DatabaseRecord.GetKeyForDeletionInProgress(NodeTag, shardNumber));

            if (DatabaseId == null)
                return null;

            if (record.UnusedDatabaseIds == null)
                record.UnusedDatabaseIds = new HashSet<string>();

            record.UnusedDatabaseIds.Add(DatabaseId);

            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(NodeTag)] = NodeTag;
            json[nameof(RaftCommandIndex)] = RaftCommandIndex;
            json[nameof(DatabaseId)] = DatabaseId;
        }
    }
}
