﻿using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Indexes
{
    internal sealed class SetIndexPriorityCommand : UpdateDatabaseCommand
    {
        public string IndexName;

        public IndexPriority Priority;

        public SetIndexPriorityCommand()
        {
            // for deserialization
        }

        public SetIndexPriorityCommand(string name, IndexPriority priority, string databaseName, string uniqueRequestId)
            : base(databaseName, uniqueRequestId)
        {
            IndexName = name;
            Priority = priority;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (record.Indexes.TryGetValue(IndexName, out IndexDefinition staticIndex))
            {
                staticIndex.Priority = Priority;
                staticIndex.ClusterState.LastIndex = etag;
            }

            if (record.AutoIndexes.TryGetValue(IndexName, out AutoIndexDefinition autoIndex))
            {
                autoIndex.Priority = Priority;
                autoIndex.ClusterState.LastIndex = etag;
            }
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(IndexName)] = IndexName;
            json[nameof(Priority)] = Priority;
        }
    }
}
