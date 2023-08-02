﻿using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Indexes
{
    internal sealed class SetIndexLockCommand : UpdateDatabaseCommand
    {
        public string IndexName;

        public IndexLockMode LockMode;

        public SetIndexLockCommand()
        {
            // for deserialization
        }

        public SetIndexLockCommand(string name, IndexLockMode mode, string databaseName, string uniqueRequestId)
            : base(databaseName, uniqueRequestId)
        {
            IndexName = name;
            LockMode = mode;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (record.Indexes.TryGetValue(IndexName, out IndexDefinition staticIndex))
            {
                staticIndex.LockMode = LockMode;
                staticIndex.ClusterState.LastIndex = etag;
            }

            if (record.AutoIndexes.ContainsKey(IndexName))
            {
                throw new RachisApplyException($"'Lock Mode' can't be set for the Auto-Index '{IndexName}'.");
            }

        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(IndexName)] = IndexName;
            json[nameof(LockMode)] = LockMode;
        }
    }
}
