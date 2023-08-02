﻿using System;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Indexes
{
    internal sealed class SetIndexStateCommand : UpdateDatabaseCommand
    {
        public string IndexName;

        public IndexState State;

        public SetIndexStateCommand()
        {
            // for deserialization
        }

        public SetIndexStateCommand(string name, IndexState state, string databaseName, string uniqueRequestId)
            : base(databaseName, uniqueRequestId)
        {
            if (string.IsNullOrEmpty(name))
                throw new RachisApplyException($"Index name cannot be null or empty");

            State = state;
            IndexName = name;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (record.AutoIndexes.TryGetValue(IndexName, out AutoIndexDefinition autoIndex))
            {
                autoIndex.State = State;
                autoIndex.ClusterState.LastIndex = etag;
                autoIndex.ClusterState.LastStateIndex = etag;
            }
            else if (record.Indexes.TryGetValue(IndexName, out IndexDefinition indexDefinition))
            {
                indexDefinition.State = State;
                indexDefinition.ClusterState.LastIndex = etag;
                indexDefinition.ClusterState.LastStateIndex = etag;
            }

        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(IndexName)] = IndexName;
            json[nameof(State)] = State;
        }
    }
}
