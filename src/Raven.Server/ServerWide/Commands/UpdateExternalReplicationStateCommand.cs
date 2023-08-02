﻿using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    internal sealed class UpdateExternalReplicationStateCommand : UpdateValueForDatabaseCommand
    {
        public ExternalReplicationState ExternalReplicationState { get; set; }

        private UpdateExternalReplicationStateCommand()
        {
            // for deserialization
        }

        public UpdateExternalReplicationStateCommand(string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
        }

        public override string GetItemId()
        {
            return ExternalReplicationState.GenerateItemName(DatabaseName, ExternalReplicationState.TaskId);
        }

        protected override BlittableJsonReaderObject GetUpdatedValue(long index, RawDatabaseRecord record, ClusterOperationContext context,
            BlittableJsonReaderObject existingValue)
        {
            return context.ReadObject(ExternalReplicationState.ToJson(), GetItemId());
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(ExternalReplicationState)] = ExternalReplicationState.ToJson();
        }
    }
}
