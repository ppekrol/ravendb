﻿using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.PeriodicBackup
{
    internal sealed class UpdatePeriodicBackupStatusCommand : UpdateValueForDatabaseCommand
    {
        public PeriodicBackupStatus PeriodicBackupStatus;

        // ReSharper disable once UnusedMember.Local
        private UpdatePeriodicBackupStatusCommand()
        {
            // for deserialization
        }

        public UpdatePeriodicBackupStatusCommand(string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
        }

        public override string GetItemId()
        {
            return PeriodicBackupStatus.GenerateItemName(DatabaseName, PeriodicBackupStatus.TaskId);
        }

        protected override BlittableJsonReaderObject GetUpdatedValue(long index, RawDatabaseRecord record, ClusterOperationContext context,
            BlittableJsonReaderObject existingValue)
        {
            return context.ReadObject(PeriodicBackupStatus.ToJson(), GetItemId());
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(PeriodicBackupStatus)] = PeriodicBackupStatus.ToJson();
        }
    }
}
