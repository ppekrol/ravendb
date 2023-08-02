﻿using System;
using System.Collections.Generic;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    internal sealed class DeleteServerWideBackupConfigurationCommand : UpdateValueCommand<string>
    {
        public DeleteServerWideBackupConfigurationCommand()
        {
            // for deserialization
        }

        [Obsolete("This command is obsolete, please use `DeleteServerWideTaskCommand` instead", true)]
        public DeleteServerWideBackupConfigurationCommand(string configurationName, string uniqueRequestId) : base(uniqueRequestId)
        {
        }

        public override object ValueToJson()
        {
            return Value;
        }

        public override BlittableJsonReaderObject GetUpdatedValue(JsonOperationContext context, BlittableJsonReaderObject previousValue, long index)
        {
            if (previousValue == null)
                return null;

            var propertyIndex = previousValue.GetPropertyIndex(Value);
            if (propertyIndex == -1)
                return null;

            previousValue.Modifications ??= new DynamicJsonValue();

            previousValue.Modifications.Removals = new HashSet<int>{ propertyIndex };
            return context.ReadObject(previousValue, Name);
        }

        public override void VerifyCanExecuteCommand(ServerStore store, TransactionOperationContext context, bool isClusterAdmin)
        {
            AssertClusterAdmin(isClusterAdmin);
        }
    }
}
