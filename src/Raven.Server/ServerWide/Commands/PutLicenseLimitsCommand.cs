﻿using Raven.Server.Commercial;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    internal sealed class PutLicenseLimitsCommand : PutValueCommand<LicenseLimits>
    {
        public PutLicenseLimitsCommand()
        {
            // for deserialization
        }

        public PutLicenseLimitsCommand(string name, LicenseLimits licenseLimits, string uniqueRequestId) : base(uniqueRequestId)
        {
            Name = name;
            Value = licenseLimits;
        }

        public override DynamicJsonValue ValueToJson()
        {
            return Value?.ToJson();
        }

        public override void VerifyCanExecuteCommand(ServerStore store, TransactionOperationContext context, bool isClusterAdmin)
        {
            AssertClusterAdmin(isClusterAdmin);
        }
    }
}
