﻿using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Raven.Server.Documents.Indexes.Sorting;
using Raven.Server.Json;
using Raven.Server.Logging;
using Raven.Server.Rachis;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands.Sorters;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Web.System.Sorters
{
    public class AdminSortersHandler : ServerRequestHandler
    {
        [RavenAction("/admin/sorters", "PUT", AuthorizationStatus.Operator)]
        public async Task Put()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var input = await context.ReadForMemoryAsync(RequestBodyStream(), "Sorters");
                if (input.TryGet("Sorters", out BlittableJsonReaderArray analyzers) == false)
                    ThrowRequiredPropertyNameInRequest("Sorters");

                var commands = new List<PutServerWideSorterCommand>();
                foreach (var sorterToAdd in analyzers)
                {
                    var sorterDefinition = JsonDeserializationServer.SorterDefinition((BlittableJsonReaderObject)sorterToAdd);
                    sorterDefinition.Name = sorterDefinition.Name?.Trim();

                    if (RavenLogManager.Instance.IsAuditEnabled)
                    {
                        var clientCert = GetCurrentCertificate();

                        var auditLog = RavenLogManager.Instance.GetAuditLoggerForServer();
                        auditLog.Audit($"Sorter {sorterDefinition.Name} PUT by {clientCert?.Subject} {clientCert?.Thumbprint} with definition: {sorterToAdd}");
                    }

                    sorterDefinition.Validate();

                    // check if sorter is compilable
                    SorterCompiler.Compile(sorterDefinition.Name, sorterDefinition.Code);

                    var command = new PutServerWideSorterCommand(sorterDefinition, GetRaftRequestIdFromQuery());

                    commands.Add(command);
                }

                var index = 0L;
                foreach (var command in commands)
                    index = (await ServerStore.SendToLeaderAsync(command)).Index;

                await ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, index);

                NoContentStatus(HttpStatusCode.Created);
            }
        }

        [RavenAction("/admin/sorters", "DELETE", AuthorizationStatus.Operator)]
        public async Task Delete()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            if (RavenLogManager.Instance.IsAuditEnabled)
            {
                var clientCert = GetCurrentCertificate();

                var auditLog = RavenLogManager.Instance.GetAuditLoggerForServer();
                auditLog.Audit($"Sorter {name} DELETE by {clientCert?.Subject} {clientCert?.Thumbprint}");
            }

            var command = new DeleteServerWideSorterCommand(name, GetRaftRequestIdFromQuery());
            var index = (await ServerStore.SendToLeaderAsync(command)).Index;

            await ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, index);

            NoContentStatus();
        }
    }
}
