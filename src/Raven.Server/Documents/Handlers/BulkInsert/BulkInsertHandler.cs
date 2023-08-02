﻿using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.BulkInsert;
using Raven.Server.Documents.Operations;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers.BulkInsert
{
    internal sealed class BulkInsertHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/bulk_insert", "POST", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task BulkInsert()
        {
            var operationCancelToken = CreateHttpRequestBoundOperationToken();
            var id = GetLongQueryString("id");
            var skipOverwriteIfUnchanged = GetBoolValueQueryString("skipOverwriteIfUnchanged", required: false) ?? false;

            await Database.Operations.AddLocalOperation(
                id,
                OperationType.BulkInsert,
                 "Bulk Insert",
                detailedDescription: null,
                async progress =>
                {
                    using (var bulkInsertProcessor = new BulkInsertHandlerProcessor(this, Database, progress, skipOverwriteIfUnchanged, operationCancelToken.Token))
                    {
                        await bulkInsertProcessor.ExecuteAsync();

                        return bulkInsertProcessor.OperationResult;
                    }
                },
                token: operationCancelToken
            );
        }
    }
}
