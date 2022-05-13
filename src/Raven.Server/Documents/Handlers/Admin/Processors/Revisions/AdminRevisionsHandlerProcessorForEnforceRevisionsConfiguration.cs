﻿using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Operations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Revisions
{
    internal class AdminRevisionsHandlerProcessorForEnforceRevisionsConfiguration : AbstractAdminRevisionsHandlerProcessorForEnforceRevisionsConfiguration<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public AdminRevisionsHandlerProcessorForEnforceRevisionsConfiguration([NotNull] DatabaseRequestHandler requestHandler) 
            : base(requestHandler)
        {
        }

        protected override ValueTask AddOperationAsync(long operationId, OperationCancelToken token)
        {
            var t = RequestHandler.Database.Operations.AddOperation(
                RequestHandler.Database.Name,
                $"Enforce revision configuration in database '{RequestHandler.Database.Name}'.",
                OperationType.EnforceRevisionConfiguration,
                onProgress => RequestHandler.Database.DocumentsStorage.RevisionsStorage.EnforceConfiguration(onProgress, token),
                operationId,
                token: token);
            return ValueTask.CompletedTask;
        }
    }
}
