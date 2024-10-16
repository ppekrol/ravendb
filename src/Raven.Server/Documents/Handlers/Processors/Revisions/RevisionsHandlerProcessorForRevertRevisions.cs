﻿using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Revisions
{
    internal sealed class RevisionsHandlerProcessorForRevertRevisions : AbstractRevisionsHandlerProcessorForRevertRevisions<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public RevisionsHandlerProcessorForRevertRevisions([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override void ScheduleRevertRevisions(long operationId, RevertRevisionsRequest configuration, OperationCancelToken token)
        {
            var collections = configuration.Collections?.Length > 0 ? new HashSet<string>(configuration.Collections, StringComparer.OrdinalIgnoreCase) : null;

            var t = RequestHandler.Database.Operations.AddLocalOperation(
                operationId,
                OperationType.DatabaseRevert,
                $"Revert database '{RequestHandler.Database.Name}' to {configuration.Time} UTC.",
                detailedDescription: null,
                onProgress => RequestHandler.Database.DocumentsStorage.RevisionsStorage.RevertRevisions(configuration.Time, TimeSpan.FromSeconds(configuration.WindowInSec), onProgress, collections, token),
                token: token);

            _ = t.ContinueWith(_ =>
            {
                token.Dispose();
            });
        }

        protected override long GetNextOperationId()
        {
            return RequestHandler.Database.Operations.GetNextOperationId();
        }
    }
}
