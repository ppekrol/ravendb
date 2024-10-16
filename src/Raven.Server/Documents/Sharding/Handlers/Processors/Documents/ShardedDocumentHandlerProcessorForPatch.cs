﻿using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Handlers.Processors.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Documents;

internal sealed class ShardedDocumentHandlerProcessorForPatch : AbstractDocumentHandlerProcessorForPatch<ShardedDocumentHandler, TransactionOperationContext>
{
    public ShardedDocumentHandlerProcessorForPatch([NotNull] ShardedDocumentHandler requestHandler) : base(requestHandler)
    {
    }

    protected override async ValueTask HandleDocumentPatchAsync(string id, string changeVector, BlittableJsonReaderObject patchRequest, bool skipPatchIfChangeVectorMismatch, bool returnDebugInformation, bool test, TransactionOperationContext context)
    {
        var command = new PatchOperation.PatchCommand(RequestHandler.ShardExecutor.Conventions, id, changeVector, patchRequest, skipPatchIfChangeVectorMismatch, returnDebugInformation, test);

        int shardNumber = RequestHandler.DatabaseContext.GetShardNumberFor(context, id);

        using (var token = RequestHandler.CreateHttpRequestBoundOperationToken())
        {
            var proxyCommand = new ProxyCommand<PatchResult>(command, HttpContext.Response);
            await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(proxyCommand, shardNumber, token.Token);
        }
    }
}
