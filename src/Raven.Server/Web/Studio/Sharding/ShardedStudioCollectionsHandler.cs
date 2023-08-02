﻿using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Handlers.Processors.Studio;
using Raven.Server.Routing;
using Raven.Server.Web.Studio.Sharding.Processors;

namespace Raven.Server.Web.Studio.Sharding
{
    internal sealed class ShardedStudioCollectionsHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/studio/collections/preview", "GET")]
        public async Task PreviewCollection()
        {
            using (var processor = new ShardedStudioCollectionsHandlerProcessorForPreviewCollection(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/studio/collections/docs", "DELETE")]
        public async Task Delete()
        {
            using (var processor = new ShardedStudioCollectionHandlerProcessorForDeleteCollection(this))
                await processor.ExecuteAsync();
        }
    }
}
