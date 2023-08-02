﻿// ------------------------------------------------------------[-----------
//  <copyright file="ChangesHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Changes;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    internal sealed class ShardedChangesHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/changes", "GET")]
        public async Task GetChanges()
        {
            using (var processor = new ShardedChangesHandlerProcessorForGetChanges(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/changes/debug", "GET")]
        public async Task GetConnectionsDebugInfo()
        {
            using (var processor = new ShardedChangesHandlerProcessorForGetConnectionsDebugInfo(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/changes", "DELETE")]
        public async Task DeleteConnections()
        {
            using (var processor = new ShardedChangesHandlerProcessorForDeleteConnections(this))
                await processor.ExecuteAsync();
        }
    }
}
