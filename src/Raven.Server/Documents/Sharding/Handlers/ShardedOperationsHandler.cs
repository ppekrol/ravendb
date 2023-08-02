﻿using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Operations;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    internal sealed class ShardedOperationsHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/operations/next-operation-id", "GET")]
        public async Task GetNextOperationId()
        {
            using (var processor = new ShardedOperationsHandlerProcessorForGetNextOperationId(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/operations/kill", "POST")]
        public async Task Kill()
        {
            using (var processor = new ShardedOperationsHandlerProcessorForKill(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/operations", "GET")]
        public async Task GetAll()
        {
            using (var processor = new ShardedOperationsHandlerProcessorForGetAll(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/operations/state", "GET")]
        public async Task State()
        {
            using (var processor = new ShardedOperationsHandlerProcessorForState(this))
                await processor.ExecuteAsync();
        }
    }
}
