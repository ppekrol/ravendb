﻿using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors;
using Raven.Server.Documents.Sharding.Handlers.Processors.Attachments;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    internal sealed class ShardedAttachmentHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/attachments", "HEAD")]
        public async Task Head()
        {
            using (var processor = new ShardedAttachmentHandlerProcessorForHeadAttachment(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/attachments", "DELETE")]
        public async Task Delete()
        {
            using (var processor = new ShardedAttachmentHandlerProcessorForDeleteAttachment(this))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenShardedAction("/databases/*/debug/attachments/hash", "GET")]
        public async Task Exists()
        {
            using (var processor = new ShardedAttachmentHandlerProcessorForGetHashCount(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/debug/attachments/metadata", "GET")]
        public async Task GetAttachmentMetadataWithCounts()
        {
            using (var processor = new ShardedAttachmentHandlerProcessorForGetAttachmentMetadataWithCounts(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/attachments", "PUT")]
        public async Task Put()
        {
            using (var processor = new ShardedAttachmentHandlerProcessorForPutAttachment(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/attachments", "GET")]
        public async Task Get()
        {
            using (var processor = new ShardedAttachmentHandlerProcessorForGetAttachment(this, isDocument: true))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/attachments", "POST")]
        public async Task GetPost()
        {
            using (var processor = new ShardedAttachmentHandlerProcessorForGetAttachment(this, isDocument: false))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/attachments/bulk", "POST")]
        public async Task GetAttachments()
        {
            using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support Get Attachments."))
                await processor.ExecuteAsync();
        }
    }
}
