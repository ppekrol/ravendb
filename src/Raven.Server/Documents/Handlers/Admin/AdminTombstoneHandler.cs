﻿using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Admin.Processors.Tombstones;
using Raven.Server.Routing;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin
{
    internal sealed class AdminTombstoneHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/tombstones/cleanup", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task Cleanup()
        {
            using (var processor = new AdminTombstoneHandlerProcessorForCleanup(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/tombstones/state", "GET", AuthorizationStatus.DatabaseAdmin, IsDebugInformationEndpoint = true)]
        public async Task State()
        {
            using (var processor = new AdminTombstoneHandlerProcessorForState(this))
                await processor.ExecuteAsync();
        }
    }
}
