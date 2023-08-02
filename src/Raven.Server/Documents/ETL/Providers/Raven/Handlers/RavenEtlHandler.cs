﻿using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.Raven.Handlers.Processors;
using Raven.Server.Routing;

namespace Raven.Server.Documents.ETL.Providers.Raven.Handlers
{
    internal sealed class RavenEtlHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/etl/raven/test", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task PostScriptTest()
        {
            using (var processor = new EtlHandlerProcessorForTest(this))
                await processor.ExecuteAsync();
        }
    }
}
