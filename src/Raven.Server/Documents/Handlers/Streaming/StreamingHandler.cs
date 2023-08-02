﻿using System.Net.Http;
using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Streaming;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers.Streaming
{
    internal sealed class StreamingHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/streams/docs", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        public async Task StreamDocsGet()
        {
            using (var processor = new StreamingHandlerProcessorForGetDocs(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/streams/timeseries", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Stream()
        {
            using (var processor = new StreamingHandlerProcessorForGetTimeSeries(this))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenAction("/databases/*/streams/queries", "HEAD", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public Task SteamQueryHead()
        {
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/streams/queries", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        public async Task StreamQueryGet()
        {
            using (var processor = new StreamingHandlerProcessorForGetStreamQuery(this, HttpMethod.Get))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/streams/queries", "POST", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        public async Task StreamQueryPost()
        {
            using (var processor = new StreamingHandlerProcessorForGetStreamQuery(this, HttpMethod.Post))
                await processor.ExecuteAsync();
        }
    }
}
