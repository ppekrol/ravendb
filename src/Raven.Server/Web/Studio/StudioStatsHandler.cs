using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Handlers.Processors.Studio;
using Raven.Server.Routing;

namespace Raven.Server.Web.Studio
{
    internal sealed class StudioStatsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/studio/footer/stats", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetFooterStats()
        {
            using (var processor = new StudioStatsHandlerProcessorForGetFooterStats(this))
            {
                await processor.ExecuteAsync();
            }
        }
    }
}
