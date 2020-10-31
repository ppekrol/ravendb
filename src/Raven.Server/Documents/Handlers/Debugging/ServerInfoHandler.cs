using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class ServerInfoHandler : RequestHandler
    {
        [RavenAction("/debug/server-id", "GET", AuthorizationStatus.ValidUser, IsDebugInformationEndpoint = true)]
        public async Task ServerId()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                await context.WriteAsync(writer, new DynamicJsonValue
                {
                    ["ServerId"] = ServerStore.GetServerId().ToString()
                });
            }
        }
    }
}
