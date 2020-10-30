using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Web.System
{
    public sealed class DebugHandler : RequestHandler
    {
        [RavenAction("/debug/routes", "GET", AuthorizationStatus.ValidUser)]
        public Task Routes()
        {
            var debugRoutes = Server.Router.AllRoutes
                .Where(x => x.IsDebugInformationEndpoint)
                .GroupBy(x => x.Path)
                .OrderBy(x => x.Key);

            var productionRoutes = Server.Router.AllRoutes
              .Where(x => x.IsDebugInformationEndpoint == false)
                .GroupBy(x => x.Path)
                .OrderBy(x => x.Key);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObjectAsync();
                writer.WritePropertyNameAsync("Debug");
                writer.WriteStartArrayAsync();
                var first = true;
                foreach (var route in debugRoutes)
                {
                    if (first == false)
                    {
                        writer.WriteCommaAsync();
                    }
                    first = false;

                    writer.WriteStartObjectAsync();
                    writer.WritePropertyNameAsync("Path");
                    writer.WriteStringAsync(route.Key);
                    writer.WriteCommaAsync();
                    writer.WritePropertyNameAsync("Methods");
                    writer.WriteStringAsync(string.Join(", ", route.Select(x => x.Method)));
                    writer.WriteEndObjectAsync();
                }
                writer.WriteEndArrayAsync();

                writer.WriteCommaAsync();
                writer.WritePropertyNameAsync("Production");
                writer.WriteStartArrayAsync();
                first = true;
                foreach (var route in productionRoutes)
                {
                    if (first == false)
                    {
                        writer.WriteCommaAsync();
                    }
                    first = false;

                    writer.WriteStartObjectAsync();
                    writer.WritePropertyNameAsync("Path");
                    writer.WriteStringAsync(route.Key);
                    writer.WriteCommaAsync();
                    writer.WritePropertyNameAsync("Methods");
                    writer.WriteStringAsync(string.Join(", ", route.Select(x => x.Method)));
                    writer.WriteEndObjectAsync();
                }
                writer.WriteEndArrayAsync();

                writer.WriteEndObjectAsync();
            }

            return Task.CompletedTask;
        }
    }
}
