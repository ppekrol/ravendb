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
        public async Task Routes()
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
               await  writer.WriteStartObjectAsync();
               await  writer.WritePropertyNameAsync("Debug");
               await  writer.WriteStartArrayAsync();
                var first = true;
                foreach (var route in debugRoutes)
                {
                    if (first == false)
                    {
                       await  writer.WriteCommaAsync();
                    }
                    first = false;

                   await  writer.WriteStartObjectAsync();
                   await  writer.WritePropertyNameAsync("Path");
                   await  writer.WriteStringAsync(route.Key);
                   await  writer.WriteCommaAsync();
                   await  writer.WritePropertyNameAsync("Methods");
                   await  writer.WriteStringAsync(string.Join(", ", route.Select(x => x.Method)));
                   await  writer.WriteEndObjectAsync();
                }
               await  writer.WriteEndArrayAsync();

               await  writer.WriteCommaAsync();
               await  writer.WritePropertyNameAsync("Production");
               await  writer.WriteStartArrayAsync();
                first = true;
                foreach (var route in productionRoutes)
                {
                    if (first == false)
                    {
                       await  writer.WriteCommaAsync();
                    }
                    first = false;

                   await  writer.WriteStartObjectAsync();
                   await  writer.WritePropertyNameAsync("Path");
                   await  writer.WriteStringAsync(route.Key);
                   await  writer.WriteCommaAsync();
                   await  writer.WritePropertyNameAsync("Methods");
                   await  writer.WriteStringAsync(string.Join(", ", route.Select(x => x.Method)));
                   await  writer.WriteEndObjectAsync();
                }
               await  writer.WriteEndArrayAsync();

               await  writer.WriteEndObjectAsync();
            }
        }
    }
}
