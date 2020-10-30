using System.Threading.Tasks;
using Raven.Server.Routing;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class ScriptRunnersDebugInfoHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/script-runners", "GET", AuthorizationStatus.ValidUser, IsDebugInformationEndpoint = true)]
        public Task GetJSDebugInfo()
        {
            var detailed = GetBoolValueQueryString("detailed", required: false) ?? false;

            using (Database.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObjectAsync();
                    writer.WritePropertyNameAsync("ScriptRunners");

                    writer.WriteStartArrayAsync();
                    var first = true;
                    foreach (var runnerInfo in Database.Scripts.GetDebugInfo(detailed))
                    {
                        if (first == false)
                            writer.WriteCommaAsync();
                        first = false;
                        using (var runnerInfoReader = context.ReadObject(runnerInfo, "runnerInfo"))
                            writer.WriteObjectAsync(runnerInfoReader);
                    }
                    writer.WriteEndArrayAsync();
                    writer.WriteEndObjectAsync();
                }
            }
            return Task.CompletedTask;
        }
    }
}
