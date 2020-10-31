using System.Threading.Tasks;
using Raven.Server.Routing;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class ScriptRunnersDebugInfoHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/script-runners", "GET", AuthorizationStatus.ValidUser, IsDebugInformationEndpoint = true)]
        public async Task GetJSDebugInfo()
        {
            var detailed = GetBoolValueQueryString("detailed", required: false) ?? false;

            using (Database.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    await writer.WriteStartObjectAsync();
                    await writer.WritePropertyNameAsync("ScriptRunners");

                    await writer.WriteStartArrayAsync();
                    var first = true;
                    foreach (var runnerInfo in Database.Scripts.GetDebugInfo(detailed))
                    {
                        if (first == false)
                            await writer.WriteCommaAsync();
                        first = false;
                        using (var runnerInfoReader = context.ReadObject(runnerInfo, "runnerInfo"))
                            await writer.WriteObjectAsync(runnerInfoReader);
                    }
                    await writer.WriteEndArrayAsync();
                    await writer.WriteEndObjectAsync();
                }
            }
        }
    }
}
