using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class AdminScriptRunnersDebugInfoHandler : RequestHandler
    {
        [RavenAction("/admin/debug/script-runners", "GET", AuthorizationStatus.Operator)]
        public async Task GetJSAdminDebugInfo()
        {
            var detailed = GetBoolValueQueryString("detailed", required: false) ?? false;

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    await writer.WriteStartObjectAsync();
                    await writer.WritePropertyNameAsync("ScriptRunners");

                    await writer.WriteStartArrayAsync();
                    var first = true;
                    foreach (var runnerInfo in Server.AdminScripts.GetDebugInfo(detailed))
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
