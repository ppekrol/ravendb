using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class AdminScriptRunnersDebugInfoHandler : RequestHandler
    {
        [RavenAction("/admin/debug/script-runners", "GET", AuthorizationStatus.Operator)]
        public Task GetJSAdminDebugInfo()
        {
            var detailed = GetBoolValueQueryString("detailed", required: false) ?? false;

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObjectAsync();
                    writer.WritePropertyNameAsync("ScriptRunners");

                    writer.WriteStartArrayAsync();
                    var first = true;
                    foreach (var runnerInfo in Server.AdminScripts.GetDebugInfo(detailed))
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
