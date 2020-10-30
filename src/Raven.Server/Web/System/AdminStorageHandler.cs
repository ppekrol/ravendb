using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;

namespace Raven.Server.Web.System
{
    public class AdminStorageHandler : RequestHandler
    {
        [RavenAction("/admin/debug/storage/environment/report", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = false)]
        public async Task SystemEnvironmentReport()
        {
            var details = GetBoolValueQueryString("details", required: false) ?? false;
            var env = ServerStore._env;

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                await  writer.WriteStartObjectAsync();
                await  writer.WritePropertyNameAsync("Environment");
                await  writer.WriteStringAsync("Server");
                await  writer.WriteCommaAsync();

                await  writer.WritePropertyNameAsync("Type");
                await  writer.WriteStringAsync(nameof(StorageEnvironmentWithType.StorageEnvironmentType.System));
                await  writer.WriteCommaAsync();

                using (var tx = env.ReadTransaction())
                {
                    var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(env.GenerateDetailedReport(tx, details));
                    await  writer.WritePropertyNameAsync("Report");
                    await  writer.WriteObjectAsync(context.ReadObject(djv, "System"));
                }

                await  writer.WriteEndObjectAsync();
            }
        }
    }
}
