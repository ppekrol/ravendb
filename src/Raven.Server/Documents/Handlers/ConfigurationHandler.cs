using System.Net;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class ConfigurationHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/configuration/studio", "GET", AuthorizationStatus.ValidUser)]
        public async Task GetStudioConfiguration()
        {
            var configuration = Database.StudioConfiguration;

            if (configuration == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return;
            }

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var val = configuration.ToJson();
                var clientConfigurationJson = context.ReadObject(val, Constants.Configuration.StudioId);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    await writer.WriteObjectAsync(clientConfigurationJson);
                }
            }
        }

        [RavenAction("/databases/*/configuration/client", "GET", AuthorizationStatus.ValidUser)]
        public async Task GetClientConfiguration()
        {
            var inherit = GetBoolValueQueryString("inherit", required: false) ?? true;

            var configuration = Database.ClientConfiguration;
            var serverConfiguration = GetServerClientConfiguration();

            if (inherit && (configuration == null || configuration.Disabled) && serverConfiguration != null)
            {
                configuration = serverConfiguration;
            }

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                BlittableJsonReaderObject clientConfigurationJson = null;
                if (configuration != null)
                {
                    var val = configuration.ToJson();
                    clientConfigurationJson = context.ReadObject(val, Constants.Configuration.ClientId);
                }

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    await writer.WriteStartObjectAsync();

                    await writer.WritePropertyNameAsync(nameof(GetClientConfigurationOperation.Result.Etag));
                    await writer.WriteIntegerAsync(Database.GetClientConfigurationEtag());
                    await writer.WriteCommaAsync();

                    await writer.WritePropertyNameAsync(nameof(GetClientConfigurationOperation.Result.Configuration));
                    if (clientConfigurationJson != null)
                    {
                        await writer.WriteObjectAsync(clientConfigurationJson);
                    }
                    else
                    {
                        await writer.WriteNullAsync();
                    }

                    await writer.WriteEndObjectAsync();
                }
            }
        }

        private ClientConfiguration GetServerClientConfiguration()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                using (context.OpenReadTransaction())
                {
                    var clientConfigurationJson = ServerStore.Cluster.Read(context, Constants.Configuration.ClientId, out _);
                    var config = clientConfigurationJson != null
                        ? JsonDeserializationServer.ClientConfiguration(clientConfigurationJson)
                        : null;

                    return config;
                }
            }
        }
    }
}
