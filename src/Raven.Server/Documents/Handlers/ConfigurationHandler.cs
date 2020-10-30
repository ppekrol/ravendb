using System.Net;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class ConfigurationHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/configuration/studio", "GET", AuthorizationStatus.ValidUser)]
        public Task GetStudioConfiguration()
        {
            var configuration = Database.StudioConfiguration;

            if (configuration == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return Task.CompletedTask;
            }

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var val = configuration.ToJson();
                var clientConfigurationJson = context.ReadObject(val, Constants.Configuration.StudioId);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteObjectAsync(clientConfigurationJson);
                }
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/configuration/client", "GET", AuthorizationStatus.ValidUser)]
        public Task GetClientConfiguration()
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
                    writer.WriteStartObjectAsync();

                    writer.WritePropertyNameAsync(nameof(GetClientConfigurationOperation.Result.Etag));
                    writer.WriteIntegerAsync(Database.GetClientConfigurationEtag());
                    writer.WriteCommaAsync();

                    writer.WritePropertyNameAsync(nameof(GetClientConfigurationOperation.Result.Configuration));
                    if (clientConfigurationJson != null)
                    {
                        writer.WriteObjectAsync(clientConfigurationJson);
                    }
                    else
                    {
                        writer.WriteNullAsync();
                    }

                    writer.WriteEndObjectAsync();
                }
            }

            return Task.CompletedTask;
        }

        private ClientConfiguration GetServerClientConfiguration()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                using (context.OpenReadTransaction())
                {
                    var clientConfigurationJson = ServerStore.Cluster.Read(context, Constants.Configuration.ClientId, out _);
                    var config =  clientConfigurationJson != null
                        ? JsonDeserializationServer.ClientConfiguration(clientConfigurationJson)
                        : null;

                    return config;
                }
            }
        }
    }
}
