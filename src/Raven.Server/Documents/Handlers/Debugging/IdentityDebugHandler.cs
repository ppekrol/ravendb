﻿using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class IdentityDebugHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/identities", "GET", AuthorizationStatus.ValidUser, IsDebugInformationEndpoint = true)]
        public async Task GetIdentities()
        {
            var start = GetStart();
            var pageSize = GetPageSize();

            using (Database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    await writer.WriteStartObjectAsync();

                    var first = true;
                    foreach (var identity in Database.ServerStore.Cluster.GetIdentitiesFromPrefix(context, Database.Name, start, pageSize))
                    {
                        if (first == false)
                            await writer.WriteCommaAsync();

                        first = false;
                        await writer.WritePropertyNameAsync(identity.Prefix);
                        await writer.WriteIntegerAsync(identity.Value);
                    }

                    await writer.WriteEndObjectAsync();
                }
            }
        }
    }
}
