using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class SortersHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/sorters", "GET", AuthorizationStatus.ValidUser)]
        public async Task Get()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                Dictionary<string, SorterDefinition> sorters;
                using (context.OpenReadTransaction())
                {
                    var rawRecord = Server.ServerStore.Cluster.ReadRawDatabaseRecord(context, Database.Name);
                    sorters = rawRecord?.Sorters;
                }

                if (sorters == null)
                {
                    sorters = new Dictionary<string, SorterDefinition>();
                }

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    await writer.WriteStartObjectAsync();

                    await writer.WriteArrayAsync(context, "Sorters", sorters.Values, async (w, c, sorter) =>
                    {
                        await w.WriteStartObjectAsync();

                        await w.WritePropertyNameAsync(nameof(SorterDefinition.Name));
                        await w.WriteStringAsync(sorter.Name);
                        await w.WriteCommaAsync();

                        await w.WritePropertyNameAsync(nameof(SorterDefinition.Code));
                        await w.WriteStringAsync(sorter.Code);

                        await w.WriteEndObjectAsync();
                    });

                    await writer.WriteEndObjectAsync();
                }
            }
        }
    }
}
