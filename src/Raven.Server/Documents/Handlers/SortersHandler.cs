﻿using System.Collections.Generic;
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
        public Task Get()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                Dictionary<string, SorterDefinition> sorters;
                using (context.OpenReadTransaction())
                {
                    var rawRecord = Server.ServerStore.Cluster.ReadDatabaseRecord(context, Database.Name);
                    sorters = rawRecord?.GetSorters();
                }

                if (sorters == null)
                {
                    sorters = new Dictionary<string, SorterDefinition>();
                }

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {

                    writer.WriteStartObject();

                    writer.WriteArray(context, "Sorters", sorters.Values, (w, c, sorter) =>
                    {
                        w.WriteStartObject();

                        w.WritePropertyName(nameof(SorterDefinition.Name));
                        w.WriteString(sorter.Name);
                        w.WriteComma();

                        w.WritePropertyName(nameof(SorterDefinition.Code));
                        w.WriteString(sorter.Code);

                        w.WriteEndObject();
                    });

                    writer.WriteEndObject();
                }
            }

            return Task.CompletedTask;
        }

    }
}
