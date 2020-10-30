﻿using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class CollectionsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/collections/stats", "GET", AuthorizationStatus.ValidUser)]
        public Task GetCollectionStats()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                DynamicJsonValue result = GetCollectionStats(context, false);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    context.WriteAsync(writer, result);
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/collections/stats/detailed", "GET", AuthorizationStatus.ValidUser)]
        public Task GetDetailedCollectionStats()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                DynamicJsonValue result = GetCollectionStats(context, true);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    context.WriteAsync(writer, result);
            }

            return Task.CompletedTask;
        }

        private DynamicJsonValue GetCollectionStats(DocumentsOperationContext context, bool detailed = false)
        {
            DynamicJsonValue collections = new DynamicJsonValue();

            DynamicJsonValue stats = new DynamicJsonValue()
            {
                [nameof(CollectionStatistics.CountOfDocuments)] = Database.DocumentsStorage.GetNumberOfDocuments(context),
                [nameof(CollectionStatistics.CountOfConflicts)] = Database.DocumentsStorage.ConflictsStorage.GetNumberOfDocumentsConflicts(context),
                [nameof(CollectionStatistics.Collections)] = collections
            };

            foreach (var collection in Database.DocumentsStorage.GetCollections(context))
            {
                if (detailed)
                {
                    collections[collection.Name] = Database.DocumentsStorage.GetCollectionDetails(context, collection.Name);
                }
                else
                {
                    collections[collection.Name] = collection.Count;
                }
            }

            return stats;
        }

        [RavenAction("/databases/*/collections/docs", "GET", AuthorizationStatus.ValidUser)]
        public Task GetCollectionDocuments()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var sw = Stopwatch.StartNew();
                var pageSize = GetPageSize();
                var documents = Database.DocumentsStorage.GetDocumentsInReverseEtagOrder(context, GetStringQueryString("name"), GetStart(), pageSize);

                long numberOfResults;
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {

                    writer.WriteStartObjectAsync();
                    writer.WritePropertyNameAsync("Results");
                    writer.WriteDocuments(context, documents, metadataOnly: false, numberOfResults: out numberOfResults);
                    writer.WriteEndObjectAsync();
                }

                AddPagingPerformanceHint(PagingOperationType.Documents, "Collection", HttpContext.Request.QueryString.Value, numberOfResults, pageSize, sw.ElapsedMilliseconds);
            }

            return Task.CompletedTask;
        }
    }
}
