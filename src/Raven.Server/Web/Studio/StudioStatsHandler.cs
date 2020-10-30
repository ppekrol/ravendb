using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Studio;
using Raven.Server.Routing;
using Sparrow.Json;

namespace Raven.Server.Web.Studio
{
    public class StudioStatsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/studio/footer/stats", "GET", AuthorizationStatus.ValidUser)]
        public Task FooterStats()
        {
            using (var context = QueryOperationContext.Allocate(Database, needsServerContext: true))
            using (var writer = new AsyncBlittableJsonTextWriter(context.Documents, ResponseBodyStream()))
            using (context.OpenReadTransaction())
            {
                var indexes = Database.IndexStore.GetIndexes().ToList();

                writer.WriteStartObjectAsync();

                writer.WritePropertyNameAsync(nameof(FooterStatistics.CountOfDocuments));
                writer.WriteIntegerAsync(Database.DocumentsStorage.GetNumberOfDocuments(context.Documents));
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(FooterStatistics.CountOfIndexes));
                writer.WriteIntegerAsync(indexes.Count);
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(FooterStatistics.CountOfStaleIndexes));
                writer.WriteIntegerAsync(indexes.Count(i => i.IsStale(context)));
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(FooterStatistics.CountOfIndexingErrors));
                writer.WriteIntegerAsync(indexes.Sum(index => index.GetErrorCount()));

                writer.WriteEndObjectAsync();
            }

            return Task.CompletedTask;
        }
    }
}
