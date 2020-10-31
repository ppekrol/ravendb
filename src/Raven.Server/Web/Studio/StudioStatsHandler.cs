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
        public async Task FooterStats()
        {
            using (var context = QueryOperationContext.Allocate(Database, needsServerContext: true))
            await using (var writer = new AsyncBlittableJsonTextWriter(context.Documents, ResponseBodyStream()))
            using (context.OpenReadTransaction())
            {
                var indexes = Database.IndexStore.GetIndexes().ToList();

                await writer.WriteStartObjectAsync();

                await writer.WritePropertyNameAsync(nameof(FooterStatistics.CountOfDocuments));
                await writer.WriteIntegerAsync(Database.DocumentsStorage.GetNumberOfDocuments(context.Documents));
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(FooterStatistics.CountOfIndexes));
                await writer.WriteIntegerAsync(indexes.Count);
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(FooterStatistics.CountOfStaleIndexes));
                await writer.WriteIntegerAsync(indexes.Count(i => i.IsStale(context)));
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(FooterStatistics.CountOfIndexingErrors));
                await writer.WriteIntegerAsync(indexes.Sum(index => index.GetErrorCount()));

                await writer.WriteEndObjectAsync();
            }
        }
    }
}
