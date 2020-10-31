using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class RevisionsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/documents/get-revisions", "GET", AuthorizationStatus.ValidUser)]
        public async Task GetRevisions()
        {
            var etag = GetLongQueryString("etag", false) ?? 0;
            var pageSize = GetPageSize();

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                await writer.WriteStartObjectAsync();
                await writer.WritePropertyNameAsync("Revisions");
                await writer.WriteStartArrayAsync();

                var first = true;
                foreach (var revision in Database.DocumentsStorage.RevisionsStorage.GetRevisionsFrom(context, etag, pageSize))
                {
                    if (first == false)
                        await writer.WriteCommaAsync();
                    first = false;

                    await writer.WriteStartObjectAsync();

                    await writer.WritePropertyNameAsync(nameof(Document.Id));
                    await writer.WriteStringAsync(revision.Id);
                    await writer.WriteCommaAsync();

                    await writer.WritePropertyNameAsync(nameof(Document.Etag));
                    await writer.WriteIntegerAsync(revision.Etag);
                    await writer.WriteCommaAsync();

                    await writer.WritePropertyNameAsync(nameof(Document.LastModified));
                    await writer.WriteDateTimeAsync(revision.LastModified, true);
                    await writer.WriteCommaAsync();

                    await writer.WritePropertyNameAsync(nameof(Document.ChangeVector));
                    await writer.WriteStringAsync(revision.ChangeVector);

                    await writer.WriteEndObjectAsync();
                }

                await writer.WriteEndArrayAsync();
                await writer.WriteEndObjectAsync();
            }
        }
    }
}
