using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class RevisionsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/documents/get-revisions", "GET", AuthorizationStatus.ValidUser)]
        public Task GetRevisions()
        {
            var etag = GetLongQueryString("etag", false) ?? 0;
            var pageSize = GetPageSize();
            
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObjectAsync();
                writer.WritePropertyNameAsync("Revisions");
                writer.WriteStartArrayAsync();
                
                var first = true;
                foreach (var revision in Database.DocumentsStorage.RevisionsStorage.GetRevisionsFrom(context, etag, pageSize))
                {
                    if (first == false)
                        writer.WriteCommaAsync();
                    first = false;

                    writer.WriteStartObjectAsync();
                    
                    writer.WritePropertyNameAsync(nameof(Document.Id));
                    writer.WriteStringAsync(revision.Id);
                    writer.WriteCommaAsync();
                    
                    writer.WritePropertyNameAsync(nameof(Document.Etag));
                    writer.WriteIntegerAsync(revision.Etag);
                    writer.WriteCommaAsync();
                    
                    writer.WritePropertyNameAsync(nameof(Document.LastModified));
                    writer.WriteDateTimeAsync(revision.LastModified, true);
                    writer.WriteCommaAsync();
                                        
                    writer.WritePropertyNameAsync(nameof(Document.ChangeVector));
                    writer.WriteStringAsync(revision.ChangeVector);
                    
                    writer.WriteEndObjectAsync();
                }
                
                writer.WriteEndArrayAsync();
                writer.WriteEndObjectAsync();
            }
            return Task.CompletedTask;
        }
    }
}
