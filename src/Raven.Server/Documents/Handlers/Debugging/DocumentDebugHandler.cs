using System.Globalization;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class DocumentDebugHandler : DatabaseRequestHandler
    {       
        [RavenAction("/databases/*/debug/documents/huge", "GET", AuthorizationStatus.ValidUser, IsDebugInformationEndpoint = true)]
        public Task HugeDocuments()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            using (context.OpenReadTransaction())
            {
                writer.WriteStartObjectAsync();
                writer.WritePropertyNameAsync("Results");

                writer.WriteStartArrayAsync();

                var isFirst = true;

                foreach (var pair in context.DocumentDatabase.HugeDocuments.GetHugeDocuments())
                {
                    if (isFirst == false)
                        writer.WriteCommaAsync();

                    isFirst = false;

                    writer.WriteStartObjectAsync();

                    writer.WritePropertyNameAsync("Id");
                    writer.WriteStringAsync(pair.Key.Item1);

                    writer.WriteCommaAsync();

                    writer.WritePropertyNameAsync("Size");
                    writer.WriteIntegerAsync(pair.Value);

                    writer.WriteCommaAsync();

                    writer.WritePropertyNameAsync("LastAccess");
                    writer.WriteStringAsync(pair.Key.Item2.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite, CultureInfo.InvariantCulture));

                    writer.WriteEndObjectAsync();
                }

                writer.WriteEndArrayAsync();

                writer.WriteEndObjectAsync();
            }

            return Task.CompletedTask;
        }
    }
}
