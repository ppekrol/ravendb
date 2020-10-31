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
        public async Task HugeDocuments()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            using (context.OpenReadTransaction())
            {
                await writer.WriteStartObjectAsync();
                await writer.WritePropertyNameAsync("Results");

                await writer.WriteStartArrayAsync();

                var isFirst = true;

                foreach (var pair in context.DocumentDatabase.HugeDocuments.GetHugeDocuments())
                {
                    if (isFirst == false)
                        await writer.WriteCommaAsync();

                    isFirst = false;

                    await writer.WriteStartObjectAsync();

                    await writer.WritePropertyNameAsync("Id");
                    await writer.WriteStringAsync(pair.Key.Item1);

                    await writer.WriteCommaAsync();

                    await writer.WritePropertyNameAsync("Size");
                    await writer.WriteIntegerAsync(pair.Value);

                    await writer.WriteCommaAsync();

                    await writer.WritePropertyNameAsync("LastAccess");
                    await writer.WriteStringAsync(pair.Key.Item2.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite, CultureInfo.InvariantCulture));

                    await writer.WriteEndObjectAsync();
                }

                await writer.WriteEndArrayAsync();

                await writer.WriteEndObjectAsync();
            }
        }
    }
}
