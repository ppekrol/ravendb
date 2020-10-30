using System;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class TransactionsModeHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/transactions-mode", "GET", AuthorizationStatus.Operator)]
        public async Task CommitNonLazyTx()
        {
            var modeStr = GetQueryStringValueAndAssertIfSingleAndNotEmpty("mode");
            if (Enum.TryParse(modeStr, true, out TransactionsMode mode) == false)
                throw new InvalidOperationException("Query string value 'mode' is not a valid mode: " + modeStr);

            var configDuration = Database.Configuration.Storage.TransactionsModeDuration.AsTimeSpan;
            var duration = GetTimeSpanQueryString("duration", required: false) ?? configDuration;
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                await writer.WriteStartObjectAsync();
                await writer.WritePropertyNameAsync(("Environments"));
                await writer.WriteStartArrayAsync();
                bool first = true;
                foreach (var storageEnvironment in Database.GetAllStoragesEnvironment())
                {
                    if (storageEnvironment == null)
                        continue;

                    if (first == false)
                    {
                        await writer.WriteCommaAsync();
                    }
                    first = false;

                    var result = storageEnvironment.Environment.SetTransactionMode(mode, duration);

                    var djv = new DynamicJsonValue
                    {
                        ["Type"] = storageEnvironment.Type,
                        ["Mode"] = mode.ToString(),
                        ["Path"] = storageEnvironment.Environment.Options.BasePath.FullPath,
                    };

                    switch (result)
                    {
                        case TransactionsModeResult.ModeAlreadySet:
                            djv["Result"] = "Mode Already Set";
                            break;

                        case TransactionsModeResult.SetModeSuccessfully:
                            djv["Result"] = "Mode Set Successfully";
                            break;

                        default:
                            throw new ArgumentOutOfRangeException("Result is unexpected value: " + result);
                    }

                    await context.WriteAsync(writer, djv);
                }
                await writer.WriteEndArrayAsync();
                await writer.WriteEndObjectAsync();
            }
        }
    }
}
