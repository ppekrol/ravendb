using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Commands;
using Raven.Server.Documents;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.TrafficWatch;
using Sparrow.Json;

namespace Raven.Server.Web.Operations
{
    public class OperationsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/operations/next-operation-id", "GET", AuthorizationStatus.ValidUser)]
        public async Task GetNextOperationId()
        {
            var nextId = Database.Operations.GetNextOperationId();

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    await writer.WriteStartObjectAsync();
                    await writer.WritePropertyNameAsync("Id");
                    await writer.WriteIntegerAsync(nextId);
                    await writer.WriteCommaAsync();
                    await writer.WritePropertyNameAsync(nameof(GetNextOperationIdCommand.NodeTag));
                    await writer.WriteStringAsync(Server.ServerStore.NodeTag);
                    await writer.WriteEndObjectAsync();
                }
            }
        }

        [RavenAction("/databases/*/operations/kill", "POST", AuthorizationStatus.ValidUser)]
        public Task Kill()
        {
            var id = GetLongQueryString("id");
            // ReSharper disable once PossibleInvalidOperationException
            Database.Operations.KillOperation(id);

            return NoContent();
        }

        [RavenAction("/databases/*/operations", "GET", AuthorizationStatus.ValidUser)]
        public async Task GetAll()
        {
            var id = GetLongQueryString("id", required: false);

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                IEnumerable<Documents.Operations.Operations.Operation> operations;
                if (id.HasValue == false)
                    operations = Database.Operations.GetAll();
                else
                {
                    var operation = Database.Operations.GetOperation(id.Value);
                    if (operation == null)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        return;
                    }

                    operations = new List<Documents.Operations.Operations.Operation> { operation };
                }

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    await writer.WriteStartObjectAsync();
                    await writer.WriteArrayAsync(context, "Results", operations, (w, c, operation) => c.WriteAsync(w, operation.ToJson()));
                    await writer.WriteEndObjectAsync();
                }
            }
        }

        [RavenAction("/databases/*/operations/state", "GET", AuthorizationStatus.ValidUser)]
        public async Task State()
        {
            var id = GetLongQueryString("id");
            // ReSharper disable once PossibleInvalidOperationException
            var state = Database.Operations.GetOperation(id)?.State;

            if (state == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return;
            }

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    await context.WriteAsync(writer, state.ToJson());
                    // writes Patch response
                    if (TrafficWatchManager.HasRegisteredClients)
                        AddStringToHttpContext(writer.ToString(), TrafficWatchChangeType.Operations);
                }
            }
        }
    }
}
