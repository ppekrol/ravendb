using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.ServerWide.Tcp;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class TcpManagementHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/tcp", "GET", AuthorizationStatus.ValidUser, IsDebugInformationEndpoint = true)]
        public async Task GetAll()
        {
            var start = GetStart();
            var pageSize = GetPageSize();

            var minDuration = GetLongQueryString("minSecDuration", false);
            var maxDuration = GetLongQueryString("maxSecDuration", false);
            var ip = GetStringQueryString("ip", false);
            var operationString = GetStringQueryString("operation", false);

            TcpConnectionHeaderMessage.OperationTypes? operation = null;
            if (string.IsNullOrEmpty(operationString) == false)
                operation = (TcpConnectionHeaderMessage.OperationTypes)Enum.Parse(typeof(TcpConnectionHeaderMessage.OperationTypes), operationString, ignoreCase: true);

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var connections = Database.RunningTcpConnections
                    .Where(connection => connection.CheckMatch(minDuration, maxDuration, ip, operation))
                    .Skip(start)
                    .Take(pageSize);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    await writer.WriteStartObjectAsync();

                    await writer.WriteArrayAsync(context, "Results", connections, (w, c, connection) => c.WriteAsync(w, connection.GetConnectionStats()));

                    await writer.WriteEndObjectAsync();
                }
            }
        }

        [RavenAction("/databases/*/tcp", "DELETE", AuthorizationStatus.ValidUser)]
        public Task Delete()
        {
            var id = GetLongQueryString("id");

            var connection = Database.RunningTcpConnections
                .FirstOrDefault(x => x.Id == id);

            if (connection == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return Task.CompletedTask;
            }

            // force a disconnection
            connection.Stream.Dispose();
            connection.TcpClient.Dispose();

            return NoContent();
        }
    }
}
