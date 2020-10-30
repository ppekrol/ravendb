﻿using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.IoMetrics;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class IoMetricsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/io-metrics", "GET", AuthorizationStatus.ValidUser, IsDebugInformationEndpoint = true)]
        public Task IoMetrics()
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var result = IoMetricsUtil.GetIoMetricsResponse(Database.GetAllStoragesEnvironment(), Database.GetAllPerformanceMetrics());
                context.Write(writer, result.ToJson());
            }
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/debug/io-metrics/live", "GET", AuthorizationStatus.ValidUser, SkipUsagesCount = true)]
        public async Task IoMetricsLive()
        {
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                var receiveBuffer = new ArraySegment<byte>(new byte[1024]);
                var receive = webSocket.ReceiveAsync(receiveBuffer, Database.DatabaseShutdown);

                using (var ms = new MemoryStream())
                using (var collector = new DatabaseLiveIoStatsCollector(Database))
                {
                    // 1. Send data to webSocket without making UI wait upon opening webSocket
                    await collector.SendDataOrHeartbeatToWebSocket(receive, webSocket, ms, 100);

                    // 2. Send data to webSocket when available
                    while (Database.DatabaseShutdown.IsCancellationRequested == false)
                    {
                        if (await collector.SendDataOrHeartbeatToWebSocket(receive, webSocket, ms, 4000) == false)
                        {
                            break;
                        }
                    }
                }
            }
        }

        private class DatabaseLiveIoStatsCollector : LiveIoStatsCollector<DocumentsOperationContext>
        {
            public DatabaseLiveIoStatsCollector(DocumentDatabase database) : base(database.IoChanges, database.GetAllStoragesEnvironment().ToList(), database.GetAllPerformanceMetrics(), database.DocumentsStorage.ContextPool, database.DatabaseShutdown)
            {
            }
        }
    }
}
