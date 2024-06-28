﻿// ------------------------------------------------------------[-----------
//  <copyright file="ChangesHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Net.WebSockets;
using System.Threading.Tasks;
using NLog;
using Raven.Server.Json;
using Raven.Server.Logging;
using Raven.Server.Routing;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.TrafficWatch
{
    public class TrafficWatchHandler : ServerRequestHandler
    {
        private static readonly Logger Logger = RavenLogManager.Instance.GetLoggerForServer<TrafficWatchHandler>();

        [RavenAction("/admin/traffic-watch", "GET", AuthorizationStatus.Operator)]
        public async Task TrafficWatchWebsockets()
        {
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    try
                    {
                        var resourceName = GetStringQueryString("resourceName", required: false);
                        var connection = new TrafficWatchConnection(webSocket, resourceName, context, ServerStore.ServerShutdown);
                        TrafficWatchManager.AddConnection(connection);
                        await connection.StartSendingNotifications();
                    }
                    catch (IOException)
                    {
                        // nothing to do - connection closed
                    }
                    catch (Exception ex)
                    {
                        if (Logger.IsInfoEnabled)
                            Logger.Info(ex, "Error encountered in TrafficWatch handler");

                        try
                        {
                            await using (var ms = new MemoryStream())
                            {
                                await using (var writer = new AsyncBlittableJsonTextWriter(context, ms))
                                {
                                    context.Write(writer, new DynamicJsonValue
                                    {
                                        ["Exception"] = ex
                                    });
                                }

                                ms.TryGetBuffer(out ArraySegment<byte> bytes);
                                await webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, ServerStore.ServerShutdown);
                            }
                        }
                        catch (Exception)
                        {
                            if (Logger.IsInfoEnabled)
                                Logger.Info(ex, "Failed to send the error in TrafficWatch handler to the client");
                        }
                    }
                }
            }
        }

        [RavenAction("/admin/traffic-watch/configuration", "GET", AuthorizationStatus.Operator)]
        public async Task GetConfiguration()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var json = context.ReadObject(TrafficWatchToLog.Instance.ToJson(), "traffic-watch/configuration");
                    writer.WriteObject(json);
                }
            }
        }

        [RavenAction("/admin/traffic-watch/configuration", "POST", AuthorizationStatus.Operator)]
        public async Task SetConfiguration()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), "traffic-watch/configuration");

                var configuration = JsonDeserializationServer.Parameters.PutTrafficWatchConfigurationParameters(json);
                
                TrafficWatchToLog.Instance.UpdateConfiguration(configuration);
            }

            NoContentStatus();
        }
    }
}
