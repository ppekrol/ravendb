﻿// ------------------------------------------------------------[-----------
//  <copyright file="ChangesHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Globalization;
using System.IO;
using System.Net.WebSockets;
using System.Threading.Tasks;
using Raven.Client.Extensions;
using Raven.Server.Routing;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class ChangesHandler : DatabaseRequestHandler
    {
        private static readonly string StudioMarker = "fromStudio";

        [RavenAction("/databases/*/changes", "GET", AuthorizationStatus.ValidUser, SkipUsagesCount = true, DisableOnCpuCreditsExhaustion = true)]
        public async Task GetChanges()
        {
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    try
                    {
                        await HandleConnection(webSocket, context);
                    }
                    catch (OperationCanceledException)
                    {
                    }
                    catch (Exception ex)
                    {
                        if (Logger.IsInfoEnabled)
                            Logger.Info("Error encountered in changes handler", ex);

                        try
                        {
                            await using (var ms = new MemoryStream())
                            {
                                await using (var writer = new AsyncBlittableJsonTextWriter(context, ms))
                                {
                                    await context.WriteAsync(writer, new DynamicJsonValue
                                    {
                                        ["Type"] = "Error",
                                        ["Exception"] = ex.ToString()
                                    });
                                }

                                ms.TryGetBuffer(out ArraySegment<byte> bytes);
                                await webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, Database.DatabaseShutdown);
                            }
                        }
                        catch (Exception)
                        {
                            if (Logger.IsInfoEnabled)
                                Logger.Info("Failed to send the error in changes handler to the client", ex);
                        }
                    }
                }
            }
        }

        [RavenAction("/databases/*/changes/debug", "GET", AuthorizationStatus.ValidUser)]
        public async Task GetConnectionsDebugInfo()
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                await writer.WriteStartObjectAsync();
                await writer.WritePropertyNameAsync("Connections");

                await writer.WriteStartArrayAsync();
                var first = true;
                foreach (var connection in Database.Changes.Connections)
                {
                    if (first == false)
                        await writer.WriteCommaAsync();
                    first = false;
                    await context.WriteAsync(writer, connection.Value.GetDebugInfo());
                }
                await writer.WriteEndArrayAsync();

                await writer.WriteEndObjectAsync();
            }
        }

        private async Task HandleConnection(WebSocket webSocket, JsonOperationContext context)
        {
            var fromStudio = GetBoolValueQueryString(StudioMarker, false) ?? false;
            var throttleConnection = GetBoolValueQueryString("throttleConnection", false).GetValueOrDefault(false);

            var connection = new ChangesClientConnection(webSocket, Database, fromStudio);
            Database.Changes.Connect(connection);
            var sendTask = connection.StartSendingNotifications(throttleConnection);
            var debugTag = "changes/" + connection.Id;
            using (context.GetMemoryBuffer(out JsonOperationContext.MemoryBuffer segment1))
            using (context.GetMemoryBuffer(out JsonOperationContext.MemoryBuffer segment2))
            {
                try
                {
                    var segments = new[] { segment1, segment2 };
                    int index = 0;
                    var receiveAsync = webSocket.ReceiveAsync(segments[index].Memory.Memory, Database.DatabaseShutdown);
                    var jsonParserState = new JsonParserState();
                    using (var parser = new UnmanagedJsonParser(context, jsonParserState, debugTag))
                    {
                        connection.SendSupportedFeatures();

                        var result = await receiveAsync;
                        Database.DatabaseShutdown.ThrowIfCancellationRequested();

                        parser.SetBuffer(segments[index], 0, result.Count);
                        index++;
                        receiveAsync = webSocket.ReceiveAsync(segments[index].Memory.Memory, Database.DatabaseShutdown);

                        while (true)
                        {
                            using (var builder =
                                new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None, debugTag, parser, jsonParserState))
                            {
                                parser.NewDocument();
                                builder.ReadObjectDocument();

                                while (builder.Read() == false)
                                {
                                    result = await receiveAsync;
                                    Database.DatabaseShutdown.ThrowIfCancellationRequested();

                                    parser.SetBuffer(segments[index], 0, result.Count);
                                    if (++index >= segments.Length)
                                        index = 0;
                                    receiveAsync = webSocket.ReceiveAsync(segments[index].Memory.Memory, Database.DatabaseShutdown);
                                }

                                builder.FinalizeDocument();

                                using (var reader = builder.CreateReader())
                                {
                                    if (reader.TryGet("Command", out string command) == false)
                                        throw new ArgumentNullException(nameof(command), "Command argument is mandatory");

                                    reader.TryGet("Param", out string commandParameter);
                                    reader.TryGet("Params", out BlittableJsonReaderArray commandParameters);

                                    connection.HandleCommand(command, commandParameter, commandParameters);

                                    if (reader.TryGet("CommandId", out int commandId))
                                    {
                                        connection.Confirm(commandId);
                                    }
                                }
                            }
                        }
                    }
                }
                catch (IOException ex)
                {
                    /* Client was disconnected, write to log */
                    if (Logger.IsInfoEnabled)
                        Logger.Info("Client was disconnected", ex);
                }
                catch (Exception ex)
                {
#pragma warning disable 4014
                    sendTask.IgnoreUnobservedExceptions();
#pragma warning restore 4014

                    // if we received close from the client, we want to ignore it and close the websocket (dispose does it)
                    if (ex is WebSocketException webSocketException
                        && webSocketException.WebSocketErrorCode == WebSocketError.InvalidState
                        && webSocket.State == WebSocketState.CloseReceived)
                    {
                        // ignore
                    }
                    else
                    {
                        throw;
                    }
                }
                finally
                {
                    Database.Changes.Disconnect(connection.Id);
                }
            }

            Database.DatabaseShutdown.ThrowIfCancellationRequested();

            await sendTask;
        }

        [RavenAction("/databases/*/changes", "DELETE", AuthorizationStatus.ValidUser)]
        public Task DeleteConnections()
        {
            var ids = GetStringValuesQueryString("id");

            foreach (var idStr in ids)
            {
                if (long.TryParse(idStr, NumberStyles.Any, CultureInfo.InvariantCulture, out long id) == false)
                    throw new ArgumentException($"Could not parse query string 'id' header as int64, value was: {idStr}");

                Database.Changes.Disconnect(id);
            }

            return NoContent();
        }
    }
}
