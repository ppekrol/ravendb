﻿using System;
using System.IO;
using Raven.Client.ServerWide.Tcp;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.ServerWide.Tcp.Sync
{
    internal static class TcpNegotiationSyncExtensions
    {
        private static readonly Logger Log = LoggingSource.Instance.GetLogger("TCP Negotiation", typeof(TcpNegotiation).FullName);

        internal static TcpConnectionHeaderMessage.SupportedFeatures NegotiateProtocolVersion(this TcpNegotiation.SyncTcpNegotiation syncTcpNegotiation, JsonOperationContext context, Stream stream, TcpNegotiateParameters parameters)
        {
            if (Log.IsInfoEnabled)
            {
                Log.Info($"Start negotiation for {parameters.Operation} operation with {parameters.DestinationNodeTag ?? parameters.DestinationUrl}");
            }

            using (var writer = new BlittableJsonTextWriter(context, stream))
            {
                var current = parameters.Version;
                while (true)
                {
                    if (parameters.CancellationToken.IsCancellationRequested)
                        throw new OperationCanceledException($"Stopped TCP negotiation for {parameters.Operation} because of cancellation request");

                    SendTcpVersionInfo(context, writer, parameters, current);
                    var version = parameters.ReadResponseAndGetVersionCallback(context, writer, stream, parameters.DestinationUrl);
                    if (Log.IsInfoEnabled)
                    {
                        Log.Info($"Read response from {parameters.SourceNodeTag ?? parameters.DestinationUrl} for '{parameters.Operation}', received version is '{version}'");
                    }

                    if (version == current)
                        break;

                    //In this case we usually throw internally but for completeness we better handle it
                    if (version == TcpNegotiation.DropStatus)
                    {
                        return TcpConnectionHeaderMessage.GetSupportedFeaturesFor(TcpConnectionHeaderMessage.OperationTypes.Drop, TcpConnectionHeaderMessage.DropBaseLine);
                    }
                    var status = TcpConnectionHeaderMessage.OperationVersionSupported(parameters.Operation, version, out current);
                    if (status == TcpConnectionHeaderMessage.SupportedStatus.OutOfRange)
                    {
                        SendTcpVersionInfo(context, writer, parameters, TcpNegotiation.OutOfRangeStatus);
                        throw new ArgumentException($"The {parameters.Operation} version {parameters.Version} is out of range, our lowest version is {current}");
                    }
                    if (Log.IsInfoEnabled)
                    {
                        Log.Info($"The version {version} is {status}, will try to agree on '{current}' for {parameters.Operation} with {parameters.DestinationNodeTag ?? parameters.DestinationUrl}.");
                    }
                }
                if (Log.IsInfoEnabled)
                {
                    Log.Info($"{parameters.DestinationNodeTag ?? parameters.DestinationUrl} agreed on version '{current}' for {parameters.Operation}.");
                }
                return TcpConnectionHeaderMessage.GetSupportedFeaturesFor(parameters.Operation, current);
            }
        }

        private static void SendTcpVersionInfo(JsonOperationContext context, BlittableJsonTextWriter writer, TcpNegotiateParameters parameters, int currentVersion)
        {
            if (Log.IsInfoEnabled)
            {
                Log.Info($"Send negotiation for {parameters.Operation} in version {currentVersion}");
            }

            context.Sync.Write(writer, new DynamicJsonValue
            {
                [nameof(TcpConnectionHeaderMessage.DatabaseName)] = parameters.Database,
                [nameof(TcpConnectionHeaderMessage.Operation)] = parameters.Operation.ToString(),
                [nameof(TcpConnectionHeaderMessage.SourceNodeTag)] = parameters.SourceNodeTag,
                [nameof(TcpConnectionHeaderMessage.OperationVersion)] = currentVersion,
                [nameof(TcpConnectionHeaderMessage.AuthorizeInfo)] = parameters.AuthorizeInfo?.ToJson()
            });
            writer.Flush();
        }
    }
}
