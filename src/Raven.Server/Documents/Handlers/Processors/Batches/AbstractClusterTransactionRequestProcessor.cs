using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.Documents.Handlers.Batches.Commands;
using Raven.Server.Rachis;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Batches;

internal abstract class AbstractClusterTransactionRequestProcessor<TRequestHandler, TBatchCommand>
    where TRequestHandler : RequestHandler
    where TBatchCommand : IBatchCommand
{
    protected readonly TRequestHandler RequestHandler;

    protected AbstractClusterTransactionRequestProcessor([NotNull] TRequestHandler requestHandler)
    {
        RequestHandler = requestHandler ?? throw new ArgumentNullException(nameof(requestHandler));
    }

    protected abstract ArraySegment<BatchRequestParser.CommandData> GetParsedCommands(TBatchCommand command);

    public async ValueTask<(long Index, DynamicJsonArray Results)> ProcessAsync(JsonOperationContext context, TBatchCommand command)
    {
        ArraySegment<BatchRequestParser.CommandData> parsedCommands = GetParsedCommands(command);

        var waitForIndexesTimeout = RequestHandler.GetTimeSpanQueryString("waitForIndexesTimeout", required: false);
        var waitForIndexThrow = RequestHandler.GetBoolValueQueryString("waitForIndexThrow", required: false) ?? true;
        var specifiedIndexesQueryString = RequestHandler.HttpContext.Request.Query["waitForSpecificIndex"];
        
        var disableAtomicDocumentWrites = RequestHandler.GetBoolValueQueryString("disableAtomicDocumentWrites", required: false) ?? false;
        CheckBackwardCompatibility(ref disableAtomicDocumentWrites);

        ClusterTransactionCommand.ValidateCommands(parsedCommands, disableAtomicDocumentWrites);

        using (RequestHandler.ServerStore.Cluster.ClusterTransactionWaiter.CreateTask(out var taskId))
        {
            var options = new ClusterTransactionCommand.ClusterTransactionOptions(taskId, disableAtomicDocumentWrites, RequestHandler.ServerStore.Engine.CommandsVersionManager.CurrentClusterMinimalVersion)
            {
                WaitForIndexesTimeout = waitForIndexesTimeout,
                WaitForIndexThrow = waitForIndexThrow,
                SpecifiedIndexesQueryString = specifiedIndexesQueryString.Count > 0 ? specifiedIndexesQueryString.ToArray() : null
            };

            var raftRequestId = RequestHandler.GetRaftRequestIdFromQuery();
            ClusterTransactionCommand clusterTransactionCommand = CreateClusterTransactionCommand(parsedCommands, options, raftRequestId);

            var clusterTransactionCommandResult = await RequestHandler.ServerStore.SendToLeaderAsync(clusterTransactionCommand);
            if (clusterTransactionCommandResult.Result is List<ClusterTransactionCommand.ClusterTransactionErrorInfo> errors)
            {
                RequestHandler.HttpContext.Response.StatusCode = (int)HttpStatusCode.Conflict;
                throw new ClusterTransactionConcurrencyException($"Failed to execute cluster transaction due to the following issues: {string.Join(Environment.NewLine, errors.Select(e => e.Message))}")
                {
                    ConcurrencyViolations = errors.Select(e => e.Violation).ToArray()
                };
            }
            await RequestHandler.ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, clusterTransactionCommandResult.Index);

            DynamicJsonArray result;
            if (clusterTransactionCommand.DatabaseCommands.Count > 0)
            {
                using var timeout = new CancellationTokenSource(RequestHandler.ServerStore.Engine.OperationTimeout);
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token, RequestHandler.HttpContext.RequestAborted);
                var databaseResult = await RequestHandler.ServerStore.Cluster.ClusterTransactionWaiter.WaitForResults(taskId, cts.Token);

                if (databaseResult.IndexTask != null)
                {
                    await databaseResult.IndexTask;
                }

                result = databaseResult.Array;
            }
            else
            {
                result = new DynamicJsonArray();
            }

            if (clusterTransactionCommand.ClusterCommands.Count > 0)
            {
                foreach (var clusterCommand in clusterTransactionCommand.ClusterCommands)
                {
                    result.Add(new DynamicJsonValue
                    {
                        [nameof(ICommandData.Type)] = clusterCommand.Type,
                        [nameof(ICompareExchangeValue.Key)] = clusterCommand.Id,
                        [nameof(ICompareExchangeValue.Index)] = clusterTransactionCommandResult.Index
                    });
                }
            }

            return (clusterTransactionCommandResult.Index, result);
        }
    }

    protected abstract ClusterTransactionCommand CreateClusterTransactionCommand(ArraySegment<BatchRequestParser.CommandData> parsedCommands,
        ClusterTransactionCommand.ClusterTransactionOptions options, string raftRequestId);

    private void CheckBackwardCompatibility(ref bool disableAtomicDocumentWrites)
    {
        if (disableAtomicDocumentWrites)
            return;

        if (RequestRouter.TryGetClientVersion(RequestHandler.HttpContext, out var clientVersion) == false)
        {
            disableAtomicDocumentWrites = true;
            return;
        }

        if (clientVersion.Major < 5 || (clientVersion.Major == 5 && clientVersion.Minor < 2))
        {
            disableAtomicDocumentWrites = true;
        }
    }
}
