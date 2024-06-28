﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Server.Documents.Includes.Sharding;
using Raven.Server.Documents.Queries.Sharding;
using Raven.Server.Documents.Sharding.Operations.Queries;
using Raven.Server.Documents.Sharding.Queries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Sharding.Subscriptions;

public class ShardedSubscriptionBatch : SubscriptionBatchBase<BlittableJsonReaderObject>
{
    public TaskCompletionSource SendBatchToClientTcs;
    public TaskCompletionSource ConfirmFromShardSubscriptionConnectionTcs;
    public string ShardName;
    private readonly ShardedDatabaseContext _databaseContext;
    public IDisposable ReturnContext;
    public JsonOperationContext Context;

    public void SetCancel()
    {
        SendBatchToClientTcs?.TrySetCanceled();
        ConfirmFromShardSubscriptionConnectionTcs?.TrySetCanceled();
    }

    public void SetException(Exception e)
    {
        SendBatchToClientTcs?.TrySetException(e);
        ConfirmFromShardSubscriptionConnectionTcs?.TrySetException(e);
    }

    public ShardedSubscriptionBatch(RequestExecutor requestExecutor, string dbName, NLog.Logger logger, ShardedDatabaseContext databaseContext) : base(requestExecutor, dbName, logger)
    {
        ShardName = dbName;
        _databaseContext = databaseContext;
    }

    protected override void EnsureDocumentId(BlittableJsonReaderObject item, string id) => throw new SubscriberErrorException($"Missing id property for {item}");

    internal override async ValueTask InitializeAsync(BatchFromServer batch)
    {
        SendBatchToClientTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        ConfirmFromShardSubscriptionConnectionTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        ReturnContext = batch.ReturnContext;
        Context = batch.Context;
        batch.ReturnContext = null; // move the release responsibility to the OrchestratedSubscriptionProcessor
        batch.Context = null;


        await InitializeDocumentIncludesAsync(batch);
        await base.InitializeAsync(batch);

        LastSentChangeVectorInBatch = null;
    }

    internal async ValueTask InitializeDocumentIncludesAsync(BatchFromServer batchFromServer)
    {
        await TryGatherMissingDocumentIncludesAsync(batchFromServer.Includes);
        batchFromServer.Includes = _result?.Includes;
    }

    private ShardedQueryResult _result;

    private ValueTask TryGatherMissingDocumentIncludesAsync(List<BlittableJsonReaderObject> list)
    {
        if (list == null || list.Count == 0)
            return ValueTask.CompletedTask;

        _result = new ShardedQueryResult
        {
            Results = new List<BlittableJsonReaderObject>(),
        };

        HashSet<string> missingDocumentIncludes = null;

        foreach (var includes in list)
        {
            _databaseContext.DatabaseShutdown.ThrowIfCancellationRequested();
            ShardedQueryOperation.HandleDocumentIncludesInternal(includes, Context, _result, ref missingDocumentIncludes);
        }

        if (missingDocumentIncludes == null)
            return ValueTask.CompletedTask;

        return ShardedQueryProcessor.HandleMissingDocumentIncludesAsync(Context, request: null, _databaseContext, missingDocumentIncludes, _result, metadataOnly: false, token: _databaseContext.DatabaseShutdown);
    }

    public void CloneIncludes(ClusterOperationContext context, OrchestratorIncludesCommandImpl includes)
    {
        if (_includes != null)
            includes.IncludeDocumentsCommand.Gather(_includes);
        if (_counterIncludes != null)
            includes.IncludeCountersCommand.Gather(_counterIncludes, context);
        if (_timeSeriesIncludes != null)
            includes.IncludeTimeSeriesCommand.Gather(_timeSeriesIncludes, context);
    }
}
