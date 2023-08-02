﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Server.Documents.Includes.Sharding;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.Utils;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Subscriptions;


/*
 * The subscription worker (client) connects to an orchestrator (SendShardedSubscriptionDocuments). 
 * The orchestrator initializes shard subscription workers (StartShardSubscriptionWorkersAsync).
 * ShardedSubscriptionWorkers are maintaining the connection with subscription on each shard.
 * The orchestrator maintains the connection with the client and checks if there is an available batch on each Sharded Worker (MaintainConnectionWithClientWorkerAsync).
 * Handle batch flow:
 * Orchestrator sends the batch to the client (WriteBatchToClientAndAckAsync).
 * Orchestrator receive batch ACK request from client.
 * Orchestrator advances the sharded worker and waits for the sharded worker.
 * Sharded worker sends an ACK to the shard and waits for CONFIRM from shard (ACK command in cluster)
 * Sharded worker advances the Orchestrator
 * Orchestrator sends the CONFIRM to client
 */

internal sealed class SubscriptionConnectionsStateOrchestrator : AbstractSubscriptionConnectionsState<OrchestratedSubscriptionConnection, OrchestratorIncludesCommandImpl>
{
    internal readonly ShardedDatabaseContext _databaseContext;
    private Dictionary<string, ShardedSubscriptionWorker> _shardWorkers;
    private TaskCompletionSource _initialConnection;
    private SubscriptionWorkerOptions _options;

    public BlockingCollection<ShardedSubscriptionBatch> Batches = new BlockingCollection<ShardedSubscriptionBatch>();

    public int ClosedDueToNoDocs;
    public bool SubscriptionClosedDueNoDocs => ClosedDueToNoDocs == _shardWorkers.Count;

    public SubscriptionConnectionsStateOrchestrator(ServerStore server, ShardedDatabaseContext databaseContext, long subscriptionId) : 
        base(server, databaseContext.DatabaseName, subscriptionId, databaseContext.DatabaseShutdown)
    {
        _databaseContext = databaseContext;
    }

    public override async Task<(IDisposable DisposeOnDisconnect, long RegisterConnectionDurationInTicks)> SubscribeAsync(OrchestratedSubscriptionConnection connection)
    {
        var result = await base.SubscribeAsync(connection);
        var initializationTask = Interlocked.CompareExchange(ref _initialConnection, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously), null);
        if (initializationTask == null)
        {
            _options = connection.Options;
            _shardWorkers = new Dictionary<string, ShardedSubscriptionWorker>();
            StartShardSubscriptionWorkers();

            _initialConnection.SetResult();
            return result;
        }

        await initializationTask.Task;
        return result;
    }

    private void StartShardSubscriptionWorkers()
    {
        ClosedDueToNoDocs = 0;
        foreach (var shardNumber in _databaseContext.ShardsTopology.Keys)
        {
            var re = _databaseContext.ShardExecutor.GetRequestExecutorAt(shardNumber);
            var shard = ShardHelper.ToShardName(_databaseContext.DatabaseName, shardNumber);
            var worker = CreateShardedWorkerHolder(shard, re, lastErrorDateTime: null);
            _shardWorkers.Add(shard, worker);
        }
    }

    private ShardedSubscriptionWorker CreateShardedWorkerHolder(string shard, RequestExecutor re, DateTime? lastErrorDateTime)
    {
        var options = _options.Clone();

        // we don't want to ensure that only one orchestrated connection handle the subscription
        options.Strategy = SubscriptionOpeningStrategy.TakeOver;
        options.WorkerId += $"/{ShardHelper.GetShardNumberFromDatabaseName(shard)}";
        options.TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(250); // failover faster
        
        // we want to limit the batch of each shard, to not hold too much memory if there are other batches while batch is proceed
        options.MaxDocsPerBatch = Math.Max(Math.Min(_options.MaxDocsPerBatch / _databaseContext.ShardCount, _options.MaxDocsPerBatch), 1);

        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "RavenDB-19085 need to ensure the sharded workers has the same sub definition. by sending my raft index?");
        var shardWorker = new ShardedSubscriptionWorker(options, shard, re, this);
        shardWorker.Run(shardWorker.TryPublishBatchAsync, CancellationTokenSource.Token);
        
        return shardWorker;
    }

    public override async Task UpdateClientConnectionTime()
    {
        var command = GetUpdateSubscriptionClientConnectionTime();
        var (etag, _) = await _server.SendToLeaderAsync(command);
        await _server.Cluster.WaitForIndexNotification(etag);
        // await WaitForIndexNotificationAsync(etag);
    }

    protected override void SetLastChangeVectorSent(OrchestratedSubscriptionConnection connection)
    {
        LastChangeVectorSent = connection.SubscriptionState.ShardingState.ChangeVectorForNextBatchStartingPointForOrchestrator;
    }

    public override void Initialize(OrchestratedSubscriptionConnection connection, bool afterSubscribe = false)
    {
        base.Initialize(connection, afterSubscribe);
        SetLastChangeVectorSent(connection);
    }

    protected override UpdateSubscriptionClientConnectionTime GetUpdateSubscriptionClientConnectionTime()
    {
        var cmd = base.GetUpdateSubscriptionClientConnectionTime();
        cmd.DatabaseName = _databaseContext.DatabaseName;
        return cmd;
    }

    public override Task WaitForIndexNotificationAsync(long index) => _databaseContext.Cluster.WaitForExecutionOnShardsAsync(index).AsTask();

    public override void DropSubscription(SubscriptionException e)
    {
        _databaseContext.SubscriptionsStorage.DropSubscriptionConnections(SubscriptionId, e);
    }

    public override async Task HandleConnectionExceptionAsync(OrchestratedSubscriptionConnection connection, Exception e)
    {
        await base.HandleConnectionExceptionAsync(connection, e);
        if (e is SubscriptionException se and not SubscriptionChangeVectorUpdateConcurrencyException and not SubscriptionInUseException)
        {
            DropSubscription(se);
        }
        else
        {
            DropSubscription(new SubscriptionClosedException("Got unexpected exception, dropping workers connections.", canReconnect: true, e));
        }
    }

    public override void Dispose()
    {
        DisposeWorkers();
        base.Dispose();
    }

    public void DisposeWorkers()
    {
        var workers = _shardWorkers;
        var connection = _initialConnection;

        while (Batches.TryTake(out var batch))
        {
            using (batch.ReturnContext)
            {
                batch.SetCancel();
            }
        }

        if (workers == null || workers.Count == 0)
            return;

        Parallel.ForEach(workers, (w) =>
        {
            try
            {
                w.Value.Dispose();
            }
            catch
            {
                // ignore
            }
        });


        if (Interlocked.CompareExchange(ref _initialConnection, null, connection) == connection)
        {
            _shardWorkers = null;
        }
    }
}
