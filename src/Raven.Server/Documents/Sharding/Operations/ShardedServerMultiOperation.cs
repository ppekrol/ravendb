﻿using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Utils;
using Sparrow.Json;
using Operation = Raven.Client.Documents.Operations.Operation;

namespace Raven.Server.Documents.Sharding.Operations;

internal sealed class ShardedServerMultiOperation : AbstractShardedMultiOperation
{
    public ShardedServerMultiOperation(long id, ShardedDatabaseContext context, Action<IOperationProgress> onProgress) : base(id, context, onProgress)
    {
    }

    public override async ValueTask<TResult> ExecuteCommandForShard<TResult>(RavenCommand<TResult> command, int shardNumber, CancellationToken token)
    {
        if (ShardedDatabaseContext.ShardsTopology.TryGetValue(shardNumber, out var topology) == false || topology.Members.Count == 0)
            throw new InvalidOperationException($"Cannot execute command '{command.GetType()}' for Database '{ShardHelper.ToShardName(ShardedDatabaseContext.DatabaseName, shardNumber)}', shard {shardNumber} doesn't exist or has no members.");
        
        var nodeTag = ShardedDatabaseContext.ShardsTopology[shardNumber].Members[0];
        
        await ShardedDatabaseContext.AllOrchestratorNodesExecutor.ExecuteForNodeAsync(command, nodeTag, token);

        return command.Result;
    }

    public override Operation CreateOperationInstance(ShardedDatabaseIdentifier key, long operationId)
    {
        var executor = ShardedDatabaseContext.AllOrchestratorNodesExecutor.GetRequestExecutorForNode(key.NodeTag);
        return new ServerWideOperation(executor, DocumentConventions.DefaultForServer, operationId, key.NodeTag);
    }

    public override async ValueTask KillAsync(CancellationToken token)
    {
        var tasks = new List<Task>(Operations.Count);
        using (ShardedDatabaseContext.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
        {
            foreach (var (key, operation) in Operations)
            {
                var command = new KillServerOperationCommand(operation.Id);
                tasks.Add(ShardedDatabaseContext.AllOrchestratorNodesExecutor.ExecuteForNodeAsync(command, key.NodeTag, token));
            }

            await Task.WhenAll(tasks);
        }
    }
}
