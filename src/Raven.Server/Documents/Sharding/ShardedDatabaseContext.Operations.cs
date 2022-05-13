using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Server.Documents.Operations;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Utils;
using Operation = Raven.Client.Documents.Operations.Operation;

namespace Raven.Server.Documents.Sharding;

public partial class ShardedDatabaseContext
{
    public ShardedOperations Operations;

    public class ShardedOperations : AbstractOperations<ShardedOperation>
    {
        private readonly ShardedDatabaseContext _context;

        private readonly ConcurrentDictionary<NodeTagAndShardNumberPair, DatabaseChanges> _changes = new();

        public ShardedOperations([NotNull] ShardedDatabaseContext context)
            : base(context.Changes)
        {
            _context = context ?? throw new ArgumentNullException(nameof(context));
        }

        public override long GetNextOperationId()
        {
            var nextId = _context._serverStore.Operations.GetNextOperationId();

            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Pawel, DevelopmentHelper.Severity.Major, "Encode NodeTag");

            return nextId;
        }

        protected override void RaiseNotifications(OperationStatusChange change, ShardedOperation operation)
        {
        }

        public Task AddOperation(
            long id,
            OperationType operationType,
            string description,
            IOperationDetailedDescription detailedDescription,
            Func<RavenCommand<OperationIdResult>> commandFactory,
            OperationCancelToken token = null)
        {
            var operation = CreateOperationInstance(id, _context.DatabaseName, operationType, description, detailedDescription, token);

            return AddOperationInternalAsync(operation, action => CreateTaskAsync(operation, commandFactory, action, token));
        }

        private async Task<IOperationResult> CreateTaskAsync(
            ShardedOperation operation,
            Func<RavenCommand<OperationIdResult>> commandFactory,
            Action<IOperationProgress> onProgress,
            OperationCancelToken token)
        {
            var t = token?.Token ?? default;

            var commands = new RavenCommand<OperationIdResult>[_context.NumberOfShardNodes];
            var tasks = new Task[commands.Length];
            for (var shardNumber = 0; shardNumber < tasks.Length; shardNumber++)
            {
                var command = commandFactory();

                commands[shardNumber] = command;
                tasks[shardNumber] = _context.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber, t);
            }

            await Task.WhenAll(tasks).WithCancellation(t);

            operation.Operation = new MultiOperation(operation.Id, _context, onProgress);

            for (var shardNumber = 0; shardNumber < commands.Length; shardNumber++)
            {
                var command = commands[shardNumber];

                var key = new NodeTagAndShardNumberPair(command.Result.OperationNodeTag, shardNumber);

                var changes = GetChanges(key);

                var shardOperation = new Operation(_context.ShardExecutor.GetRequestExecutorAt(shardNumber), () => changes, DocumentConventions.DefaultForServer, command.Result.OperationId, command.Result.OperationNodeTag);

                operation.Operation.Watch(key, shardOperation);
            }

            return await operation.Operation.WaitForCompletionAsync(t);
        }

        private DatabaseChanges GetChanges(NodeTagAndShardNumberPair key) => _changes.GetOrAdd(key, k => new DatabaseChanges(_context.ShardExecutor.GetRequestExecutorAt(k.ShardNumber), ShardHelper.ToShardName(_context.DatabaseName, k.ShardNumber), onDispose: null, k.NodeTag));
    }

    public readonly struct NodeTagAndShardNumberPair
    {
        public readonly string NodeTag;

        public readonly int ShardNumber;

        public NodeTagAndShardNumberPair([NotNull] string nodeTag, int shardNumber)
        {
            NodeTag = nodeTag ?? throw new ArgumentNullException(nameof(nodeTag));
            ShardNumber = shardNumber;
        }

        public bool Equals(NodeTagAndShardNumberPair other)
        {
            return string.Equals(NodeTag, other.NodeTag, StringComparison.OrdinalIgnoreCase) && ShardNumber == other.ShardNumber;
        }

        public override bool Equals(object obj)
        {
            return obj is NodeTagAndShardNumberPair other && Equals(other);
        }

        public override int GetHashCode()
        {
            var hashCode = new HashCode();
            hashCode.Add(NodeTag, StringComparer.OrdinalIgnoreCase);
            hashCode.Add(ShardNumber);
            return hashCode.ToHashCode();
        }
    }

    public class MultiOperation
    {
        private readonly long _id;
        private readonly ShardedDatabaseContext _context;

        private readonly Action<IOperationProgress> _onProgress;

        private readonly Dictionary<NodeTagAndShardNumberPair, Operation> _operations;

        private IOperationProgress[] _progresses;

        public MultiOperation(long id, ShardedDatabaseContext context, Action<IOperationProgress> onProgress)
        {
            _id = id;
            _context = context;
            _onProgress = onProgress;
            _operations = new Dictionary<NodeTagAndShardNumberPair, Operation>();
        }

        public void Watch(NodeTagAndShardNumberPair key, Operation operation)
        {
            Debug.Assert(_progresses == null);

            operation.OnProgressChanged += (_, progress) => OnProgressChanged(key, progress);

            _operations.Add(key, operation);

            void OnProgressChanged(NodeTagAndShardNumberPair key, IOperationProgress progress)
            {
                if (progress.CanMerge == false)
                {
                    NotifyAboutProgress(progress);
                    return;
                }

                _progresses[key.ShardNumber] = progress;

                MaybeNotifyAboutProgress();
            }
        }

        private void MaybeNotifyAboutProgress()
        {
            IOperationProgress result = null;

            for (var i = 0; i < _progresses.Length; i++)
            {
                var progress = _progresses[i];
                if (progress == null)
                    continue;

                if (result == null)
                {
                    result = progress.Clone();
                    continue;
                }

                result.MergeWith(progress);
            }

            NotifyAboutProgress(result);
        }

        private void NotifyAboutProgress(IOperationProgress progress)
        {
            _onProgress(progress);
        }

        public async Task<IOperationResult> WaitForCompletionAsync(CancellationToken token)
        {
            _progresses = new IOperationProgress[_operations.Count];

            var tasks = new List<Task<IOperationResult>>(_progresses.Length);

            foreach (var operation in _operations.Values)
                tasks.Add(operation.WaitForCompletionAsync());

            await Task.WhenAll(tasks).WithCancellation(token);

            return CreateOperationResult(tasks);
        }

        private static IOperationResult CreateOperationResult(IEnumerable<Task<IOperationResult>> tasks)
        {
            foreach (var task in tasks)
            {
                // TODO [ppekrol]
                return task.Result;
            }

            return null;
        }

        public async ValueTask KillAsync(bool waitForCompletion, CancellationToken token)
        {
            var tasks = new List<Task>(_operations.Count);
            foreach (var key in _operations.Keys)
                tasks.Add(_context.ShardExecutor.ExecuteSingleShardAsync(new KillOperationCommand(_id, key.NodeTag), key.ShardNumber, token));

            if (waitForCompletion)
                await Task.WhenAll(tasks).WithCancellation(token);
        }
    }

    public class ShardedOperation : AbstractOperation
    {
        public MultiOperation Operation;

        public override async ValueTask KillAsync(bool waitForCompletion, CancellationToken token)
        {
            if (Operation != null)
                await Operation.KillAsync(waitForCompletion, token);

            await base.KillAsync(waitForCompletion, token);
        }
    }
}
