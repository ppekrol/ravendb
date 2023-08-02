using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Server;

namespace Raven.Server.Documents.PeriodicBackup.Restore.Sharding
{
    internal sealed class ShardedRestoreOrchestrationTask : AbstractRestoreBackupTask
    {
        private readonly long _operationId;
        private IOperationResult _result;

        public ShardedRestoreOrchestrationTask(ServerStore serverStore,
            RestoreBackupConfigurationBase configuration,
            long operationId,
            OperationCancelToken operationCancelToken) : base(serverStore, configuration, restoreSource: null, filesToRestore: null, operationCancelToken)
        {
            _operationId = operationId;
        }

        protected override async Task InitializeAsync()
        {
            await base.InitializeAsync();
            CreateRestoreSettings();

            // shard nodes will handle saving the secret key
            HasEncryptionKey = false;
        }

        protected override async Task<IDisposable> OnBeforeRestoreAsync()
        {
            ModifyDatabaseRecordSettings();
            InitializeShardingConfiguration();

            var databaseRecord = RestoreSettings.DatabaseRecord;

            // we are currently restoring, shouldn't try to access it
            databaseRecord.DatabaseState = DatabaseStateStatus.RestoreInProgress;
            var index = await SaveDatabaseRecordAsync(DatabaseName, databaseRecord, databaseValues: null, Result, Progress);

            var dbSearchResult = ServerStore.DatabasesLandlord.TryGetOrCreateDatabase(DatabaseName);
            var shardedDbContext = dbSearchResult.DatabaseContext;

            var op = new WaitForIndexNotificationOnServerOperation(index);
            await shardedDbContext.AllOrchestratorNodesExecutor.ExecuteParallelForAllAsync(op);

            return null;
        }

        protected override async Task RestoreAsync()
        {
            var dbSearchResult = ServerStore.DatabasesLandlord.TryGetOrCreateDatabase(DatabaseName);
            var shardedDbContext = dbSearchResult.DatabaseContext;
            var conventions = shardedDbContext.ShardExecutor.Conventions;

            var multiOperationTask = shardedDbContext.Operations.CreateServerWideMultiOperationTask<OperationIdResult, ShardedRestoreResult, ShardedRestoreProgress>(
                id: _operationId,
                commandFactory: (context, i) => GenerateCommandForShard(conventions, shardNumber: i, configuration: RestoreConfiguration.Clone()),
                onProgress: Progress,
                token: OperationCancelToken);

            _result = await multiOperationTask;
        }

        protected override async Task OnAfterRestoreAsync()
        {
            DisableOngoingTasksIfNeeded(RestoreSettings.DatabaseRecord);

            Progress.Invoke(Result.Progress);

            // after the db for restore is done, we can safely set the db state to normal and write the DatabaseRecord
            DatabaseRecord databaseRecord;
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverContext))
            using (serverContext.OpenReadTransaction())
            {
                databaseRecord = ServerStore.Cluster.ReadDatabase(serverContext, DatabaseName);
                databaseRecord.DatabaseState = DatabaseStateStatus.Normal;
            }

            await SaveDatabaseRecordAsync(DatabaseName, databaseRecord, databaseValues: null, Result, Progress);
        }

        protected override IOperationResult OperationResult() => _result;

        private void InitializeShardingConfiguration()
        {
            var databaseRecord = RestoreSettings.DatabaseRecord;

            databaseRecord.Sharding = new ShardingConfiguration
            {
                Shards = new Dictionary<int, DatabaseTopology>(RestoreConfiguration.ShardRestoreSettings.Shards.Count),
                Orchestrator = new OrchestratorConfiguration
                {
                    Topology = new OrchestratorTopology
                    {
                        Members = new List<string>()
                    }
                }
            };

            var clusterTransactionIdBase64 = Guid.NewGuid().ToBase64Unpadded();
            var nodes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var (shardNumber, shardRestoreSetting) in RestoreConfiguration.ShardRestoreSettings.Shards)
            {
                Debug.Assert(shardRestoreSetting.ShardNumber == shardNumber);
                
                databaseRecord.Sharding.Shards[shardNumber] = new DatabaseTopology
                {
                    Members = new List<string>
                    {
                        shardRestoreSetting.NodeTag
                    },
                    ClusterTransactionIdBase64 = clusterTransactionIdBase64,
                    ReplicationFactor = 1
                };

                if (nodes.Add(shardRestoreSetting.NodeTag))
                {
                    databaseRecord.Sharding.Orchestrator.Topology.Members.Add(shardRestoreSetting.NodeTag);
                    databaseRecord.Sharding.Orchestrator.Topology.ClusterTransactionIdBase64 = clusterTransactionIdBase64;
                }
            }

            databaseRecord.Sharding.Orchestrator.Topology.ReplicationFactor = databaseRecord.Sharding.Orchestrator.Topology.Members.Count;
        }


        private static RavenCommand<OperationIdResult> GenerateCommandForShard(DocumentConventions conventions, int shardNumber, RestoreBackupConfigurationBase configuration)
        {
            Debug.Assert(configuration.ShardRestoreSettings?.Shards.ContainsKey(shardNumber) ?? false);

            var shardRestoreSetting = configuration.ShardRestoreSettings.Shards.Single(s => s.Value.ShardNumber == shardNumber).Value;
            configuration.DatabaseName = ShardHelper.ToShardName(configuration.DatabaseName, shardNumber);
            configuration.LastFileNameToRestore = shardRestoreSetting.LastFileNameToRestore;
            configuration.ShardRestoreSettings = null;

            switch (configuration)
            {
                case RestoreBackupConfiguration restoreBackupConfiguration:
                    restoreBackupConfiguration.BackupLocation = shardRestoreSetting.FolderName;
                    break;
                case RestoreFromS3Configuration restoreFromS3Configuration:
                    restoreFromS3Configuration.Settings.RemoteFolderName = shardRestoreSetting.FolderName;
                    break;
                case RestoreFromAzureConfiguration restoreFromAzureConfiguration:
                    restoreFromAzureConfiguration.Settings.RemoteFolderName = shardRestoreSetting.FolderName;
                    break;
                case RestoreFromGoogleCloudConfiguration restoreFromGoogleCloudConfiguration:
                    restoreFromGoogleCloudConfiguration.Settings.RemoteFolderName = shardRestoreSetting.FolderName;
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(configuration));
            }

            return new RestoreBackupOperation.RestoreBackupCommand(conventions, configuration);
        }
    }
}
