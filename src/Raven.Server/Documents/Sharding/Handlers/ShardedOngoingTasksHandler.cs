// -----------------------------------------------------------------------
//  <copyright file="ShardedOngoingTasksHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.OngoingTasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Replication;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers
{
    internal sealed class ShardedOngoingTasksHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/tasks", "GET")]
        public async Task GetOngoingTasks()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForGetOngoingTasks(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/connection-strings", "PUT")]
        public async Task PutConnectionString()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForPutConnectionString(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/connection-strings", "GET")]
        public async Task GetConnectionStrings()
        {
            using (var processor = new OngoingTasksHandlerProcessorForGetConnectionString<ShardedDatabaseRequestHandler, TransactionOperationContext>(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/connection-strings", "DELETE")]
        public async Task RemoveConnectionString()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForRemoveConnectionString(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/etl", "PUT")]
        public async Task AddEtl()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForAddEtl(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/etl", "RESET")]
        public async Task ResetEtl()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForResetEtl(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/subscription-tasks", "DELETE")]
        public async Task DeleteSubscriptionTask()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForDeleteSubscriptionTasks(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/tasks", "DELETE")]
        public async Task DeleteOngoingTask()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForDeleteOngoingTask(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/subscription-tasks/state", "POST")]
        public async Task ToggleSubscriptionTaskState()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForPostSubscriptionTasksState(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/tasks/state", "POST")]
        public async Task ToggleTaskState()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForToggleTaskState(this, requireAdmin: true))
                await processor.ExecuteAsync();
        }

        // Get Info about a specific task - For Edit View in studio - Each task should return its own specific object
        [RavenShardedAction("/databases/*/task", "GET")]
        public async Task GetOngoingTaskInfo()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForGetOngoingTask(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/periodic-backup", "POST")]
        public async Task UpdatePeriodicBackup()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForUpdatePeriodicBackup(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/periodic-backup/config", "GET")]
        public async Task GetConfiguration()
        {
            using (var processor = new OngoingTasksHandlerProcessorForGetPeriodicBackupConfiguration<ShardedDatabaseRequestHandler, TransactionOperationContext>(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/debug/periodic-backup/timers", "GET")]
        public async Task GetPeriodicBackupTimers()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForGetPeriodicBackupTimers(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/backup-data-directory", "GET")]
        public async Task FullBackupDataDirectory()
        {
            using (var processor = new OngoingTasksHandlerProcessorForGetFullBackupDataDirectory<ShardedDatabaseRequestHandler, TransactionOperationContext>(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/tasks/external-replication", "POST")]
        public async Task UpdateExternalReplication()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForUpdateExternalReplication(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/tasks/pull-replication/hub", "GET")]
        public async Task GetHubTasksInfo()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForGetPullReplicationHubTasksInfo(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/backup/database", "POST")]
        public async Task BackupDatabase()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForBackupDatabaseNow(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/backup", "POST")]
        public async Task BackupDatabaseOnce()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForBackupDatabaseOnce(this))
                await processor.ExecuteAsync();
        }
    }
}
