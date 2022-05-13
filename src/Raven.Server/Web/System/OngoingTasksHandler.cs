﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors.OngoingTasks;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.System.Processors.OngoingTasks;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Server.Web.System
{
    public class OngoingTasksHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/tasks", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task GetOngoingTasks()
        {
            using (var processor = new OngoingTasksHandlerProcessorForGetOngoingTasks(this))
                await processor.ExecuteAsync();
        }

        internal OngoingTaskPullReplicationAsHub GetPullReplicationAsHubTaskInfo(ClusterTopology clusterTopology, ExternalReplication ex)
        {
            var connectionResult = Database.ReplicationLoader.GetPullReplicationDestination(ex.TaskId, ex.Database);
            var tag = Server.ServerStore.NodeTag; // we can't know about pull replication tasks on other nodes.

            return new OngoingTaskPullReplicationAsHub
            {
                TaskId = ex.TaskId,
                TaskName = ex.Name,
                ResponsibleNode = new NodeId { NodeTag = tag, NodeUrl = clusterTopology.GetUrlFromTag(tag) },
                TaskState = ex.Disabled ? OngoingTaskState.Disabled : OngoingTaskState.Enabled,
                DestinationDatabase = ex.Database,
                DestinationUrl = connectionResult.Url,
                MentorNode = ex.MentorNode,
                TaskConnectionStatus = connectionResult.Status,
                DelayReplicationFor = ex.DelayReplicationFor
            };
        }

        [RavenAction("/databases/*/admin/periodic-backup/config", "GET", AuthorizationStatus.DatabaseAdmin)]
        public async Task GetConfiguration()
        {
            using (var processor = new OngoingTasksHandlerProcessorForGetPeriodicBackupConfiguration<DatabaseRequestHandler, DocumentsOperationContext>(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/debug/periodic-backup/timers", "GET", AuthorizationStatus.DatabaseAdmin)]
        public async Task GetPeriodicBackupTimer()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                BackupDatabaseHandler.WriteStartOfTimers(writer);
                BackupDatabaseHandler.WritePeriodicBackups(Database, writer, context, out int count);
                BackupDatabaseHandler.WriteEndOfTimers(writer, count);
            }
        }

        [RavenAction("/databases/*/admin/periodic-backup", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task UpdatePeriodicBackup()
        {
            using (var processor = new OngoingTasksHandlerProcessorForUpdatePeriodicBackup(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/backup-data-directory", "GET", AuthorizationStatus.DatabaseAdmin)]
        public async Task FullBackupDataDirectory()
        {
            using (var processor = new OngoingTasksHandlerProcessorForGetFullBackupDataDirectory<DatabaseRequestHandler, DocumentsOperationContext>(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/backup/database", "POST", AuthorizationStatus.DatabaseAdmin, CorsMode = CorsMode.Cluster)]
        public async Task BackupDatabase()
        {
            var taskId = GetLongQueryString("taskId");
            var isFullBackup = GetBoolValueQueryString("isFullBackup", required: false);

            // task id == raft index
            // we must wait here to ensure that the task was actually created on this node
            await ServerStore.Cluster.WaitForIndexNotification(taskId);

            var nodeTag = Database.PeriodicBackupRunner.WhoseTaskIsIt(taskId);
            if (nodeTag == null)
                throw new InvalidOperationException($"Couldn't find a node which is responsible for backup task id: {taskId}");

            if (nodeTag == ServerStore.NodeTag)
            {
                var operationId = Database.PeriodicBackupRunner.StartBackupTask(taskId, isFullBackup ?? true);
                using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName(nameof(StartBackupOperationResult.ResponsibleNode));
                    writer.WriteString(ServerStore.NodeTag);
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(StartBackupOperationResult.OperationId));
                    writer.WriteInteger(operationId);
                    writer.WriteEndObject();
                }

                return;
            }

            RedirectToRelevantNode(nodeTag);
        }

        private void RedirectToRelevantNode(string nodeTag)
        {
            ClusterTopology topology;
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                topology = ServerStore.GetClusterTopology(context);
            }
            var url = topology.GetUrlFromTag(nodeTag);
            if (url == null)
            {
                throw new InvalidOperationException($"Couldn't find the node url for node tag: {nodeTag}");
            }

            var location = url + HttpContext.Request.Path + HttpContext.Request.QueryString;
            HttpContext.Response.StatusCode = (int)HttpStatusCode.TemporaryRedirect;
            HttpContext.Response.Headers.Remove("Content-Type");
            HttpContext.Response.Headers.Add("Location", location);
        }

        private static int _oneTimeBackupCounter;

        [RavenAction("/databases/*/admin/backup", "POST", AuthorizationStatus.DatabaseAdmin, CorsMode = CorsMode.Cluster)]
        public async Task BackupDatabaseOnce()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), "database-backup");
                var backupConfiguration = JsonDeserializationServer.BackupConfiguration(json);
                var backupName = $"One Time Backup #{Interlocked.Increment(ref _oneTimeBackupCounter)}";

                PeriodicBackupRunner.CheckServerHealthBeforeBackup(ServerStore, backupName);
                ServerStore.LicenseManager.AssertCanAddPeriodicBackup(backupConfiguration);
                BackupConfigurationHelper.AssertBackupConfigurationInternal(backupConfiguration);
                BackupConfigurationHelper.AssertDestinationAndRegionAreAllowed(backupConfiguration, ServerStore);

                var sw = Stopwatch.StartNew();
                ServerStore.ConcurrentBackupsCounter.StartBackup(backupName, Logger);
                try
                {
                    var operationId = ServerStore.Operations.GetNextOperationId();
                    var cancelToken = CreateOperationToken();
                    var backupParameters = new BackupParameters
                    {
                        RetentionPolicy = null,
                        StartTimeUtc = SystemTime.UtcNow,
                        IsOneTimeBackup = true,
                        BackupStatus = new PeriodicBackupStatus { TaskId = -1 },
                        OperationId = operationId,
                        BackupToLocalFolder = BackupConfiguration.CanBackupUsing(backupConfiguration.LocalSettings),
                        IsFullBackup = true,
                        TempBackupPath = (Database.Configuration.Storage.TempPath ?? Database.Configuration.Core.DataDirectory).Combine("OneTimeBackupTemp"),
                        Name = backupName
                    };

                    var backupTask = new BackupTask(Database, backupParameters, backupConfiguration, Logger);
                    var threadName = $"Backup thread {backupName} for database '{Database.Name}'";

                    var t = Database.Operations.AddOperation(
                        null,
                        $"Manual backup for database: {Database.Name}",
                        OperationType.DatabaseBackup,
                        onProgress =>
                        {
                            var tcs = new TaskCompletionSource<IOperationResult>(TaskCreationOptions.RunContinuationsAsynchronously);
                            PoolOfThreads.GlobalRavenThreadPool.LongRunning(x =>
                            {
                                try
                                {
                                    ThreadHelper.TrySetThreadPriority(ThreadPriority.BelowNormal, threadName, Logger);
                                    NativeMemory.EnsureRegistered();

                                    using (Database.PreventFromUnloadingByIdleOperations())
                                    {
                                        var runningBackupStatus = new PeriodicBackupStatus { TaskId = 0, BackupType = backupConfiguration.BackupType };
                                        var backupResult = backupTask.RunPeriodicBackup(onProgress, ref runningBackupStatus);
                                        BackupTask.SaveBackupStatus(runningBackupStatus, Database, Logger, backupResult);
                                        tcs.SetResult(backupResult);
                                    }
                                }
                                catch (OperationCanceledException)
                                {
                                    tcs.SetCanceled();
                                }
                                catch (Exception e)
                                {
                                    if (Logger.IsOperationsEnabled)
                                        Logger.Operations($"Failed to run the backup thread: '{backupName}'", e);

                                    tcs.SetException(e);
                                }
                                finally
                                {
                                    ServerStore.ConcurrentBackupsCounter.FinishBackup(backupName, backupStatus: null, sw.Elapsed, Logger);
                                }
                            }, null, threadName);
                            return tcs.Task;
                        },
                        id: operationId, token: cancelToken);

                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        writer.WriteOperationIdAndNodeTag(context, operationId, ServerStore.NodeTag);
                    }
                }
                catch (Exception e)
                {
                    ServerStore.ConcurrentBackupsCounter.FinishBackup(backupName, backupStatus: null, sw.Elapsed, Logger);

                    var message = $"Failed to run backup: '{backupName}'";

                    if (Logger.IsOperationsEnabled)
                        Logger.Operations(message, e);

                    Database.NotificationCenter.Add(AlertRaised.Create(
                        Database.Name,
                        message,
                        null,
                        AlertType.PeriodicBackup,
                        NotificationSeverity.Error,
                        details: new ExceptionDetails(e)));

                    throw;
                }
            }
        }

        [RavenAction("/databases/*/admin/connection-strings", "DELETE", AuthorizationStatus.DatabaseAdmin)]
        public async Task RemoveConnectionString()
        {
            using (var processor = new OngoingTasksHandlerProcessorForRemoveConnectionString(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/connection-strings", "GET", AuthorizationStatus.DatabaseAdmin)]
        public async Task GetConnectionStrings()
        {
            using (var processor = new OngoingTasksHandlerProcessorForGetConnectionString<DatabaseRequestHandler, DocumentsOperationContext>(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/connection-strings", "PUT", AuthorizationStatus.DatabaseAdmin)]
        public async Task PutConnectionString()
        {
            using (var processor = new OngoingTasksHandlerProcessorForPutConnectionString(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/etl", "RESET", AuthorizationStatus.DatabaseAdmin)]
        public async Task ResetEtl()
        {
            using (var processor = new OngoingTasksHandlerProcessorForResetEtl(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/etl", "PUT", AuthorizationStatus.DatabaseAdmin)]
        public async Task AddEtl()
        {
            using (var processor = new OngoingTasksHandlerProcessorForAddEtl(this))
                await processor.ExecuteAsync();
        }

        // Get Info about a specific task - For Edit View in studio - Each task should return its own specific object
        [RavenAction("/databases/*/task", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetOngoingTaskInfo()
        {
            using (var processor = new OngoingTasksHandlerProcessorForGetOngoingTaskInfo(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/tasks/pull-replication/hub", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetHubTasksInfo()
        {
            using (var processor = new OngoingTasksHandlerProcessorForGetPullReplicationHubTasksInfo(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/subscription-tasks/state", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task ToggleSubscriptionTaskState()
        {
            // Note: Only Subscription task needs User authentication, All other tasks need Admin authentication
            var typeStr = GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");
            if (Enum.TryParse<OngoingTaskType>(typeStr, true, out var type) == false)
                throw new ArgumentException($"Unknown task type: {type}", nameof(type));

            if (type != OngoingTaskType.Subscription)
                throw new ArgumentException("Only Subscription type can call this method");

            await ToggleTaskState();
        }

        [RavenAction("/databases/*/admin/tasks/state", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task ToggleTaskState()
        {
            using (var processor = new OngoingTasksHandlerProcessorForToggleTaskState(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/tasks/external-replication", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task UpdateExternalReplication()
        {
            using (var processor = new OngoingTasksHandlerProcessorForUpdateExternalReplication(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/subscription-tasks", "DELETE", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task DeleteSubscriptionTask()
        {
            // Note: Only Subscription task needs User authentication, All other tasks need Admin authentication
            var typeStr = GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");
            if (Enum.TryParse<OngoingTaskType>(typeStr, true, out var type) == false)
                throw new ArgumentException($"Unknown task type: {type}", nameof(type));

            if (type != OngoingTaskType.Subscription)
                throw new ArgumentException("Only Subscription type can call this method");

            await DeleteOngoingTask();
        }

        [RavenAction("/databases/*/admin/tasks", "DELETE", AuthorizationStatus.DatabaseAdmin)]
        public async Task DeleteOngoingTask()
        {
            using (var processor = new OngoingTasksHandlerProcessorForDeleteOngoingTask(this))
                await processor.ExecuteAsync();
        }

        internal static OngoingTaskState GetEtlTaskState<T>(EtlConfiguration<T> config) where T : ConnectionString
        {
            var taskState = OngoingTaskState.Enabled;

            if (config.Disabled || config.Transforms.All(x => x.Disabled))
                taskState = OngoingTaskState.Disabled;
            else if (config.Transforms.Any(x => x.Disabled))
                taskState = OngoingTaskState.PartiallyEnabled;

            return taskState;
        }
    }

    public class OngoingTasksResult : IDynamicJson
    {
        public List<OngoingTask> OngoingTasksList { get; set; }
        public int SubscriptionsCount { get; set; }

        public List<PullReplicationDefinition> PullReplications { get; set; }

        public OngoingTasksResult()
        {
            OngoingTasksList = new List<OngoingTask>();
            PullReplications = new List<PullReplicationDefinition>();
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(OngoingTasksList)] = new DynamicJsonArray(OngoingTasksList.Select(x => x.ToJson())),
                [nameof(SubscriptionsCount)] = SubscriptionsCount,
                [nameof(PullReplications)] = new DynamicJsonArray(PullReplications.Select(x => x.ToJson()))
            };
        }
    }
}
