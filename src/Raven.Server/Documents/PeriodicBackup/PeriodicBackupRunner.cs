using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NCrontab.Advanced;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Config.Settings;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Collections;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Utils;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class PeriodicBackupRunner : ITombstoneAware, IDisposable
    {
        private readonly Logger _logger;

        private readonly DocumentDatabase _database;
        private readonly ServerStore _serverStore;
        private readonly CancellationTokenSource _cancellationToken;
        private readonly PathSetting _tempBackupPath;

        private readonly ConcurrentDictionary<long, PeriodicBackup> _periodicBackups
            = new ConcurrentDictionary<long, PeriodicBackup>();

        private static readonly Dictionary<string, long> EmptyDictionary = new Dictionary<string, long>();
        private readonly ConcurrentSet<Task> _inactiveRunningPeriodicBackupsTasks = new ConcurrentSet<Task>();

        private bool _disposed;
        private readonly DateTime? _databaseWakeUpTimeUtc;

        // interval can be 2^32-2 milliseconds at most
        // this is the maximum interval acceptable in .Net's threading timer
        public readonly TimeSpan MaxTimerTimeout = TimeSpan.FromMilliseconds(Math.Pow(2, 32) - 2);

        public ICollection<PeriodicBackup> PeriodicBackups => _periodicBackups.Values;

        public PeriodicBackupRunner(DocumentDatabase database, ServerStore serverStore, DateTime? wakeup = null)
        {
            _database = database;
            _serverStore = serverStore;
            _logger = LoggingSource.Instance.GetLogger<PeriodicBackupRunner>(_database.Name);
            _cancellationToken = CancellationTokenSource.CreateLinkedTokenSource(_database.DatabaseShutdown);
            _tempBackupPath = (_database.Configuration.Storage.TempPath ?? _database.Configuration.Core.DataDirectory).Combine("PeriodicBackupTemp");

            // we pass wakeup-1 to ensure the backup will run right after DB woke up on wakeup time, and not on the next occurrence.
            // relevant only if it's the first backup after waking up
            _databaseWakeUpTimeUtc = wakeup?.AddMinutes(-1);

            _database.TombstoneCleaner.Subscribe(this);
            IOExtensions.DeleteDirectory(_tempBackupPath.FullPath);
            Directory.CreateDirectory(_tempBackupPath.FullPath);
        }

        private Timer GetTimer(
            PeriodicBackupConfiguration configuration,
            PeriodicBackupStatus backupStatus)
        {
            var nextBackup = GetNextBackupDetails(configuration, backupStatus, _serverStore.NodeTag);
            if (nextBackup == null)
                return null;

            if (_logger.IsInfoEnabled)
                _logger.Info($"Next {(nextBackup.IsFull ? "full" : "incremental")} " +
                             $"backup is in {nextBackup.TimeSpan.TotalMinutes} minutes");

            var isValidTimeSpanForTimer = nextBackup.TimeSpan < MaxTimerTimeout;
            var timer = isValidTimeSpanForTimer
                ? new Timer(TimerCallback, nextBackup, nextBackup.TimeSpan, Timeout.InfiniteTimeSpan)
                : new Timer(LongPeriodTimerCallback, nextBackup, MaxTimerTimeout, Timeout.InfiniteTimeSpan);

            return timer;
        }

        public NextBackup GetNextBackupDetails(
            DatabaseRecord databaseRecord,
            PeriodicBackupConfiguration configuration,
            PeriodicBackupStatus backupStatus,
            string responsibleNodeTag)
        {
            var taskStatus = GetTaskStatus(databaseRecord.Topology, configuration, skipErrorLog: true);
            return taskStatus == TaskStatus.Disabled ? null : GetNextBackupDetails(configuration, backupStatus, responsibleNodeTag, skipErrorLog: true);
        }

        private DateTime? GetNextWakeupTimeLocal(long lastEtag, PeriodicBackupConfiguration configuration, PeriodicBackupStatus backupStatus)
        {
            // we will always wake up the database for a full backup.
            // but for incremental we will wake the database only if there were changes made.

            var now = SystemTime.UtcNow;

            if (backupStatus == null)
            {
                return GetNextBackupOccurrenceLocal(configuration.FullBackupFrequency, now, configuration, skipErrorLog: false);
            }

            if (backupStatus.LastEtag != lastEtag)
            {
                var lastIncrementalBackupUtc = backupStatus.LastIncrementalBackupInternal ?? backupStatus.LastFullBackupInternal ?? now;
                var nextLastIncrementalBackupLocal = GetNextBackupOccurrenceLocal(configuration.IncrementalBackupFrequency, lastIncrementalBackupUtc, configuration, skipErrorLog: false);

                if (nextLastIncrementalBackupLocal != null)
                    return nextLastIncrementalBackupLocal;
            }

            var lastFullBackup = backupStatus.LastFullBackupInternal ?? now;
            return GetNextBackupOccurrenceLocal(configuration.FullBackupFrequency, lastFullBackup, configuration, skipErrorLog: false);
        }

        private NextBackup GetNextBackupDetails(
            PeriodicBackupConfiguration configuration,
            PeriodicBackupStatus backupStatus,
            string responsibleNodeTag,
            bool skipErrorLog = false)
        {
            var nowUtc = SystemTime.UtcNow;
            var lastFullBackupUtc = backupStatus.LastFullBackupInternal ?? _databaseWakeUpTimeUtc ?? nowUtc;
            var lastIncrementalBackupUtc = backupStatus.LastIncrementalBackupInternal ?? backupStatus.LastFullBackupInternal ?? _databaseWakeUpTimeUtc ?? nowUtc;
            var nextFullBackup = GetNextBackupOccurrenceLocal(configuration.FullBackupFrequency,
                lastFullBackupUtc, configuration, skipErrorLog: skipErrorLog);
            var nextIncrementalBackup = GetNextBackupOccurrenceLocal(configuration.IncrementalBackupFrequency,
                lastIncrementalBackupUtc, configuration, skipErrorLog: skipErrorLog);

            if (nextFullBackup == null && nextIncrementalBackup == null)
            {
                var message = "Couldn't schedule next backup " +
                              $"full backup frequency: {configuration.FullBackupFrequency}, " +
                              $"incremental backup frequency: {configuration.IncrementalBackupFrequency}";
                if (string.IsNullOrWhiteSpace(configuration.Name) == false)
                    message += $", backup name: {configuration.Name}";

                _database.NotificationCenter.Add(AlertRaised.Create(
                    _database.Name,
                    "Couldn't schedule next backup, this shouldn't happen",
                    message,
                    AlertType.PeriodicBackup,
                    NotificationSeverity.Warning));

                return null;
            }

            Debug.Assert(configuration.TaskId != 0);

            var isFullBackup = IsFullBackup(backupStatus, configuration, nextFullBackup, nextIncrementalBackup, responsibleNodeTag);
            var nextBackupTimeLocal = GetNextBackupDateTime(nextFullBackup, nextIncrementalBackup);
            var nowLocalTime = SystemTime.UtcNow.ToLocalTime();
            var timeSpan = nextBackupTimeLocal - nowLocalTime;

            TimeSpan nextBackupTimeSpan;
            if (timeSpan.Ticks <= 0)
            {
                // overdue backup of current node or first backup
                if (backupStatus.NodeTag == _serverStore.NodeTag || backupStatus.NodeTag == null)
                {
                    // the backup will run now
                    nextBackupTimeSpan = TimeSpan.Zero;
                    nextBackupTimeLocal = nowLocalTime;
                }
                else
                {
                    // overdue backup from other node
                    nextBackupTimeSpan = TimeSpan.FromMinutes(1);
                    nextBackupTimeLocal = nowLocalTime + nextBackupTimeSpan;
                }
            }
            else
            {
                nextBackupTimeSpan = timeSpan;
            }

            return new NextBackup
            {
                TimeSpan = nextBackupTimeSpan,
                DateTime = nextBackupTimeLocal.ToUniversalTime(),
                IsFull = isFullBackup,
                TaskId = configuration.TaskId
            };
        }

        private bool IsFullBackup(PeriodicBackupStatus backupStatus,
            PeriodicBackupConfiguration configuration,
            DateTime? nextFullBackup, DateTime? nextIncrementalBackup, string responsibleNodeTag)
        {
            if (backupStatus.LastFullBackup == null ||
                backupStatus.NodeTag != responsibleNodeTag ||
                backupStatus.BackupType != configuration.BackupType ||
                backupStatus.LastEtag == null)
            {
                // Reasons to start a new full backup:
                // 1. there is no previous full backup, we are going to create one now
                // 2. the node which is responsible for the backup was replaced
                // 3. the backup type changed (e.g. from backup to snapshot)
                // 4. last etag wasn't updated

                return true;
            }

            // 1. there is a full backup setup but the next incremental backup wasn't setup
            // 2. there is a full backup setup and the next full backup is before the incremental one
            return nextFullBackup != null &&
                   (nextIncrementalBackup == null || nextFullBackup <= nextIncrementalBackup);
        }

        private static bool IsFullBackupOrSnapshot(string filePath)
        {
            var extension = Path.GetExtension(filePath);
            return Constants.Documents.PeriodicBackup.FullBackupExtension.Equals(extension, StringComparison.OrdinalIgnoreCase) ||
                   Constants.Documents.PeriodicBackup.SnapshotExtension.Equals(extension, StringComparison.OrdinalIgnoreCase) ||
                   Constants.Documents.PeriodicBackup.EncryptedFullBackupExtension.Equals(extension, StringComparison.OrdinalIgnoreCase) ||
                   Constants.Documents.PeriodicBackup.EncryptedSnapshotExtension.Equals(extension, StringComparison.OrdinalIgnoreCase);
        }

        private static DateTime GetNextBackupDateTime(DateTime? nextFullBackup, DateTime? nextIncrementalBackup)
        {
            Debug.Assert(nextFullBackup != null || nextIncrementalBackup != null);

            if (nextFullBackup == null)
                return nextIncrementalBackup.Value;

            if (nextIncrementalBackup == null)
                return nextFullBackup.Value;

            var nextBackup = nextFullBackup <= nextIncrementalBackup ? nextFullBackup.Value : nextIncrementalBackup.Value;
            return nextBackup;
        }

        private DateTime? GetNextBackupOccurrenceLocal(string backupFrequency,
            DateTime lastBackupUtc, PeriodicBackupConfiguration configuration, bool skipErrorLog)
        {
            if (string.IsNullOrWhiteSpace(backupFrequency))
                return null;

            try
            {
                var backupParser = CrontabSchedule.Parse(backupFrequency);
                return backupParser.GetNextOccurrence(lastBackupUtc.ToLocalTime());
            }
            catch (Exception e)
            {
                if (skipErrorLog == false)
                {
                    var message = "Couldn't parse periodic backup " +
                                  $"frequency {backupFrequency}, task id: {configuration.TaskId}";
                    if (string.IsNullOrWhiteSpace(configuration.Name) == false)
                        message += $", backup name: {configuration.Name}";

                    message += $", error: {e.Message}";

                    if (_logger.IsInfoEnabled)
                        _logger.Info(message);

                    _database.NotificationCenter.Add(AlertRaised.Create(
                        _database.Name,
                        "Backup frequency parsing error",
                        message,
                        AlertType.PeriodicBackup,
                        NotificationSeverity.Error,
                        details: new ExceptionDetails(e)));
                }

                return null;
            }
        }

        private void TimerCallback(object backupTaskDetails)
        {
            try
            {
                if (_cancellationToken.IsCancellationRequested)
                    return;

                var backupDetails = (NextBackup)backupTaskDetails;

                if (ShouldRunBackupAfterTimerCallback(backupDetails, out PeriodicBackup periodicBackup) == false)
                    return;

                StartBackupTaskAndRescheduleIfNeeded(periodicBackup, backupDetails);
            }
            catch (Exception e)
            {
                _logger.Operations("Error during timer callback", e);
            }
        }

        private void LongPeriodTimerCallback(object backupTaskDetails)
        {
            try
            {
                if (_cancellationToken.IsCancellationRequested)
                    return;

                var backupDetails = (NextBackup)backupTaskDetails;

                if (ShouldRunBackupAfterTimerCallback(backupDetails, out PeriodicBackup periodicBackup) == false)
                    return;

                var remainingInterval = backupDetails.TimeSpan - MaxTimerTimeout;
                if (remainingInterval.TotalMilliseconds <= 0)
                {
                    StartBackupTaskAndRescheduleIfNeeded(periodicBackup, backupDetails);
                    return;
                }

                periodicBackup.UpdateTimer(GetTimer(periodicBackup.Configuration, periodicBackup.BackupStatus));
            }
            catch (Exception e)
            {
                _logger.Operations("Error during long timer callback", e);
            }
        }

        private void StartBackupTaskAndRescheduleIfNeeded(PeriodicBackup periodicBackup, NextBackup currentBackup)
        {
            try
            {
                CreateBackupTask(periodicBackup, currentBackup.IsFull, currentBackup.DateTime);
            }
            catch (BackupDelayException e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Backup task will be retried in {(int)e.DelayPeriod.TotalSeconds} seconds.", e);

                // we'll retry in one minute
                var backupTaskDetails = new NextBackup
                {
                    IsFull = currentBackup.IsFull,
                    TaskId = periodicBackup.Configuration.TaskId,
                    DateTime = DateTime.UtcNow.Add(e.DelayPeriod),
                    TimeSpan = e.DelayPeriod
                };

                var timer = new Timer(TimerCallback, backupTaskDetails, backupTaskDetails.TimeSpan, Timeout.InfiniteTimeSpan);
                periodicBackup.UpdateTimer(timer);
            }
        }

        public string WhoseTaskIsIt(long taskId)
        {
            if (_periodicBackups.TryGetValue(taskId, out var periodicBackup) == false)
            {
                throw new InvalidOperationException($"Backup task id: {taskId} doesn't exist");
            }

            if (periodicBackup.Configuration.Disabled)
            {
                throw new InvalidOperationException($"Backup task id: {taskId} is disabled");
            }

            if (periodicBackup.Configuration.HasBackup() == false)
            {
                throw new InvalidOperationException($"All backup destinations are disabled for backup task id: {taskId}");
            }

            var topology = _serverStore.LoadDatabaseTopology(_database.Name);
            var backupStatus = GetBackupStatus(taskId);
            return _database.WhoseTaskIsIt(topology, periodicBackup.Configuration, backupStatus, keepTaskOnOriginalMemberNode: true);
        }

        public long StartBackupTask(long taskId, bool isFullBackup)
        {
            if (_periodicBackups.TryGetValue(taskId, out var periodicBackup) == false)
            {
                throw new InvalidOperationException($"Backup task id: {taskId} doesn't exist");
            }

            return CreateBackupTask(periodicBackup, isFullBackup, SystemTime.UtcNow);
        }

        public DateTime? GetWakeDatabaseTimeUtc()
        {
            long lastEtag;

            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var tx = context.OpenReadTransaction())
            {
                lastEtag = DocumentsStorage.ReadLastEtag(tx.InnerTransaction);
            }

            DateTime? wakeupDatabase = null;
            foreach (var backup in _periodicBackups)
            {
                var nextBackup = GetNextWakeupTimeLocal(lastEtag, backup.Value.Configuration, backup.Value.BackupStatus);
                if (nextBackup == null)
                    continue;

                if (wakeupDatabase == null)
                {
                    // first time
                    wakeupDatabase = nextBackup;
                }
                else if (nextBackup < wakeupDatabase)
                {
                    // next backup is earlier than the current one
                    wakeupDatabase = nextBackup.Value;
                }
            }

            return wakeupDatabase?.ToUniversalTime();
        }

        private long CreateBackupTask(PeriodicBackup periodicBackup, bool isFullBackup, DateTime startTimeInUtc)
        {
            using (periodicBackup.UpdateBackupTask())
            {
                if (periodicBackup.Disposed)
                    throw new InvalidOperationException("Backup task was already disposed");

                if (periodicBackup.RunningTask != null)
                    return periodicBackup.RunningBackupTaskId ?? -1;

                if (_serverStore.Server.CpuCreditsBalance.BackgroundTasksAlertRaised.IsRaised())
                {
                    throw new BackupDelayException(
                        $"Failed to start Backup Task: '{periodicBackup.Configuration.Name}'. " +
                        $"The task cannot run because the CPU credits allocated to this machine are nearing exhaustion.")
                    {
                        DelayPeriod = _serverStore.Configuration.Server.CpuCreditsExhaustionBackupDelay.AsTimeSpan
                    };
                }

                if (LowMemoryNotification.Instance.LowMemoryState)
                {
                    throw new BackupDelayException(
                        $"Failed to start Backup Task: '{periodicBackup.Configuration.Name}'. " +
                        $"The task cannot run because the server is in low memory state.")
                    {
                        DelayPeriod = _serverStore.Configuration.Backup.LowMemoryBackupDelay.AsTimeSpan
                    };
                }

                if (LowMemoryNotification.Instance.DirtyMemoryState.IsHighDirty)
                {
                    throw new BackupDelayException(
                        $"Failed to start Backup Task: '{periodicBackup.Configuration.Name}'. " +
                        $"The task cannot run because the server is in high dirty memory state.")
                    {
                        DelayPeriod = _serverStore.Configuration.Backup.LowMemoryBackupDelay.AsTimeSpan
                    };
                }

                var backupCounter = _serverStore.ConcurrentBackupsCounter.StartBackup(periodicBackup.Configuration.Name);

                try
                {
                    var backupStatus = periodicBackup.BackupStatus = GetBackupStatus(periodicBackup.Configuration.TaskId, periodicBackup.BackupStatus);
                    var backupToLocalFolder = PeriodicBackupConfiguration.CanBackupUsing(periodicBackup.Configuration.LocalSettings);

                    // check if we need to do a new full backup
                    if (backupStatus.LastFullBackup == null || // no full backup was previously performed
                        backupStatus.NodeTag != _serverStore.NodeTag || // last backup was performed by a different node
                        backupStatus.BackupType != periodicBackup.Configuration.BackupType || // backup type has changed
                        backupStatus.LastEtag == null || // last document etag wasn't updated
                        backupToLocalFolder && BackupTask.DirectoryContainsBackupFiles(backupStatus.LocalBackup.BackupDirectory, IsFullBackupOrSnapshot) == false)
                    // the local folder already includes a full backup or snapshot
                    {
                        isFullBackup = true;
                    }

                    var operationId = _database.Operations.GetNextOperationId();
                    var backupTypeText = GetBackupTypeText(isFullBackup, periodicBackup.Configuration.BackupType);

                    periodicBackup.StartTimeInUtc = startTimeInUtc;
                    var backupTask = new BackupTask(
                        _serverStore,
                        _database,
                        periodicBackup,
                        isFullBackup,
                        backupToLocalFolder,
                        operationId,
                        _tempBackupPath,
                        _logger,
                        _cancellationToken.Token);

                    periodicBackup.RunningBackupTaskId = operationId;
                    periodicBackup.CancelToken = backupTask.TaskCancelToken;
                    var backupTaskName = $"{backupTypeText} backup task: '{periodicBackup.Configuration.Name}'. Database: '{_database.Name}'";

                    var task = _database.Operations.AddOperation(
                        null,
                        backupTaskName,
                        Operations.Operations.OperationType.DatabaseBackup,
                        taskFactory: onProgress => StartBackupThread(periodicBackup, backupTask, backupCounter, onProgress),
                        id: operationId,
                        token: backupTask.TaskCancelToken);

                    periodicBackup.RunningTask = task;
                    task.ContinueWith(_ => backupTask.TaskCancelToken.Dispose());

                    return operationId;
                }
                catch (Exception e)
                {
                    backupCounter.Dispose();

                    var message = $"Failed to start the backup task: '{periodicBackup.Configuration.Name}'";
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations(message, e);

                    _database.NotificationCenter.Add(AlertRaised.Create(
                        _database.Name,
                        $"Periodic Backup task: '{periodicBackup.Configuration.Name}'",
                        message,
                        AlertType.PeriodicBackup,
                        NotificationSeverity.Error,
                        details: new ExceptionDetails(e)));

                    throw;
                }
            }
        }

        private Task<IOperationResult> StartBackupThread(PeriodicBackup periodicBackup, BackupTask backupTask, IDisposable backupCounter, Action<IOperationProgress> onProgress)
        {
            var tcs = new TaskCompletionSource<IOperationResult>(TaskCreationOptions.RunContinuationsAsynchronously);
            PoolOfThreads.GlobalRavenThreadPool.LongRunning(x => RunBackupThread(periodicBackup, backupTask, backupCounter, onProgress, tcs), null, $"Backup task {periodicBackup.Configuration.Name} for database '{_database.Name}'");
            return tcs.Task;
        }

        private void RunBackupThread(PeriodicBackup periodicBackup, BackupTask backupTask, IDisposable backupCounter, Action<IOperationProgress> onProgress, TaskCompletionSource<IOperationResult> tcs)
        {
            try
            {
                Thread.CurrentThread.Priority = ThreadPriority.BelowNormal;
                NativeMemory.EnsureRegistered();

                using (_database.PreventFromUnloading())
                {
                    tcs.SetResult(backupTask.RunPeriodicBackup(onProgress));
                }
            }
            catch (OperationCanceledException)
            {
                tcs.SetCanceled();
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Failed to run the backup thread: '{periodicBackup.Configuration.Name}'", e);

                tcs.SetException(e);
            }
            finally
            {
                try
                {
                    backupCounter.Dispose();

                    periodicBackup.RunningTask = null;
                    periodicBackup.RunningBackupTaskId = null;
                    periodicBackup.CancelToken = null;
                    periodicBackup.RunningBackupStatus = null;

                    if (periodicBackup.HasScheduledBackup() && _cancellationToken.IsCancellationRequested == false)
                    {
                        var newBackupTimer = GetTimer(periodicBackup.Configuration, periodicBackup.BackupStatus);
                        periodicBackup.UpdateTimer(newBackupTimer, discardIfDisabled: true);
                    }
                }
                catch (Exception e)
                {
                    var msg = $"Failed to schedule next backup for backup thread: '{periodicBackup.Configuration.Name}'";
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations(msg, e);

                    _database.NotificationCenter.Add(AlertRaised.Create(
                        _database.Name,
                        "Couldn't schedule next backup.",
                        msg,
                        AlertType.PeriodicBackup,
                        NotificationSeverity.Warning,
                        details: new ExceptionDetails(e)));
                }
            }
        }

        private static string GetBackupTypeText(bool isFullBackup, BackupType backupType)
        {
            if (backupType == BackupType.Backup)
            {
                return isFullBackup ? "Full" : "Incremental";
            }

            return isFullBackup ? "Snapshot" : "Incremental Snapshot";
        }

        private bool ShouldRunBackupAfterTimerCallback(NextBackup backupInfo, out PeriodicBackup periodicBackup)
        {
            if (_periodicBackups.TryGetValue(backupInfo.TaskId, out periodicBackup) == false)
            {
                // periodic backup doesn't exist anymore
                return false;
            }

            DatabaseTopology topology;
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            using (var rawRecord = _serverStore.Cluster.ReadDatabaseRecord(context, _database.Name))
            {
                if (rawRecord == null)
                    return false;

                topology = rawRecord.GetTopology();
            }

            var taskStatus = GetTaskStatus(topology, periodicBackup.Configuration);
            return taskStatus == TaskStatus.ActiveByCurrentNode;
        }

        public PeriodicBackupStatus GetBackupStatus(long taskId)
        {
            PeriodicBackupStatus inMemoryBackupStatus = null;
            if (_periodicBackups.TryGetValue(taskId, out PeriodicBackup periodicBackup))
                inMemoryBackupStatus = periodicBackup.BackupStatus;

            return GetBackupStatus(taskId, inMemoryBackupStatus);
        }

        private PeriodicBackupStatus GetBackupStatus(long taskId, PeriodicBackupStatus inMemoryBackupStatus)
        {
            var backupStatus = GetBackupStatusFromCluster(_serverStore, _database.Name, taskId);
            if (backupStatus == null)
            {
                backupStatus = inMemoryBackupStatus ?? new PeriodicBackupStatus
                {
                    TaskId = taskId
                };
            }
            else if (inMemoryBackupStatus?.Version > backupStatus.Version &&
                     inMemoryBackupStatus?.NodeTag == backupStatus.NodeTag)
            {
                // the in memory backup status is more updated
                // and is of the same node (current one)
                backupStatus = inMemoryBackupStatus;
            }

            return backupStatus;
        }

        private static PeriodicBackupStatus GetBackupStatusFromCluster(ServerStore serverStore, string databaseName, long taskId)
        {
            using (serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                return GetBackupStatusFromCluster(serverStore, context, databaseName, taskId);
            }
        }

        internal static PeriodicBackupStatus GetBackupStatusFromCluster(ServerStore serverStore, TransactionOperationContext context, string databaseName, long taskId)
        {
            var statusBlittable = serverStore.Cluster.Read(context, PeriodicBackupStatus.GenerateItemName(databaseName, taskId));

            if (statusBlittable == null)
                return null;

            var periodicBackupStatusJson = JsonDeserializationClient.PeriodicBackupStatus(statusBlittable);
            return periodicBackupStatusJson;
        }

        private long GetMinLastEtag()
        {
            var min = long.MaxValue;

            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var record = _serverStore.Cluster.ReadDatabaseRecord(context, _database.Name);
                foreach (var taskId in record.GetPeriodicBackupsTaskIds())
                {
                    var config = record.GetPeriodicBackupConfiguration(taskId);
                    if (config.IncrementalBackupFrequency == null)
                        continue; // if the backup is always full, we don't need to take into account the tombstones, since we never back them up.

                    var status = GetBackupStatusFromCluster(_serverStore, context, _database.Name, taskId);
                    var etag = ChangeVectorUtils.GetEtagById(status.LastDatabaseChangeVector, _database.DbBase64Id);
                    min = Math.Min(etag, min);
                }

                return min;
            }
        }

        public void UpdateConfigurations(RawDatabaseRecord databaseRecord)
        {
            if (_disposed)
                return;

            var periodicBackups = databaseRecord.GetPeriodicBackupConfigurations();

            if (periodicBackups == null)
            {
                foreach (var periodicBackup in _periodicBackups)
                {
                    periodicBackup.Value.Dispose();
                }
                _periodicBackups.Clear();
                return;
            }

            var allBackupTaskIds = new List<long>();
            foreach (var periodicBackupConfiguration in periodicBackups)
            {
                var newBackupTaskId = periodicBackupConfiguration.TaskId;
                allBackupTaskIds.Add(newBackupTaskId);

                var taskState = GetTaskStatus(databaseRecord.GetTopology(), periodicBackupConfiguration);

                UpdatePeriodicBackup(newBackupTaskId, periodicBackupConfiguration, taskState);
            }

            var deletedBackupTaskIds = _periodicBackups.Keys.Except(allBackupTaskIds).ToList();
            foreach (var deletedBackupId in deletedBackupTaskIds)
            {
                if (_periodicBackups.TryRemove(deletedBackupId, out var deletedBackup) == false)
                    continue;

                // stopping any future backups
                // currently running backups will continue to run
                deletedBackup.Dispose();
            }
        }

        private void UpdatePeriodicBackup(long taskId,
            PeriodicBackupConfiguration newConfiguration,
            TaskStatus taskState)
        {
            Debug.Assert(taskId == newConfiguration.TaskId);

            var backupStatus = GetBackupStatus(taskId, inMemoryBackupStatus: null);
            if (_periodicBackups.TryGetValue(taskId, out var existingBackupState) == false)
            {
                var newPeriodicBackup = new PeriodicBackup(_inactiveRunningPeriodicBackupsTasks)
                {
                    Configuration = newConfiguration
                };

                var periodicBackup = _periodicBackups.GetOrAdd(taskId, newPeriodicBackup);
                if (periodicBackup != newPeriodicBackup)
                {
                    newPeriodicBackup.Dispose();
                }

                if (taskState == TaskStatus.ActiveByCurrentNode)
                    periodicBackup.UpdateTimer(GetTimer(newConfiguration, backupStatus));

                return;
            }

            var previousConfiguration = existingBackupState.Configuration;
            existingBackupState.Configuration = newConfiguration;

            switch (taskState)
            {
                case TaskStatus.Disabled:
                case TaskStatus.ActiveByOtherNode:
                    // the task is disabled or this node isn't responsible for the backup task
                    existingBackupState.DisableFutureBackups();
                    return;
                case TaskStatus.ClusterDown:
                    // this node cannot connect to cluster, the task will continue on this node
                    return;
                case TaskStatus.ActiveByCurrentNode:
                    // a backup is already running, the next one will be re-scheduled by the backup task if needed
                    if (existingBackupState.RunningTask != null)
                        return;

                    // backup frequency hasn't changed, and we have a scheduled backup
                    if (previousConfiguration.HasBackupFrequencyChanged(newConfiguration) == false && existingBackupState.HasScheduledBackup())
                        return;

                    existingBackupState.UpdateTimer(GetTimer(newConfiguration, backupStatus));
                    return;
                default:
                    throw new ArgumentOutOfRangeException(nameof(taskState), taskState, null);
            }
        }

        private enum TaskStatus
        {
            Disabled,
            ActiveByCurrentNode,
            ActiveByOtherNode,
            ClusterDown
        }

        private TaskStatus GetTaskStatus(
            DatabaseTopology topology,
            PeriodicBackupConfiguration configuration,
            bool skipErrorLog = false)
        {
            if (configuration.Disabled)
                return TaskStatus.Disabled;

            if (configuration.HasBackup() == false)
            {
                if (skipErrorLog == false)
                {
                    var message = $"All backup destinations are disabled for backup task id: {configuration.TaskId}";
                    _database.NotificationCenter.Add(AlertRaised.Create(
                        _database.Name,
                        "Periodic Backup",
                        message,
                        AlertType.PeriodicBackup,
                        NotificationSeverity.Info));
                }

                return TaskStatus.Disabled;
            }

            var backupStatus = GetBackupStatus(configuration.TaskId);
            var whoseTaskIsIt = _database.WhoseTaskIsIt(topology, configuration, backupStatus, keepTaskOnOriginalMemberNode: true);
            if (whoseTaskIsIt == null)
                return TaskStatus.ClusterDown;

            if (whoseTaskIsIt == _serverStore.NodeTag)
                return TaskStatus.ActiveByCurrentNode;

            if (_logger.IsInfoEnabled)
                _logger.Info($"Backup job is skipped at {SystemTime.UtcNow}, because it is managed " +
                             $"by '{whoseTaskIsIt}' node and not the current node ({_serverStore.NodeTag})");

            return TaskStatus.ActiveByOtherNode;
        }

        private void WaitForTaskCompletion(Task task)
        {
            try
            {
                task?.Wait();
            }
            catch (ObjectDisposedException)
            {
                // shutting down, probably
            }
            catch (AggregateException e) when (e.InnerException is OperationCanceledException)
            {
                // shutting down
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Error when disposing periodic backup runner task", e);
            }
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            lock (this)
            {
                if (_disposed)
                    return;

                _disposed = true;
                _database.TombstoneCleaner.Unsubscribe(this);

                using (_cancellationToken)
                {
                    _cancellationToken.Cancel();

                    foreach (var periodicBackup in _periodicBackups)
                    {
                        periodicBackup.Value.Dispose();
                    }

                    foreach (var inactiveTask in _inactiveRunningPeriodicBackupsTasks)
                    {
                        WaitForTaskCompletion(inactiveTask);
                    }
                }

                if (_tempBackupPath != null)
                    IOExtensions.DeleteDirectory(_tempBackupPath.FullPath);
            }
        }

        public bool HasRunningBackups()
        {
            foreach (var periodicBackup in _periodicBackups)
            {
                if (periodicBackup.Value.RunningTask != null &&
                    periodicBackup.Value.RunningTask.IsCompleted == false)
                    return true;
            }

            return false;
        }

        public BackupInfo GetBackupInfo()
        {
            if (_periodicBackups.Count == 0)
                return null;

            var allBackupTicks = new List<long>();
            var allNextBackupTimeSpanSeconds = new List<double>();
            foreach (var periodicBackup in _periodicBackups)
            {
                var configuration = periodicBackup.Value.Configuration;
                var backupStatus = GetBackupStatus(configuration.TaskId, periodicBackup.Value.BackupStatus);
                if (backupStatus == null)
                    continue;

                if (backupStatus.LastFullBackup != null)
                    allBackupTicks.Add(backupStatus.LastFullBackup.Value.Ticks);

                if (backupStatus.LastIncrementalBackup != null)
                    allBackupTicks.Add(backupStatus.LastIncrementalBackup.Value.Ticks);

                var nextBackup = GetNextBackupDetails(configuration, backupStatus, _serverStore.NodeTag, skipErrorLog: true);
                if (nextBackup != null)
                {
                    allNextBackupTimeSpanSeconds.Add(nextBackup.TimeSpan.TotalSeconds);
                }
            }

            return new BackupInfo
            {
                LastBackup = allBackupTicks.Count == 0 ? (DateTime?)null : new DateTime(allBackupTicks.Max()),
                IntervalUntilNextBackupInSec = allNextBackupTimeSpanSeconds.Count == 0 ? 0 : allNextBackupTimeSpanSeconds.Min()
            };
        }

        public RunningBackup OnGoingBackup(long taskId)
        {
            if (_periodicBackups.TryGetValue(taskId, out var periodicBackup) == false)
                return null;

            if (periodicBackup.RunningTask == null)
                return null;

            return new RunningBackup
            {
                StartTime = periodicBackup.StartTimeInUtc,
                IsFull = periodicBackup.RunningBackupStatus?.IsFull ?? false,
                RunningBackupTaskId = periodicBackup.RunningBackupTaskId
            };
        }

        public string TombstoneCleanerIdentifier => "Periodic Backup";

        public Dictionary<string, long> GetLastProcessedTombstonesPerCollection()
        {
            var minLastEtag = GetMinLastEtag();

            if (minLastEtag == long.MaxValue)
                return EmptyDictionary;

            return new Dictionary<string, long>
            {
                [Constants.Documents.Collections.AllDocumentsCollection] = minLastEtag
            };
        }
    }
}
