using System;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Commercial;
using Sparrow.Logging;
using BackupConfiguration = Raven.Server.Config.Categories.BackupConfiguration;

namespace Raven.Server.Documents.PeriodicBackup
{
    public sealed class ConcurrentBackupsCounter
    {
        private readonly object _locker = new object();

        private readonly LicenseManager _licenseManager;
        private int _concurrentBackups;
        private int _maxConcurrentBackups;
        private readonly TimeSpan _concurrentBackupsDelay;
        private readonly bool _skipModifications;

        public int MaxNumberOfConcurrentBackups
        {
            get
            {
                lock (_locker)
                {
                    return _maxConcurrentBackups;
                }
            }
        }

        public int CurrentNumberOfRunningBackups
        {
            get
            {
                lock (_locker)
                {
                    return _maxConcurrentBackups - _concurrentBackups;
                }
            }
        }

        public ConcurrentBackupsCounter(BackupConfiguration backupConfiguration, LicenseManager licenseManager)
        {
            _licenseManager = licenseManager;

            int numberOfCoresToUse;
            var skipModifications = backupConfiguration.MaxNumberOfConcurrentBackups != null;
            if (skipModifications)
            {
                numberOfCoresToUse = backupConfiguration.MaxNumberOfConcurrentBackups.Value;
            }
            else
            {
                var utilizedCores = _licenseManager.GetCoresLimitForNode(out _, false);
                numberOfCoresToUse = GetNumberOfCoresToUseForBackup(utilizedCores);
            }

            _concurrentBackups = numberOfCoresToUse;
            _maxConcurrentBackups = numberOfCoresToUse;
            _concurrentBackupsDelay = backupConfiguration.ConcurrentBackupsDelay.AsTimeSpan;
            _skipModifications = skipModifications;
        }

        public void StartBackup(string backupName, Logger logger)
        {
            lock (_locker)
            {
                if (_concurrentBackups <= 0)
                {
                    throw new BackupDelayException(
                        $"Failed to start Backup Task: '{backupName}'. " +
                        $"The task exceeds the maximum number of concurrent backup tasks configured. " +
                        $"Current maximum number of concurrent backups is: {_maxConcurrentBackups:#,#;;0}")
                    {
                        DelayPeriod = _concurrentBackupsDelay
                    };
                }

                _concurrentBackups--;
            }

            if (logger.IsOperationsEnabled)
                logger.Operations($"Starting backup task '{backupName}'");
        }

        public void FinishBackup(string nodeTag, string backupName, PeriodicBackupStatus backupStatus, TimeSpan? elapsed, Logger logger)
        {
            lock (_locker)
            {
                _concurrentBackups++;
            }

            if (logger.IsOperationsEnabled)
            {
                string backupTypeString = "backup";
                string extendedBackupTimings = string.Empty;
                if (backupStatus != null)
                {
                    var backupStatusPerNode = backupStatus.GetStatusPerNode(nodeTag, out _);
                    backupTypeString = BackupTask.GetBackupDescription(backupStatus.BackupType, backupStatusPerNode.IsFull);
                    
                    var first = true;
                    AddBackupTimings(backupStatusPerNode.LocalBackup, "local");
                    AddBackupTimings(backupStatusPerNode.UploadToS3, "Amazon S3");
                    AddBackupTimings(backupStatusPerNode.UploadToGlacier, "Amazon Glacier");
                    AddBackupTimings(backupStatusPerNode.UploadToAzure, "Azure");
                    AddBackupTimings(backupStatusPerNode.UploadToGoogleCloud, "Google Cloud");
                    AddBackupTimings(backupStatusPerNode.UploadToFtp, "FTP");

                    void AddBackupTimings(IReadOnlyBackupStatus perDestinationBackupStatus, string backupTypeName)
                    {
                        if (perDestinationBackupStatus == null || 
                            perDestinationBackupStatus is CloudUploadStatus cus && cus.Skipped)
                            return;

                        if (first == false)
                            extendedBackupTimings += ", ";

                        first = false;
                        extendedBackupTimings +=
                            $"backup to {backupTypeName} took: " +
                            $"{(backupStatusPerNode.IsFull ? perDestinationBackupStatus.FullBackupDurationInMs : perDestinationBackupStatus.IncrementalBackupDurationInMs)}ms";
                    }
                }

                var message = $"Finished {backupTypeString} task '{backupName}'";
                if (elapsed != null)
                    message += $", took: {elapsed}";

                message += $" {extendedBackupTimings}";

                logger.Operations(message);
            }
        }

        public void ModifyMaxConcurrentBackups()
        {
            if (_skipModifications)
                return;

            var utilizedCores = _licenseManager.GetCoresLimitForNode(out _);
            var newMaxConcurrentBackups = GetNumberOfCoresToUseForBackup(utilizedCores);

            lock (_locker)
            {
                var diff = newMaxConcurrentBackups - _maxConcurrentBackups;
                _maxConcurrentBackups = newMaxConcurrentBackups;
                _concurrentBackups += diff;
            }
        }

        public int GetNumberOfCoresToUseForBackup(int utilizedCores)
        {
            return Math.Max(1, utilizedCores / 2);
        }
    }
}
