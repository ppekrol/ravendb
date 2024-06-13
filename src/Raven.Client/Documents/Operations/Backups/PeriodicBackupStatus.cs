using System;
using System.Collections.Generic;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Backups
{
    internal interface IReadOnlyPeriodicBackupStatusPerNode
    {
        DateTime? LastFullBackup { get; }
        DateTime? LastIncrementalBackup { get; }
        DateTime? LastFullBackupInternal { get; }
        DateTime? LastIncrementalBackupInternal { get; }
        long? LastEtag { get; }
        string LastDatabaseChangeVector { get; }
        IReadOnlyLocalBackup LocalBackup { get; }
        IReadOnlyLastRaftIndex LastRaftIndex { get; }
        bool IsFull { get; }
        string FolderName { get; }
        long? LastOperationId { get; }
        IReadOnlyError Error { get; }
        long? DurationInMs { get; }
        IReadOnlyUploadToS3 UploadToS3 { get; }
        IReadOnlyUploadToGlacier UploadToGlacier { get; }
        IReadOnlyUploadToAzure UploadToAzure { get; }
        IReadOnlyUploadToGoogleCloud UploadToGoogleCloud { get; }
        IReadOnlyUploadToFtp UploadToFtp { get; }
        long? LocalRetentionDurationInMs { get; }
    }

    public sealed class PeriodicBackupStatusPerNode : IReadOnlyPeriodicBackupStatusPerNode
    {
        public DateTime? LastFullBackup { get; set; }

        public DateTime? LastIncrementalBackup { get; set; }

        public DateTime? LastFullBackupInternal { get; set; }

        public DateTime? LastIncrementalBackupInternal { get; set; }

        public long? LastEtag { get; set; }

        public string LastDatabaseChangeVector { get; set; }

        public LocalBackup LocalBackup { get; set; }

        public LastRaftIndex LastRaftIndex { get; set; }

        public bool IsFull { get; set; }

        public string FolderName { get; set; }

        public long? LastOperationId { get; set; }

        public Error Error { get; set; }

        public long? DurationInMs { get; set; }

        public UploadToS3 UploadToS3 { get; set; }

        public UploadToGlacier UploadToGlacier { get; set; }

        public UploadToAzure UploadToAzure { get; set; }

        public UploadToGoogleCloud UploadToGoogleCloud { get; set; }

        public UploadToFtp UploadToFtp { get; set; }

        public long? LocalRetentionDurationInMs { get; set; }

        IReadOnlyLocalBackup IReadOnlyPeriodicBackupStatusPerNode.LocalBackup => LocalBackup;

        IReadOnlyLastRaftIndex IReadOnlyPeriodicBackupStatusPerNode.LastRaftIndex => LastRaftIndex;

        IReadOnlyError IReadOnlyPeriodicBackupStatusPerNode.Error => Error;

        IReadOnlyUploadToS3 IReadOnlyPeriodicBackupStatusPerNode.UploadToS3 => UploadToS3;

        IReadOnlyUploadToGlacier IReadOnlyPeriodicBackupStatusPerNode.UploadToGlacier => UploadToGlacier;

        IReadOnlyUploadToAzure IReadOnlyPeriodicBackupStatusPerNode.UploadToAzure => UploadToAzure;

        IReadOnlyUploadToGoogleCloud IReadOnlyPeriodicBackupStatusPerNode.UploadToGoogleCloud => UploadToGoogleCloud;

        IReadOnlyUploadToFtp IReadOnlyPeriodicBackupStatusPerNode.UploadToFtp => UploadToFtp;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(IsFull)] = IsFull,
                [nameof(LastFullBackup)] = LastFullBackup,
                [nameof(LastIncrementalBackup)] = LastIncrementalBackup,
                [nameof(LastFullBackupInternal)] = LastFullBackupInternal,
                [nameof(LastIncrementalBackupInternal)] = LastIncrementalBackupInternal,
                [nameof(LocalBackup)] = LocalBackup?.ToJson(),
                [nameof(UploadToS3)] = UploadToS3?.ToJson(),
                [nameof(UploadToGlacier)] = UploadToGlacier?.ToJson(),
                [nameof(UploadToAzure)] = UploadToAzure?.ToJson(),
                [nameof(UploadToGoogleCloud)] = UploadToGoogleCloud?.ToJson(),
                [nameof(UploadToFtp)] = UploadToFtp?.ToJson(),
                [nameof(LastEtag)] = LastEtag,
                [nameof(LastRaftIndex)] = LastRaftIndex?.ToJson(),
                [nameof(FolderName)] = FolderName,
                [nameof(DurationInMs)] = DurationInMs,
                [nameof(LocalRetentionDurationInMs)] = LocalRetentionDurationInMs,
                [nameof(Error)] = Error?.ToJson(),
                [nameof(LastOperationId)] = LastOperationId,
                [nameof(LastDatabaseChangeVector)] = LastDatabaseChangeVector
            };
        }
    }

    public sealed class PeriodicBackupStatus : IDatabaseTaskStatus
    {
        public Dictionary<string, PeriodicBackupStatusPerNode> StatusPerNode { get; set; }

        public long TaskId { get; set; }

        public BackupType BackupType { get; set; }

        [Obsolete]
        public bool IsFull { get; set; }

        public string NodeTag { get; set; }

        public DateTime? DelayUntil { get; set; }

        public DateTime? OriginalBackupTime { get; set; }

        [Obsolete]
        public DateTime? LastFullBackup { get; set; }

        [Obsolete]
        public DateTime? LastIncrementalBackup { get; set; }

        [Obsolete]
        public DateTime? LastFullBackupInternal { get; set; }

        [Obsolete]
        public DateTime? LastIncrementalBackupInternal { get; set; }

        [Obsolete]
        public LocalBackup LocalBackup { get; set; }

        [Obsolete]
        public UploadToS3 UploadToS3;

        [Obsolete]
        public UploadToGlacier UploadToGlacier;

        [Obsolete]
        public UploadToAzure UploadToAzure;

        [Obsolete]
        public UploadToGoogleCloud UploadToGoogleCloud;

        [Obsolete]
        public UploadToFtp UploadToFtp;

        [Obsolete]
        public long? LastEtag { get; set; }

        [Obsolete]
        public string LastDatabaseChangeVector { get; set; }

        [Obsolete]
        public LastRaftIndex LastRaftIndex { get; set; }

        [Obsolete]
        public string FolderName { get; set; }

        [Obsolete]
        public long? DurationInMs { get; set; }

        [Obsolete]
        public long? LocalRetentionDurationInMs { get; set; }

        public long Version { get; set; }

        [Obsolete]
        public Error Error { get; set; }

        [Obsolete]
        public long? LastOperationId { get; set; }

        public bool IsEncrypted { get; set; }

        public DynamicJsonValue ToJson()
        {
            var json = new DynamicJsonValue();
            UpdateJson(json);
            return json;
        }

        public void UpdateJson(DynamicJsonValue json)
        {
#pragma warning disable CS0612 // Type or member is obsolete
            json[nameof(TaskId)] = TaskId;
            json[nameof(BackupType)] = BackupType;
            json[nameof(IsFull)] = IsFull;
            json[nameof(NodeTag)] = NodeTag;
            json[nameof(DelayUntil)] = DelayUntil;
            json[nameof(OriginalBackupTime)] = OriginalBackupTime;
            json[nameof(LastFullBackup)] = LastFullBackup;
            json[nameof(LastIncrementalBackup)] = LastIncrementalBackup;
            json[nameof(LastFullBackupInternal)] = LastFullBackupInternal;
            json[nameof(LastIncrementalBackupInternal)] = LastIncrementalBackupInternal;
            json[nameof(LocalBackup)] = LocalBackup?.ToJson();
            json[nameof(UploadToS3)] = UploadToS3?.ToJson();
            json[nameof(UploadToGlacier)] = UploadToGlacier?.ToJson();
            json[nameof(UploadToAzure)] = UploadToAzure?.ToJson();
            json[nameof(UploadToGoogleCloud)] = UploadToGoogleCloud?.ToJson();
            json[nameof(UploadToFtp)] = UploadToFtp?.ToJson();
            json[nameof(LastEtag)] = LastEtag;
            json[nameof(LastRaftIndex)] = LastRaftIndex?.ToJson();
            json[nameof(FolderName)] = FolderName;
            json[nameof(DurationInMs)] = DurationInMs;
            json[nameof(LocalRetentionDurationInMs)] = LocalRetentionDurationInMs;
            json[nameof(Version)] = Version;
            json[nameof(Error)] = Error?.ToJson();
            json[nameof(LastOperationId)] = LastOperationId;
            json[nameof(LastDatabaseChangeVector)] = LastDatabaseChangeVector;
            json[nameof(IsEncrypted)] = IsEncrypted;
#pragma warning restore CS0612 // Type or member is obsolete

            DynamicJsonValue djv = null;
            if (StatusPerNode != null)
            {
                djv = new DynamicJsonValue();
                foreach (var kvp in StatusPerNode)
                    djv[kvp.Key] = kvp.Value.ToJson();
            }

            json[nameof(StatusPerNode)] = djv;
        }

        public static string Prefix => "periodic-backups/";

        public static string GenerateItemName(string databaseName, long taskId)
        {
            return $"values/{databaseName}/{Prefix}{taskId}";
        }

        internal void SetLastIncrementalBackup(string nodeTag, DateTime? lastIncrementalBackup)
        {
            var statusPerNode = EnsureStatusPerNode(nodeTag);

            statusPerNode.LastIncrementalBackup = statusPerNode.LastIncrementalBackupInternal = lastIncrementalBackup;
            statusPerNode.LocalBackup.LastIncrementalBackup = lastIncrementalBackup;
            statusPerNode.LocalBackup.IncrementalBackupDurationInMs = 0;

            // backward compatibility
#pragma warning disable CS0612 // Type or member is obsolete
            LastIncrementalBackup = LastIncrementalBackupInternal = lastIncrementalBackup;
            LocalBackup.LastIncrementalBackup = lastIncrementalBackup;
            LocalBackup.IncrementalBackupDurationInMs = 0;
#pragma warning restore CS0612 // Type or member is obsolete
        }

        private PeriodicBackupStatusPerNode EnsureStatusPerNode(string nodeTag)
        {
            StatusPerNode ??= new Dictionary<string, PeriodicBackupStatusPerNode>();
            if (StatusPerNode.TryGetValue(nodeTag, out var statusPerNode) == false)
                StatusPerNode[nodeTag] = statusPerNode = new PeriodicBackupStatusPerNode();

            return statusPerNode;
        }

        internal IReadOnlyPeriodicBackupStatusPerNode GetStatusPerNode(string nodeTag, out string lastNodeTag)
        {
            lastNodeTag = null;
            if (string.IsNullOrEmpty(nodeTag))
                return null;

            StatusPerNode ??= new Dictionary<string, PeriodicBackupStatusPerNode>();
            if (StatusPerNode.TryGetValue(nodeTag, out var statusPerNode))
                return statusPerNode;

            // backward compatibility
            lastNodeTag = NodeTag;
            return new PeriodicBackupStatusPerNode
            {
#pragma warning disable CS0612 // Type or member is obsolete
                LastFullBackup = LastFullBackup,
                LastEtag = LastEtag,
                LocalBackup = LocalBackup,
                IsFull = IsFull,
                LastRaftIndex = LastRaftIndex,
                LastDatabaseChangeVector = LastDatabaseChangeVector,
                LastFullBackupInternal = LastFullBackupInternal,
                LastIncrementalBackup = LastIncrementalBackup,
                LastIncrementalBackupInternal = LastIncrementalBackupInternal,
                DurationInMs = DurationInMs,
                UploadToS3 = UploadToS3,
                UploadToGlacier = UploadToGlacier,
                Error = Error,
                FolderName = FolderName,
                LastOperationId = LastOperationId,
                LocalRetentionDurationInMs = LocalRetentionDurationInMs,
                UploadToAzure = UploadToAzure,
                UploadToFtp = UploadToFtp,
                UploadToGoogleCloud = UploadToGoogleCloud
#pragma warning restore CS0612 // Type or member is obsolete
            };
        }

        internal void SetLastInternalBackupTime(string nodeTag, DateTime startTimeInUtc, bool isFullBackup)
        {
            var statusPerNode = EnsureStatusPerNode(nodeTag);

            if (isFullBackup)
            {
                statusPerNode.LastFullBackupInternal = startTimeInUtc;

                // backward compatibility
#pragma warning disable CS0612 // Type or member is obsolete
                LastFullBackupInternal = startTimeInUtc;
#pragma warning restore CS0612 // Type or member is obsolete
            }
            else
            {
                statusPerNode.LastIncrementalBackupInternal = startTimeInUtc;

                // backward compatibility
#pragma warning disable CS0612 // Type or member is obsolete
                LastIncrementalBackupInternal = startTimeInUtc;
#pragma warning restore CS0612 // Type or member is obsolete
            }
        }

        internal void EnsureLocalBackup(string nodeTag)
        {
            var statusPerNode = EnsureStatusPerNode(nodeTag);
            statusPerNode.LocalBackup ??= new LocalBackup();

            // backward compatibility
#pragma warning disable CS0612 // Type or member is obsolete
            LocalBackup ??= new LocalBackup();
#pragma warning restore CS0612 // Type or member is obsolete
        }

        internal void EnsureLastRaftIndex(string nodeTag)
        {
            var statusPerNode = EnsureStatusPerNode(nodeTag);
            statusPerNode.LastRaftIndex ??= new LastRaftIndex();

            // backward compatibility
#pragma warning disable CS0612 // Type or member is obsolete
            LastRaftIndex ??= new LastRaftIndex();
#pragma warning restore CS0612 // Type or member is obsolete
        }

        internal void SetBackupType(string nodeTag, bool isFullBackup)
        {
            var statusPerNode = EnsureStatusPerNode(nodeTag);
            statusPerNode.IsFull = isFullBackup;

            // backward compatibility
#pragma warning disable CS0612 // Type or member is obsolete
            IsFull = isFullBackup;
#pragma warning restore CS0612 // Type or member is obsolete
        }

        internal void SetLastBackupTime(string nodeTag, DateTime startTimeUtc, bool isFullBackup)
        {
            var statusPerNode = EnsureStatusPerNode(nodeTag);

            // backward compatibility
            if (isFullBackup)
            {
                statusPerNode.LastFullBackup = startTimeUtc;

                // backward compatibility
#pragma warning disable CS0612 // Type or member is obsolete
                LastFullBackup = startTimeUtc;
#pragma warning restore CS0612 // Type or member is obsolete
            }
            else
            {
                statusPerNode.LastIncrementalBackup = startTimeUtc;

                // backward compatibility
#pragma warning disable CS0612 // Type or member is obsolete
                LastIncrementalBackup = startTimeUtc;
#pragma warning restore CS0612 // Type or member is obsolete
            }
        }

        internal void SetLocalBackupBackupDirectory(string nodeTag, string path)
        {
            var statusPerNode = EnsureStatusPerNode(nodeTag);
            statusPerNode.LocalBackup.BackupDirectory = path;

            // backward compatibility
#pragma warning disable CS0612 // Type or member is obsolete
            LocalBackup.BackupDirectory = path;
#pragma warning restore CS0612 // Type or member is obsolete
        }

        internal void SetLocalBackupTempFolderUsed(string nodeTag, bool isUsed)
        {
            var statusPerNode = EnsureStatusPerNode(nodeTag);
            statusPerNode.LocalBackup.TempFolderUsed = isUsed;

            // backward compatibility
#pragma warning disable CS0612 // Type or member is obsolete
            LocalBackup.TempFolderUsed = isUsed;
#pragma warning restore CS0612 // Type or member is obsolete
        }

        internal void SetLastEtag(string nodeTag, long lastEtag)
        {
            var statusPerNode = EnsureStatusPerNode(nodeTag);
            statusPerNode.LastEtag = lastEtag;

            // backward compatibility
#pragma warning disable CS0612 // Type or member is obsolete
            LastEtag = lastEtag;
#pragma warning restore CS0612 // Type or member is obsolete
        }

        internal void SetLastDatabaseChangeVector(string nodeTag, string lastDatabaseChangeVector)
        {
            var statusPerNode = EnsureStatusPerNode(nodeTag);
            statusPerNode.LastDatabaseChangeVector = lastDatabaseChangeVector;

            // backward compatibility
#pragma warning disable CS0612 // Type or member is obsolete
            LastDatabaseChangeVector = lastDatabaseChangeVector;
#pragma warning restore CS0612 // Type or member is obsolete
        }

        internal void SetLastRaftIndexLastEtag(string nodeTag, long lastRaftIndex)
        {
            var statusPerNode = EnsureStatusPerNode(nodeTag);
            statusPerNode.LastRaftIndex.LastEtag = lastRaftIndex;

            // backward compatibility
#pragma warning disable CS0612 // Type or member is obsolete
            LastRaftIndex.LastEtag = lastRaftIndex;
#pragma warning restore CS0612 // Type or member is obsolete
        }

        internal void SetFolderName(string nodeTag, string folderName)
        {
            var statusPerNode = EnsureStatusPerNode(nodeTag);
            statusPerNode.FolderName = folderName;

            // backward compatibility
#pragma warning disable CS0612 // Type or member is obsolete
            FolderName = folderName;
#pragma warning restore CS0612 // Type or member is obsolete
        }

        internal void SetLastOperationId(string nodeTag, long operationId)
        {
            var statusPerNode = EnsureStatusPerNode(nodeTag);
            statusPerNode.LastOperationId = operationId;

            // backward compatibility
#pragma warning disable CS0612 // Type or member is obsolete
            LastOperationId = operationId;
#pragma warning restore CS0612 // Type or member is obsolete
        }

        internal void SetError(string nodeTag, Error error)
        {
            var statusPerNode = EnsureStatusPerNode(nodeTag);
            statusPerNode.Error = error;

            // backward compatibility
#pragma warning disable CS0612 // Type or member is obsolete
            Error = error;
#pragma warning restore CS0612 // Type or member is obsolete
        }

        internal void SetDurationInMs(string nodeTag, long durationInMs)
        {
            var statusPerNode = EnsureStatusPerNode(nodeTag);
            statusPerNode.DurationInMs = durationInMs;

            // backward compatibility
#pragma warning disable CS0612 // Type or member is obsolete
            DurationInMs = durationInMs;
#pragma warning restore CS0612 // Type or member is obsolete
        }

        internal void SetUploadToS3(string nodeTag, UploadToS3 result)
        {
            var statusPerNode = EnsureStatusPerNode(nodeTag);
            statusPerNode.UploadToS3 = result;

            // backward compatibility
#pragma warning disable CS0612 // Type or member is obsolete
            UploadToS3 = result;
#pragma warning restore CS0612 // Type or member is obsolete
        }

        internal void SetUploadToAzure(string nodeTag, UploadToAzure result)
        {
            var statusPerNode = EnsureStatusPerNode(nodeTag);
            statusPerNode.UploadToAzure = result;

            // backward compatibility
#pragma warning disable CS0612 // Type or member is obsolete
            UploadToAzure = result;
#pragma warning restore CS0612 // Type or member is obsolete
        }

        internal void SetUploadToGoogleCloud(string nodeTag, UploadToGoogleCloud result)
        {
            var statusPerNode = EnsureStatusPerNode(nodeTag);
            statusPerNode.UploadToGoogleCloud = result;

            // backward compatibility
#pragma warning disable CS0612 // Type or member is obsolete
            UploadToGoogleCloud = result;
#pragma warning restore CS0612 // Type or member is obsolete
        }

        internal void SetUploadToGlacier(string nodeTag, UploadToGlacier result)
        {
            var statusPerNode = EnsureStatusPerNode(nodeTag);
            statusPerNode.UploadToGlacier = result;

            // backward compatibility
#pragma warning disable CS0612 // Type or member is obsolete
            UploadToGlacier = result;
#pragma warning restore CS0612 // Type or member is obsolete
        }

        internal void SetUploadToFtp(string nodeTag, UploadToFtp result)
        {
            var statusPerNode = EnsureStatusPerNode(nodeTag);
            statusPerNode.UploadToFtp = result;

            // backward compatibility
#pragma warning disable CS0612 // Type or member is obsolete
            UploadToFtp = result;
#pragma warning restore CS0612 // Type or member is obsolete
        }

        internal void SetLocalRetentionDurationInMs(string nodeTag, long durationInMs)
        {
            var statusPerNode = EnsureStatusPerNode(nodeTag);
            statusPerNode.LocalRetentionDurationInMs = durationInMs;

            // backward compatibility
#pragma warning disable CS0612 // Type or member is obsolete
            LocalRetentionDurationInMs = durationInMs;
#pragma warning restore CS0612 // Type or member is obsolete
        }

        internal void SetLocalBackupException(string nodeTag, string exception)
        {
            var statusPerNode = EnsureStatusPerNode(nodeTag);
            statusPerNode.LocalBackup.Exception = exception;

#pragma warning disable CS0612 // Type or member is obsolete
            LocalBackup.Exception = exception;
#pragma warning restore CS0612 // Type or member is obsolete
        }

        internal IDisposable UpdateLocalBackupStats(string nodeTag, bool isFullBackup)
        {
            var statusPerNode = EnsureStatusPerNode(nodeTag);
            var disposable = statusPerNode.LocalBackup.UpdateStats(isFullBackup);

            return new DisposableAction(() =>
            {
                disposable.Dispose();

                if (isFullBackup)
                {
#pragma warning disable CS0612 // Type or member is obsolete
                    LocalBackup.LastFullBackup = statusPerNode.LocalBackup.LastFullBackup;
                    LocalBackup.FullBackupDurationInMs = statusPerNode.LocalBackup.FullBackupDurationInMs;
#pragma warning restore CS0612 // Type or member is obsolete
                }
                else
                {
#pragma warning disable CS0612 // Type or member is obsolete
                    LocalBackup.LastIncrementalBackup = statusPerNode.LocalBackup.LastIncrementalBackup;
                    LocalBackup.IncrementalBackupDurationInMs = statusPerNode.LocalBackup.IncrementalBackupDurationInMs;
#pragma warning restore CS0612 // Type or member is obsolete
                }
            });
        }
    }

    internal interface IReadOnlyError
    {
        string Exception { get; }
        DateTime At { get; }
    }

    public sealed class Error : IReadOnlyError
    {
        public string Exception { get; set; }

        public DateTime At { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Exception)] = Exception,
                [nameof(At)] = At
            };
        }
    }
}
