using System;
using System.Diagnostics;
using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Backups
{
    internal interface IReadOnlyBackupStatus
    {
        DateTime? LastFullBackup { get; }
        DateTime? LastIncrementalBackup { get; }
        long? FullBackupDurationInMs { get; }
        long? IncrementalBackupDurationInMs { get; }
        string Exception { get; }
    }

    public abstract class BackupStatus : IReadOnlyBackupStatus
    {
        public DateTime? LastFullBackup { get; set; }

        public DateTime? LastIncrementalBackup { get; set; }

        public long? FullBackupDurationInMs { get; set; }

        public long? IncrementalBackupDurationInMs { get; set; }

        public string Exception { get; set; }

        public IDisposable UpdateStats(bool isFullBackup)
        {
            var now = SystemTime.UtcNow;
            var sw = Stopwatch.StartNew();

            return new DisposableAction(() =>
            {
                if (isFullBackup)
                {
                    LastFullBackup = now;
                    FullBackupDurationInMs = sw.ElapsedMilliseconds;
                }
                else
                {
                    LastIncrementalBackup = now;
                    IncrementalBackupDurationInMs = sw.ElapsedMilliseconds;
                }
            });
        }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(LastFullBackup)] = LastFullBackup,
                [nameof(LastIncrementalBackup)] = LastIncrementalBackup,
                [nameof(FullBackupDurationInMs)] = FullBackupDurationInMs,
                [nameof(IncrementalBackupDurationInMs)] = IncrementalBackupDurationInMs,
                [nameof(Exception)] = Exception
            };
        }
    }

    internal interface IReadOnlyLastRaftIndex
    {
        long? LastEtag { get; }
    }

    public sealed class LastRaftIndex : IReadOnlyLastRaftIndex
    {
        public long? LastEtag { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(LastEtag)] = LastEtag
            };
        }
    }

    internal interface IReadOnlyLocalBackup : IReadOnlyBackupStatus
    {
        string BackupDirectory { get; }
        string FileName { get; }
        bool TempFolderUsed { get; }
    }

    public sealed class LocalBackup : BackupStatus, IReadOnlyLocalBackup
    {
        public string BackupDirectory { get; set; }
        public string FileName { get; set; }

        public bool TempFolderUsed { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(BackupDirectory)] = BackupDirectory;
            json[nameof(FileName)] = FileName;
            json[nameof(TempFolderUsed)] = TempFolderUsed;
            return json;
        }
    }

    internal interface IReadOnlyCloudUploadStatus : IReadOnlyBackupStatus
    {
        bool Skipped { get; }
        IReadOnlyUploadProgress UploadProgress { get; }
    }

    public abstract class CloudUploadStatus : BackupStatus, IReadOnlyCloudUploadStatus
    {
        protected CloudUploadStatus()
        {
            UploadProgress = new UploadProgress();
        }

        public bool Skipped { get; set; }

        public UploadProgress UploadProgress { get; set; }

        IReadOnlyUploadProgress IReadOnlyCloudUploadStatus.UploadProgress => UploadProgress;

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(Skipped)] = Skipped;
            json[nameof(UploadProgress)] = UploadProgress.ToJson();
            return json;
        }
    }

    internal interface IReadOnlyUploadToS3 : IReadOnlyCloudUploadStatus
    {
    }

    public sealed class UploadToS3 : CloudUploadStatus, IReadOnlyUploadToS3
    {

    }

    internal interface IReadOnlyUploadToGlacier : IReadOnlyCloudUploadStatus
    {
    }

    public sealed class UploadToGlacier : CloudUploadStatus, IReadOnlyUploadToGlacier
    {

    }

    internal interface IReadOnlyUploadToAzure : IReadOnlyCloudUploadStatus
    {
    }

    public sealed class UploadToAzure : CloudUploadStatus, IReadOnlyUploadToAzure
    {

    }

    internal interface IReadOnlyUploadToGoogleCloud : IReadOnlyCloudUploadStatus
    {
    }

    public sealed class UploadToGoogleCloud : CloudUploadStatus, IReadOnlyUploadToGoogleCloud
    {

    }

    internal interface IReadOnlyUploadToFtp : IReadOnlyCloudUploadStatus
    {
    }

    public sealed class UploadToFtp : CloudUploadStatus, IReadOnlyUploadToFtp
    {

    }

    internal interface IReadOnlyUploadProgress
    {
        UploadType UploadType { get; }
        UploadState UploadState { get; }
        long UploadedInBytes { get; }
        long TotalInBytes { get; }
        double BytesPutsPerSec { get; }
        long UploadTimeInMs { get; }
    }

    public sealed class UploadProgress : IReadOnlyUploadProgress
    {
        public UploadProgress()
        {
            UploadType = UploadType.Regular;
            _sw = Stopwatch.StartNew();
        }

        private readonly Stopwatch _sw;

        public UploadType UploadType { get; set; }

        public UploadState UploadState { get; private set; }

        public long UploadedInBytes { get; set; }

        public long TotalInBytes { get; set; }

        public double BytesPutsPerSec { get; set; }

        public long UploadTimeInMs => _sw.ElapsedMilliseconds;

        public void ChangeState(UploadState newState)
        {
            UploadState = newState;
            switch (newState)
            {
                case UploadState.PendingUpload:
                    _sw.Restart();
                    break;
                case UploadState.Done:
                    _sw.Stop();
                    break;
            }
        }

        public void SetTotal(long totalLength)
        {
            TotalInBytes = totalLength;
        }

        public void UpdateUploaded(long length)
        {
            UploadedInBytes += length;
        }

        public void SetUploaded(long length)
        {
            UploadedInBytes = length;
        }

        public void ChangeType(UploadType newType)
        {
            UploadType = newType;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                [nameof(UploadType)] = UploadType,
                [nameof(UploadState)] = UploadState,
                [nameof(UploadedInBytes)] = UploadedInBytes,
                [nameof(TotalInBytes)] = TotalInBytes,
                [nameof(BytesPutsPerSec)] = BytesPutsPerSec,
                [nameof(UploadTimeInMs)] = UploadTimeInMs
            };
        }
    }

    public enum UploadState
    {
        PendingUpload,
        Uploading,
        PendingResponse,
        Done
    }

    public enum UploadType
    {
        Regular,
        Chunked
    }
}
