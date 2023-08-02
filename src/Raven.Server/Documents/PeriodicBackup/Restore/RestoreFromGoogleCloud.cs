﻿using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.GoogleCloud;
using Raven.Server.ServerWide;
using Raven.Server.Utils;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    internal sealed class RestoreFromGoogleCloud : IRestoreSource
    {
        private readonly ServerStore _serverStore;
        private readonly RavenGoogleCloudClient _client;
        private readonly string _remoteFolderName;

        public RestoreFromGoogleCloud([NotNull] ServerStore serverStore, RestoreFromGoogleCloudConfiguration restoreFromConfiguration)
        {
            _serverStore = serverStore ?? throw new ArgumentNullException(nameof(serverStore));
            _client = new RavenGoogleCloudClient(restoreFromConfiguration.Settings, serverStore.Configuration.Backup);
            _remoteFolderName = restoreFromConfiguration.Settings.RemoteFolderName;
        }

        public Task<Stream> GetStream(string path)
        {
            return Task.FromResult(_client.DownloadObject(path));
        }

        public async Task<ZipArchive> GetZipArchiveForSnapshot(string path)
        {
            Stream stream = _client.DownloadObject(path);
            var file = await RestoreUtils.CopyRemoteStreamLocallyAsync(stream, _serverStore.Configuration);
            return new DeleteOnCloseZipArchive(file, ZipArchiveMode.Read);
        }

        public async Task<List<string>> GetFilesForRestore()
        {
            var prefix = string.IsNullOrEmpty(_remoteFolderName) ? "" : _remoteFolderName.TrimEnd('/');
            var allObjects = await _client.ListObjectsAsync(prefix, delimiter: null);
            var result = new List<string>();
            foreach (var obj in allObjects)
            {
                result.Add(obj.Name);
            }

            return result;
        }

        public string GetBackupPath(string fileName)
        {
            return fileName;
        }

        public string GetSmugglerBackupPath(string smugglerFile)
        {
            return smugglerFile;
        }

        public string GetBackupLocation()
        {
            return _remoteFolderName;
        }

        public void Dispose()
        {
            _client?.Dispose();
        }
    }
}
