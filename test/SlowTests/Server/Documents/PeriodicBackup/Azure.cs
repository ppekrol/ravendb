﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.Azure;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.PeriodicBackup
{
    public class Azure : CloudBackupTestBase
    {
        public Azure(ITestOutputHelper output) : base(output)
        {
        }

        [AzureFact]
        public void list_blobs()
        {
            using (var holder = new AzureClientHolder(AzureFactAttribute.AzureSettings))
            {
                var blobNames = GenerateBlobNames(holder.Settings, 2, out var prefix);
                foreach (var blob in blobNames)
                {
                    holder.Client.PutBlob(blob, new MemoryStream(Encoding.UTF8.GetBytes("abc")), new Dictionary<string, string>());
                }

                var blobsCount = holder.Client.ListBlobs(prefix, delimiter: null, listFolders: false).List.Count();
                Assert.Equal(blobsCount, 2);
            }
        }

        [AzureFact]
        public void CanRemoveBlobsInBatch()
        {
            using (var holder = new AzureClientHolder(AzureFactAttribute.AzureSettings))
            {
                var blobs = new List<string>();
                string prefix = holder.Client.RemoteFolderName;

                for (int i = 0; i < 10; i++)
                {
                    var key = $"{prefix}/northwind_{i}.ravendump";
                    var tmpArr = new byte[3];
                    new Random().NextBytes(tmpArr);
                    holder.Client.PutBlob(key, new MemoryStream(tmpArr), new Dictionary<string, string> { { $"property_{i}", $"value_{i}" } });
                    blobs.Add(key);
                }

                Assert.Equal(blobs.Count, holder.Client.ListBlobs(prefix, delimiter: null, listFolders: false).List.Count());

                holder.Client.DeleteBlobs(blobs);

                var listBlobs = holder.Client.ListBlobs(prefix, null, listFolders: false);
                var blobNames = listBlobs.List.Select(b => b.Name).ToList();
                Assert.Equal(0, blobNames.Count);
            }
        }

        [AzureFact]
        public async Task CanRemoveBlobsWithNonExistingBlobsInBatch()
        {
            using (var holder = new AzureClientHolder(AzureFactAttribute.AzureSettings))
            {
                var blobs = new List<string>();
                // put blob
                var k = GenerateBlobNames(holder.Settings, 1, out var prefix).First();
                var tmpArr = new byte[3];
                new Random().NextBytes(tmpArr);
                holder.Client.PutBlob(k, new MemoryStream(tmpArr), new Dictionary<string, string> { { "Nice", "NotNice" } });

                var blob = await holder.Client.GetBlobAsync(k);
                Assert.NotNull(blob);
                blobs.Add(k);

                for (int i = 0; i < 10; i++)
                {
                    blobs.Add($"{prefix}/northwind_{i}.ravendump");
                }

                holder.Client.DeleteBlobs(blobs);

                var listBlobs = holder.Client.ListBlobs(prefix, null, listFolders: false);
                var blobNames = listBlobs.List.Select(b => b.Name).ToList();
                Assert.Equal(0, blobNames.Count);
            }
        }

        [AzureFact]
        public async Task put_blob()
        {
            using (var holder = new AzureClientHolder(AzureFactAttribute.AzureSettings))
            {
                var blobKey = GenerateBlobNames(holder.Settings, 1, out _).First();

                holder.Client.PutBlob(blobKey, new MemoryStream(Encoding.UTF8.GetBytes("123")), new Dictionary<string, string>
                    {
                        {"property1", "value1"},
                        {"property2", "value2"}
                    });
                var blob = await holder.Client.GetBlobAsync(blobKey);
                Assert.NotNull(blob);

                using (var reader = new StreamReader(blob.Data))
                    Assert.Equal("123", reader.ReadToEnd());

                var property1 = blob.Metadata.Keys.Single(x => x.Contains("property1"));
                var property2 = blob.Metadata.Keys.Single(x => x.Contains("property2"));

                Assert.Equal("value1", blob.Metadata[property1]);
                Assert.Equal("value2", blob.Metadata[property2]);
            }
        }

        [AzureFact]
        public async Task put_blob_in_folder()
        {
            using (var holder = new AzureClientHolder(AzureFactAttribute.AzureSettings))
            {
                var blobNames = GenerateBlobNames(holder.Settings, 1, out _);

                holder.Client.PutBlob(blobNames[0], new MemoryStream(Encoding.UTF8.GetBytes("123")),
                        new Dictionary<string, string> { { "property1", "value1" }, { "property2", "value2" } });

                var blob = await holder.Client.GetBlobAsync(blobNames[0]);
                Assert.NotNull(blob);

                using (var reader = new StreamReader(blob.Data))
                    Assert.Equal("123", reader.ReadToEnd());

                var property1 = blob.Metadata.Keys.Single(x => x.Contains("property1"));
                var property2 = blob.Metadata.Keys.Single(x => x.Contains("property2"));

                Assert.Equal("value1", blob.Metadata[property1]);
                Assert.Equal("value2", blob.Metadata[property2]);
            }
        }

        [AzureFact]
        public Task put_blob_without_sas_token()
        {
            return PutBlobsAsync(5, useSasToken: false);
        }

        [AzureSasTokenFact]
        public Task put_blob_with_sas_token()
        {
            return PutBlobsAsync(5, useSasToken: true);
        }

        private static async Task PutBlobsAsync(int blobsCount, bool useSasToken)
        {
            using (var holder = new AzureClientHolder(useSasToken == false ? AzureFactAttribute.AzureSettings : AzureSasTokenFactAttribute.AzureSettings))
            {
                var blobNames = GenerateBlobNames(holder.Settings, blobsCount, out var prefix);
                for (var i = 0; i < blobsCount; i++)
                {
                    holder.Client.PutBlob(blobNames[i], new MemoryStream(Encoding.UTF8.GetBytes("123")),
                        new Dictionary<string, string> { { "property1", "value1" }, { "property2", "value2" } });
                }

                for (var i = 0; i < blobsCount; i++)
                {
                    var blob = await holder.Client.GetBlobAsync(blobNames[i]);
                    Assert.NotNull(blob);

                    using (var reader = new StreamReader(blob.Data))
                        Assert.Equal("123", reader.ReadToEnd());

                    var property1 = blob.Metadata.Keys.Single(x => x.Contains("property1"));
                    var property2 = blob.Metadata.Keys.Single(x => x.Contains("property2"));

                    Assert.Equal("value1", blob.Metadata[property1]);
                    Assert.Equal("value2", blob.Metadata[property2]);
                }

                var listBlobs = holder.Client.ListBlobs(prefix, null, listFolders: false);
                Assert.Equal(blobsCount, listBlobs.List.Count());
            }
        }

        internal class AzureClientHolder : IDisposable
        {
            public IRavenAzureClient Client { get; set; }
            public AzureSettings Settings { get; set; }
            private readonly string _remoteFolder;

            public AzureClientHolder(AzureSettings setting, Progress progress = null, [CallerMemberName] string caller = null)
            {
                Assert.False(string.IsNullOrEmpty(setting.StorageContainer), "string.IsNullOrEmpty(setting.StorageContainer)");
                Settings = setting;

                // keep only alphanumeric characters
                Settings.RemoteFolderName = _remoteFolder = GetRemoteFolder(caller);
                Client = RavenAzureClient.Create(Settings, DefaultConfiguration, progress);
            }

            public void Dispose()
            {
                const int numberOfObjectsInBatch = 256;
                var blobsToDelete = new List<string>();
                string continuationToken = null;
                var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5));

                try
                {
                    do
                    {
                        var blobs = Client.ListBlobs(_remoteFolder, delimiter: null, listFolders: false, continuationToken: continuationToken);

                        foreach (var blob in blobs.List)
                        {
                            if (blobsToDelete.Count == numberOfObjectsInBatch)
                            {
                                Client.DeleteBlobs(blobsToDelete);
                                blobsToDelete.Clear();
                            }

                            blobsToDelete.Add(blob.Name);
                        }

                        continuationToken = blobs.ContinuationToken;

                        cts.Token.ThrowIfCancellationRequested();

                    } while (continuationToken != null);

                    if (blobsToDelete.Count > 0)
                        Client.DeleteBlobs(blobsToDelete);

                    Assert.Empty(Client.ListBlobs(_remoteFolder, delimiter: null, listFolders: false).List);
                }
                catch
                {
                    // ignored
                }

                Client.Dispose();
                cts.Dispose();
            }
        }
    }
}
