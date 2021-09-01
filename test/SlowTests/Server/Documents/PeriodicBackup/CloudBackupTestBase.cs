﻿using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.ServerWide;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.PeriodicBackup
{
    public abstract class CloudBackupTestBase : RavenTestBase
    {
        protected CloudBackupTestBase(ITestOutputHelper output) : base(output)
        {
        }

        protected static readonly BackupConfiguration DefaultConfiguration = RavenConfiguration.CreateForTesting("foo", ResourceType.Database).Backup;

        public static List<string> GenerateBlobNames(ICloudBackupSettings setting, int blobsCount, out string prefix)
        {
            var blobNames = new List<string>();

            prefix = Guid.NewGuid().ToString();
            if (string.IsNullOrEmpty(setting.RemoteFolderName) == false)
                prefix = $"{setting.RemoteFolderName}/{prefix}";

            for (var i = 0; i < blobsCount; i++)
            {
                var name = $"{prefix}/{i}";

                blobNames.Add(name);
            }

            return blobNames;
        }

        public static string GetRemoteFolder(string caller)
        {
            // keep only alphanumeric characters
            var str = caller == null ? string.Empty : string.Concat(caller.Where(char.IsLetterOrDigit));
            return $"{str}{Guid.NewGuid()}";
        }
    }
}
