using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Commercial;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_26585 : RavenTestBase
    {
        public RavenDB_26585(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Subscriptions | RavenTestCategory.BackupExportImport)]
        public async Task Restore_Backup_With_Subscriptions_Under_Community_License_Should_Succeed()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren" }, "users/1");
                    await session.SaveChangesAsync();
                }

                store.Subscriptions.Create(new SubscriptionCreationOptions<User> { Name = "sub1" });

                var sourceSubs = store.Subscriptions.GetSubscriptions(0, 10);
                Assert.Single(sourceSubs);

                var config = Backup.CreateBackupConfiguration(backupPath);
                await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                var databaseName = $"restored_{Guid.NewGuid()}";
                using (Backup.RestoreDatabase(store,
                    new RestoreBackupConfiguration { BackupLocation = Directory.GetDirectories(backupPath).First(), DatabaseName = databaseName }))
                {
                    using (var restoredStore = GetDocumentStore(new Options { ModifyDatabaseName = _ => databaseName, CreateDatabase = false }))
                    {
                        var restoredSubs = restoredStore.Subscriptions.GetSubscriptions(0, 10, databaseName);
                        Assert.Single(restoredSubs);
                        Assert.Equal("sub1", restoredSubs[0].SubscriptionName);
                    }
                }
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Subscriptions)]
        public async Task Cluster_Subscriptions_Limit_Is_Still_Enforced_On_Community_License()
        {
            DoNotReuseServer();

            // Set community license on the empty cluster (no databases yet, so the change is unconditional).
            await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_COMM);

            // Community license: per-database = 3, per-cluster = 15.
            // Fill 5 databases with 3 subscriptions each (cluster total = 15, at the limit).
            var stores = new List<IDocumentStore>();
            try
            {
                for (var dbIndex = 0; dbIndex < 5; dbIndex++)
                {
                    var store = GetDocumentStore();
                    stores.Add(store);
                    for (var subIndex = 0; subIndex < 3; subIndex++)
                        await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions<User> { Name = $"sub-{dbIndex}-{subIndex}" });
                }

                // A subscription on a sixth database would push the cluster total to 16 — must be rejected.
                var sixth = GetDocumentStore();
                stores.Add(sixth);

                var ex = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    await sixth.Subscriptions.CreateAsync(new SubscriptionCreationOptions<User> { Name = "sub-overflow" }));
                Assert.Equal(LimitType.Subscriptions, ex.LimitType);
            }
            finally
            {
                foreach (var store in stores)
                    store.Dispose();
            }
        }
    }
}
