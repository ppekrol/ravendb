using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Server.ServerWide.Context;
using SlowTests.Rolling;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Rachis.DatabaseCluster
{
    public class ClusterDatabaseMaintenanceSlow : ReplicationTestBase
    {
        public ClusterDatabaseMaintenanceSlow(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Name { get; set; }
            public string Email { get; set; }
            public int Age { get; set; }
        }

        private class UsersByName : AbstractIndexCreationTask<User>
        {
            public UsersByName()
            {
                Map = usersCollection => from user in usersCollection
                                         select new { user.Name };
                Index(x => x.Name, FieldIndexing.Search);
            }
        }

        [RavenFact(RavenTestCategory.ClusterTransactions)]
        public async Task DontPurgeTombstonesWhenNodeIsDown()
        {
            var clusterSize = 3;
            var (_, leader) = await CreateRaftCluster(clusterSize, leaderIndex: 0);
            using (var store = GetDocumentStore(new Options
            {
                CreateDatabase = true,
                ReplicationFactor = clusterSize,
                Server = leader,
                DeleteDatabaseOnDispose = false // we bring one node down, so we can't delete the entire database, but we run in mem, so we don't care
            }))
            {
                var index = new UsersByName();
                await index.ExecuteAsync(store);
                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(TimeSpan.FromSeconds(30), replicas: 2);
                    await session.StoreAsync(new User
                    {
                        Name = "Karmel"
                    }, "users/1");
                    await session.SaveChangesAsync();
                }

                // we need to deploy the index before bringing the node down
                await RollingIndexesClusterTests.WaitForRollingIndex(store.Database, index.IndexName, Servers);
                await DisposeServerAndWaitForFinishOfDisposalAsync(Servers[1]);
                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(TimeSpan.FromSeconds(30), replicas: 1);
                    session.Delete("users/1");
                    await session.SaveChangesAsync();
                }

                Indexes.WaitForIndexing(store);

                var database = await leader.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                await database.TombstoneCleaner.ExecuteCleanup();
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    Assert.Equal(2, database.DocumentsStorage.GetLastTombstoneEtag(ctx.Transaction.InnerTransaction, "Users"));
                }
            }
        }
    }
}
