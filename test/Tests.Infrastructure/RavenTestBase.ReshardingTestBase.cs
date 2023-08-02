﻿using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Sharding;
using Raven.Server;
using Raven.Server.Utils;
using Sparrow.Json;
using Xunit;

namespace FastTests;

public partial class RavenTestBase
{
    public class ReshardingTestBase
    {
        private readonly RavenTestBase _parent;

        public ReshardingTestBase(RavenTestBase parent)
        {
            _parent = parent;
        }

        internal async Task<int> StartMovingShardForId(IDocumentStore store, string id, int? toShard = null, List<RavenServer> servers = null)
        {
            servers ??= _parent.GetServers();

            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            var bucket = _parent.Sharding.GetBucket(record.Sharding, id);
            PrefixedShardingSetting prefixed = null;
            foreach (var setting in record.Sharding.Prefixed)
            {
                if (id.StartsWith(setting.Prefix, StringComparison.OrdinalIgnoreCase))
                {
                    prefixed = setting;
                    break;
                }
            }

            var shardNumber = ShardHelper.GetShardNumberFor(record.Sharding, bucket);
            var moveToShard = toShard ?? (prefixed != null 
                ? ShardingTestBase.GetNextSortedShardNumber(prefixed, shardNumber)
                : ShardingTestBase.GetNextSortedShardNumber(record.Sharding.Shards, shardNumber));

            using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, shardNumber)))
            {
                Assert.True(await session.Advanced.ExistsAsync(id), "The document doesn't exists on the source");
            }

            foreach (var server in servers)
            {
                try
                {
                    await server.ServerStore.Sharding.StartBucketMigration(store.Database, bucket, moveToShard);
                    break;
                }
                catch
                {
                    //
                }
            }
                
            var exists = _parent.WaitForDocument<dynamic>(store, id, predicate: null, database: ShardHelper.ToShardName(store.Database, moveToShard), timeout: 30_000);
            Assert.True(exists, $"{id} wasn't found at shard {moveToShard}");

            return bucket;
        }

        public async Task WaitForMigrationComplete(IDocumentStore store, int bucket)
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30)))
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database), cts.Token);
                while (record.Sharding.BucketMigrations.ContainsKey(bucket))
                {
                    await Task.Delay(250, cts.Token);
                    record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database), cts.Token);
                }
            }
        }

        internal async Task MoveShardForId(IDocumentStore store, string id, int? toShard = null, List<RavenServer> servers = null)
        {
            try
            {
                servers ??= _parent.GetServers();
                var bucket = await StartMovingShardForId(store, id, toShard, servers);
                await WaitForMigrationComplete(store, bucket);
            }
            catch (Exception e)
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                using (var ctx = JsonOperationContext.ShortTermSingleUse())
                {
                    var sharding = store.Conventions.Serialization.DefaultConverter.ToBlittable(record.Sharding, ctx).ToString();
                    throw new InvalidOperationException(
                        $"Failed to completed the migration for {id}{Environment.NewLine}{sharding}{Environment.NewLine}{_parent.Cluster.CollectLogsFromNodes(servers ?? new List<RavenServer> { _parent.Server })}",
                        e);
                }
            }
        }
    }
}
