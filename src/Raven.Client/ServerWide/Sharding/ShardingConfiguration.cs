﻿using System.Collections.Generic;

namespace Raven.Client.ServerWide.Sharding;

public class ShardingConfiguration
{
    public DatabaseTopology[] Shards;

    public List<ShardBucketRange> ShardBucketRanges = new List<ShardBucketRange>();

    public Dictionary<int, ShardBucketMigration> ShardBucketMigrations;

    // change vectors with a MOVE element below this will be considered as permanent
    // pointers with the migration index below this one will be purged
    public long MigrationCutOffIndex;

    // the dbid part with the MOVE tag upon migration
    public string ShardedDatabaseId;
}
