﻿using System;
using System.Threading.Tasks;
using Raven.Server.Background;
using Raven.Server.ServerWide.Commands.Sharding;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;

namespace Raven.Server.Documents.Sharding.Background
{
    public class ShardedPeriodicDocumentsMigrator : BackgroundWorkBase
    {
        private readonly ShardedDocumentDatabase _database;

        public ShardedPeriodicDocumentsMigrator(ShardedDocumentDatabase database) : base(database.ShardedDatabaseName, database.ShardedDocumentsStorage.DocumentDatabase.DatabaseShutdown)
        {
            _database = database;
        }

        protected override async Task DoWork()
        {
            await WaitOrThrowOperationCanceled(_database.Configuration.Sharding.PeriodicDocumentsMigrationInterval.AsTimeSpan);

            while (_database.ServerStore.Sharding.HasActiveMigrations(_database.ShardedDatabaseName))
                await WaitOrThrowOperationCanceled(TimeSpan.FromMilliseconds(300));

            await ExecuteMoveDocuments();
        }

        internal async Task ExecuteMoveDocuments()
        {
            try
            {
                int bucket = -1;
                int moveToShard = -1;
                bool found = false;
                using (_database.ShardedDocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var configuration = _database.ShardingConfiguration;
                    for (var index = 0; index < configuration.BucketRanges.Count; index++)
                    {
                        var range = configuration.BucketRanges[index];
                        if (range.ShardNumber == _database.ShardNumber)
                            continue;

                        var start = range.BucketRangeStart;
                        var end = index == configuration.BucketRanges.Count - 1
                            ? int.MaxValue
                            : configuration.BucketRanges[index + 1].BucketRangeStart;

                        var bucketStatistics = ShardedDocumentsStorage.GetBucketStatistics(context, start, end);

                        if (bucketStatistics == null)
                            continue;

                        foreach (var bucketStats in bucketStatistics)
                        {
                            if (bucketStats.NumberOfDocuments == 0)
                                continue;

                            bucket = bucketStats.Bucket;
                            moveToShard = range.ShardNumber;
                            found = true;
                            break;
                        }

                        if (found)
                        {
                            await MoveDocumentsToShard(bucket, moveToShard);
                            return;
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // this will stop processing
                throw;
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations($"Failed to execute documents migration for '{_database.Name}'", e);

                throw;
            }
        }

        private async Task MoveDocumentsToShard(int bucket, int moveToShard)
        {
            var cmd = new StartBucketMigrationCommand(bucket, _database.ShardNumber, moveToShard, _database.ShardedDatabaseName,
                $"{Guid.NewGuid()}/{bucket}", backgroundMigration: true);

            var result = await _database.ServerStore.SendToLeaderAsync(cmd);
            await _database.ServerStore.Cluster.WaitForIndexNotification(result.Index);
        }
    }
}
