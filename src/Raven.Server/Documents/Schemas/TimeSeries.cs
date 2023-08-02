﻿using Raven.Server.Documents.TimeSeries;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Schemas
{
    internal static class TimeSeries
    {

        internal static readonly TableSchema TimeSeriesSchemaBase = new TableSchema
        {
            TableType = (byte)TableType.TimeSeries
        };

        internal static readonly TableSchema ShardingTimeSeriesSchemaBase = new TableSchema
        {
            TableType = (byte)TableType.TimeSeries
        };

        internal static readonly Slice AllTimeSeriesEtagSlice;
        internal static readonly Slice CollectionTimeSeriesEtagsSlice;
        internal static readonly Slice TimeSeriesKeysSlice;
        internal static readonly Slice TimeSeriesBucketAndEtagSlice;

        internal enum TimeSeriesTable
        {
            // Format of this is:
            // lower document id, record separator, lower time series name, record separator, segment start
            TimeSeriesKey = 0,

            Etag = 1,
            ChangeVector = 2,
            Segment = 3,
            Collection = 4,
            TransactionMarker = 5
        }

        static TimeSeries()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "AllTimeSeriesEtag", ByteStringType.Immutable, out AllTimeSeriesEtagSlice);
                Slice.From(ctx, "CollectionTimeSeriesEtags", ByteStringType.Immutable, out CollectionTimeSeriesEtagsSlice);
                Slice.From(ctx, "TimeSeriesKeys", ByteStringType.Immutable, out TimeSeriesKeysSlice);
                Slice.From(ctx, "TimeSeriesBucketAndEtag", ByteStringType.Immutable, out TimeSeriesBucketAndEtagSlice);
            }

            DefineIndexesForTimeSeriesSchema(TimeSeriesSchemaBase);
            DefineIndexesForShardingTimeSeriesSchemaBase();

            void DefineIndexesForTimeSeriesSchema(TableSchema schema)
            {
                schema.DefineKey(new TableSchema.IndexDef
                {
                    StartIndex = (int)TimeSeriesTable.TimeSeriesKey,
                    Count = 1,
                    Name = TimeSeriesKeysSlice,
                    IsGlobal = true
                });

                schema.DefineFixedSizeIndex(new TableSchema.FixedSizeKeyIndexDef
                {
                    StartIndex = (int)TimeSeriesTable.Etag,
                    Name = AllTimeSeriesEtagSlice,
                    IsGlobal = true
                });

                schema.DefineFixedSizeIndex(new TableSchema.FixedSizeKeyIndexDef
                {
                    StartIndex = (int)TimeSeriesTable.Etag,
                    Name = CollectionTimeSeriesEtagsSlice
                });
            }

            void DefineIndexesForShardingTimeSeriesSchemaBase()
            {
                DefineIndexesForTimeSeriesSchema(ShardingTimeSeriesSchemaBase);

                ShardingTimeSeriesSchemaBase.DefineIndex(new TableSchema.DynamicKeyIndexDef
                {
                    GenerateKey = TimeSeriesStorage.GenerateBucketAndEtagIndexKeyForTimeSeries,
                    OnEntryChanged = TimeSeriesStorage.UpdateBucketStatsForTimeSeries,
                    IsGlobal = true,
                    Name = TimeSeriesBucketAndEtagSlice
                });
            }
        }
    }
}
