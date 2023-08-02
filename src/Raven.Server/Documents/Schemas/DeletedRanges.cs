﻿using Raven.Server.Documents.TimeSeries;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Schemas
{
    internal static class DeletedRanges
    {

        internal static readonly TableSchema DeleteRangesSchemaBase = new TableSchema();
        internal static readonly TableSchema ShardingDeleteRangesSchemaBase = new TableSchema();

        internal static readonly Slice PendingDeletionSegments;
        internal static readonly Slice DeletedRangesKey;
        internal static readonly Slice AllDeletedRangesEtagSlice;
        internal static readonly Slice CollectionDeletedRangesEtagsSlice;
        internal static readonly Slice DeletedRangesBucketAndEtagSlice;

        internal enum DeletedRangeTable
        {
            // lower document id, record separator, lower time series name, record separator, change vector hash
            RangeKey = 0,

            Etag = 1,
            ChangeVector = 2,
            Collection = 3,
            TransactionMarker = 4,
            From = 5,
            To = 6
        }

        static DeletedRanges()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "PendingDeletionSegments", ByteStringType.Immutable, out PendingDeletionSegments);
                Slice.From(ctx, "DeletedRangesKey", ByteStringType.Immutable, out DeletedRangesKey);
                Slice.From(ctx, "AllDeletedRangesEtag", ByteStringType.Immutable, out AllDeletedRangesEtagSlice);
                Slice.From(ctx, "CollectionDeletedRangesEtags", ByteStringType.Immutable, out CollectionDeletedRangesEtagsSlice);
                Slice.From(ctx, "DeletedRangesBucketAndEtag", ByteStringType.Immutable, out DeletedRangesBucketAndEtagSlice);
            }

            DefineIndexesForDeletedRangesSchema(DeleteRangesSchemaBase);
            DefineIndexesForShardingDeletedRangesSchemaBase();

            void DefineIndexesForDeletedRangesSchema(TableSchema schema)
            {
                schema.DefineKey(new TableSchema.IndexDef
                {
                    StartIndex = (int)DeletedRangeTable.RangeKey,
                    Count = 1,
                    Name = DeletedRangesKey,
                    IsGlobal = true
                });

                schema.DefineFixedSizeIndex(new TableSchema.FixedSizeKeyIndexDef
                {
                    StartIndex = (int)DeletedRangeTable.Etag,
                    Name = AllDeletedRangesEtagSlice,
                    IsGlobal = true
                });

                schema.DefineFixedSizeIndex(new TableSchema.FixedSizeKeyIndexDef
                {
                    StartIndex = (int)DeletedRangeTable.Etag,
                    Name = CollectionDeletedRangesEtagsSlice
                });

            }

            void DefineIndexesForShardingDeletedRangesSchemaBase()
            {
                DefineIndexesForDeletedRangesSchema(ShardingDeleteRangesSchemaBase);

                ShardingDeleteRangesSchemaBase.DefineIndex(new TableSchema.DynamicKeyIndexDef
                {
                    GenerateKey = TimeSeriesStorage.GenerateBucketAndEtagIndexKeyForDeletedRanges,
                    OnEntryChanged = TimeSeriesStorage.UpdateBucketStatsForDeletedRanges,
                    IsGlobal = true,
                    Name = DeletedRangesBucketAndEtagSlice
                });
            }
        }
    }
}
