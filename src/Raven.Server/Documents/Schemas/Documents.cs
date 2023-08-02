﻿using Raven.Server.Documents.Sharding;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Schemas
{
    internal static class Documents
    {
        public static readonly Slice CollectionEtagsSlice;

        internal static readonly Slice DocsSlice;
        internal static readonly Slice AllDocsEtagsSlice;
        internal static readonly Slice AllDocsBucketAndEtagSlice;

        internal static readonly TableSchema ShardingDocsSchemaBase = new TableSchema
        {
            TableType = (byte)TableType.Documents
        };

        internal static readonly TableSchema ShardingCompressedDocsSchemaBase = new TableSchema
        {
            TableType = (byte)TableType.Documents
        };

        internal static readonly TableSchema DocsSchemaBase = new TableSchema
        {
            TableType = (byte)TableType.Documents
        };

        internal static readonly TableSchema CompressedDocsSchemaBase = new TableSchema
        {
            TableType = (byte)TableType.Documents
        };

        public enum DocumentsTable
        {
            LowerId = 0,
            Etag = 1,
            Id = 2, // format of lazy string id is detailed in GetLowerIdSliceAndStorageKey
            Data = 3,
            ChangeVector = 4,
            LastModified = 5,
            Flags = 6,
            TransactionMarker = 7
        }

        static Documents()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "Docs", ByteStringType.Immutable, out DocsSlice);
                Slice.From(ctx, "CollectionEtags", ByteStringType.Immutable, out CollectionEtagsSlice);
                Slice.From(ctx, "AllDocsEtags", ByteStringType.Immutable, out AllDocsEtagsSlice);
                Slice.From(ctx, "AllDocsBucketAndEtag", ByteStringType.Immutable, out AllDocsBucketAndEtagSlice);
            }

            DefineIndexesForDocsSchemaBase(DocsSchemaBase);
            DefineIndexesForDocsSchemaBase(CompressedDocsSchemaBase);

            DocsSchemaBase.CompressValues(DocsSchemaBase.FixedSizeIndexes[CollectionEtagsSlice], compress: false);
            CompressedDocsSchemaBase.CompressValues(CompressedDocsSchemaBase.FixedSizeIndexes[CollectionEtagsSlice], compress: true);

            DefineIndexesForShardingDocsSchemaBase(ShardingDocsSchemaBase);
            DefineIndexesForShardingDocsSchemaBase(ShardingCompressedDocsSchemaBase);

            ShardingDocsSchemaBase.CompressValues(DocsSchemaBase.FixedSizeIndexes[CollectionEtagsSlice], compress: false);
            ShardingCompressedDocsSchemaBase.CompressValues(CompressedDocsSchemaBase.FixedSizeIndexes[CollectionEtagsSlice], compress: true);

            void DefineIndexesForDocsSchemaBase(TableSchema docsSchema)
            {
                docsSchema.DefineKey(new TableSchema.IndexDef
                {
                    StartIndex = (int)DocumentsTable.LowerId, 
                    Count = 1, 
                    IsGlobal = true, 
                    Name = DocsSlice
                });
                docsSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeKeyIndexDef
                {
                    StartIndex = (int)DocumentsTable.Etag, 
                    IsGlobal = false, 
                    Name = CollectionEtagsSlice
                });
                docsSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeKeyIndexDef
                {
                    StartIndex = (int)DocumentsTable.Etag, 
                    IsGlobal = true, 
                    Name = AllDocsEtagsSlice
                });
            }

            void DefineIndexesForShardingDocsSchemaBase(TableSchema docsSchema)
            {
                DefineIndexesForDocsSchemaBase(docsSchema);

                docsSchema.DefineIndex(new TableSchema.DynamicKeyIndexDef
                {
                    GenerateKey = ShardedDocumentsStorage.GenerateBucketAndEtagIndexKeyForDocuments,
                    OnEntryChanged = ShardedDocumentsStorage.UpdateBucketStatsForDocument,
                    IsGlobal = true,
                    Name = AllDocsBucketAndEtagSlice
                });
            }
        }
    }

    public enum TableType : byte
    {
        None = 0,
        Documents = 1,
        Revisions = 2,
        Conflicts = 3,
        LegacyCounter = 4,
        Counters = 5,
        TimeSeries = 6
    }
}
