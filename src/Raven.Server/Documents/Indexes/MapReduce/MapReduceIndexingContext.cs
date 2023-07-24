using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using NLog;
using Raven.Server.Logging;
using Sparrow;
using Sparrow.Logging;
using Sparrow.Server;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Fixed;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public class MapReduceIndexingContext : IDisposable
    {
        private readonly Index _index;
        private Logger _logger;

        private Logger Logger
        {
            get => _logger ??= RavenLogManager.Instance.GetLoggerForIndex<MapReduceIndexingContext>(_index);
        }

        internal static Slice LastMapResultIdKey;

        public FixedSizeTree DocumentMapEntries;

        public Tree MapPhaseTree;
        public Tree ReducePhaseTree;
        
        public FixedSizeTree ResultsStoreTypes;
        public Dictionary<ulong, MapReduceResultsStore> StoreByReduceKeyHash = new Dictionary<ulong, MapReduceResultsStore>(NumericEqualityComparer.BoxedInstanceUInt64);
        public Dictionary<string, long> ProcessedDocEtags = new Dictionary<string, long>();
        public Dictionary<string, long> ProcessedTombstoneEtags = new Dictionary<string, long>();
        public readonly HashSet<long> FreedPages = new HashSet<long>();

        public long NextMapResultId;

        static MapReduceIndexingContext()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "__raven/map-reduce/#next-map-result-id", ByteStringType.Immutable, out LastMapResultIdKey);
            }
        }

        public MapReduceIndexingContext([NotNull] Index index)
        {
            _index = index ?? throw new ArgumentNullException(nameof(index));
        }

        public void Dispose()
        {
            DocumentMapEntries?.Dispose();
            DocumentMapEntries = null;
            MapPhaseTree = null;
            ReducePhaseTree = null;
            ProcessedDocEtags.Clear();
            ProcessedTombstoneEtags.Clear();
            StoreByReduceKeyHash.Clear();
            FreedPages.Clear();
        }

        public unsafe void StoreNextMapResultId()
        {
            if (MapPhaseTree.Llt.Environment.Options.IsCatastrophicFailureSet)
                return; // avoid re-throwing it

            try
            {
                using (MapPhaseTree.DirectAdd(LastMapResultIdKey, sizeof(long), out byte* ptr))
                    *(long*)ptr = NextMapResultId;
            }
            catch (Exception e)
            {
                if (Logger.IsDebugEnabled)
                    Logger.Debug(e, "Failed to store next map result id");

                throw;
            }
        }

        public unsafe void Initialize(Tree mapEntriesTree)
        {
            var read = mapEntriesTree.Read(LastMapResultIdKey);

            if (read == null)
                return;

            NextMapResultId = *(long*)read.Reader.Base;
        }
    }
}
