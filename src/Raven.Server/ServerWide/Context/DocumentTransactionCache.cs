using System;
using System.Collections.Generic;

namespace Raven.Server.ServerWide.Context
{
    internal sealed class DocumentTransactionCache
    {
        public long LastDocumentEtag;
        public long LastTombstoneEtag;
        public long LastCounterEtag;
        public long LastTimeSeriesEtag;
        public long LastConflictEtag;
        public long LastRevisionsEtag;
        public long LastAttachmentsEtag;

        public long LastEtag;

        internal sealed class CollectionCache
        {
            public long LastDocumentEtag;
            public long LastTombstoneEtag;
            public string LastChangeVector;
        }

        public readonly Dictionary<string, CollectionCache> LastEtagsByCollection = new Dictionary<string, CollectionCache>(StringComparer.OrdinalIgnoreCase);
    }
}
