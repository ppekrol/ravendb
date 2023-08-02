using System.Threading;

namespace Raven.Server.Documents.Indexes
{
    internal sealed class IndexIdentities
    {
        private int _indexingStatsId;

        public int GetNextIndexingStatsId()
        {
            return Interlocked.Increment(ref _indexingStatsId);
        }
    }
}