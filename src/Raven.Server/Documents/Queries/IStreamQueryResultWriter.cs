using System;
using System.Threading.Tasks;

namespace Raven.Server.Documents.Queries
{
    public interface IStreamQueryResultWriter<in T> : IAsyncDisposable
    {
        ValueTask StartResponse();
        ValueTask StartResults();
        ValueTask EndResults();
        ValueTask AddResult(T res);
        ValueTask EndResponse();
        ValueTask WriteError(Exception e);
        ValueTask WriteError(string error);
        ValueTask WriteQueryStatistics(long resultEtag, bool isStale, string indexName, long totalResults, DateTime timestamp);
        bool SupportStatistics { get; }
    }
}
