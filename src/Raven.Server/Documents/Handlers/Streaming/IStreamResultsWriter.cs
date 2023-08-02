using System;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Server.Documents.Handlers.Streaming;

internal interface IStreamResultsWriter<in T> : IAsyncDisposable
{
    void StartResponse();
    
    void StartResults();
    
    void EndResults();
    
    ValueTask AddResultAsync(T res, CancellationToken token);
    
    void EndResponse();
}
