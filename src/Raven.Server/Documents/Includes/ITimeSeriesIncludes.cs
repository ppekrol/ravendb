using System.Threading;
using System.Threading.Tasks;
using Sparrow.Json;

namespace Raven.Server.Documents.Includes;

internal interface ITimeSeriesIncludes
{
    ValueTask<int> WriteIncludesAsync(AsyncBlittableJsonTextWriter writer, JsonOperationContext context, CancellationToken token);

    int Count { get; }

    long GetEntriesCountForStats();
}
