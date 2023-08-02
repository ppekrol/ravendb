using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    internal sealed class MapResult
    {
        public BlittableJsonReaderObject Data;

        public ulong ReduceKeyHash;

        public long Id;
    }
}