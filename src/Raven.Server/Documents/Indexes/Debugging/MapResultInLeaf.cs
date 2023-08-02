using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Debugging
{
    internal sealed class MapResultInLeaf
    {
        public BlittableJsonReaderObject Data;

        public string Source;
    }
}