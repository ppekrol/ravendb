using Sparrow.Json.Parsing;

namespace Sparrow.Json
{
    public partial class JsonOperationContext
    {
        internal class Sync
        {
            internal JsonOperationContext Context { get; }

            internal Sync(JsonOperationContext context)
            {
                Context = context;
            }

            internal void EnsureNotDisposed()
            {
                Context.EnsureNotDisposed();
            }

            internal JsonParserState JsonParserState => Context._jsonParserState;

            internal ObjectJsonParser ObjectJsonParser => Context._objectJsonParser;
        }
    }
}
