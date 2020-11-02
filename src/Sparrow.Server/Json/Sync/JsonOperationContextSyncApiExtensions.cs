using System;
using System.IO;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Sparrow.Server.Json.Sync
{
    public static class JsonOperationContextSyncExtensions
    {
        internal static JsonOperationContext.Sync Sync(this JsonOperationContext context)
        {
            return new JsonOperationContext.Sync(context); // TODO arek - avoid creating new instance on every call
        }

        internal static void Write(this JsonOperationContext.Sync syncContext, Stream stream, BlittableJsonReaderObject json)
        {
            syncContext.EnsureNotDisposed();

            using (var writer = new BlittableJsonTextWriter(syncContext.Context, stream))
            {
                writer.WriteObject(json);
            }
        }

        internal static void Write(this JsonOperationContext.Sync syncContext, BlittableJsonTextWriter writer, DynamicJsonValue json)
        {
            syncContext.EnsureNotDisposed();

            WriteInternal(syncContext, writer, json);
        }


        private static void WriteInternal(JsonOperationContext.Sync syncContext, BlittableJsonTextWriter writer, object json)
        {
            syncContext.JsonParserState.Reset();
            syncContext.ObjectJsonParser.Reset(json);

            syncContext.ObjectJsonParser.Read();

            WriteObject(syncContext, writer, syncContext.JsonParserState, syncContext.ObjectJsonParser);

            syncContext.ObjectJsonParser.Reset(null);
        }

        private static void WriteObject(JsonOperationContext.Sync syncContext, BlittableJsonTextWriter writer, JsonParserState state, ObjectJsonParser parser)
        {
            syncContext.EnsureNotDisposed();

            if (state.CurrentTokenType != JsonParserToken.StartObject)
                throw new InvalidOperationException("StartObject expected, but got " + state.CurrentTokenType);

            writer.WriteStartObject();
            bool first = true;
            while (true)
            {
                if (parser.Read() == false)
                    throw new InvalidOperationException("Object json parser can't return partial results");
                if (state.CurrentTokenType == JsonParserToken.EndObject)
                    break;

                if (state.CurrentTokenType != JsonParserToken.String)
                    throw new InvalidOperationException("Property expected, but got " + state.CurrentTokenType);

                if (first == false)
                    writer.WriteComma();
                first = false;

                var lazyStringValue = syncContext.Context.AllocateStringValue(null, state.StringBuffer, state.StringSize);
                writer.WritePropertyName(lazyStringValue);

                if (parser.Read() == false)
                    throw new InvalidOperationException("Object json parser can't return partial results");

                await WriteValueAsync(writer, state, parser).ConfigureAwait(false);
            }

            writer.WriteEndObject();
        }
    }
}
