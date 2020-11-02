using System;
using System.IO;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Sparrow.Server.Json.Sync
{
    internal static class JsonOperationContextSyncExtensions
    {
        internal static void Write(this JsonOperationContext.SyncJsonOperationContext syncContext, Stream stream, BlittableJsonReaderObject json)
        {
            syncContext.EnsureNotDisposed();

            using (var writer = new BlittableJsonTextWriter(syncContext.Context, stream))
            {
                writer.WriteObject(json);
            }
        }

        internal static void Write(this JsonOperationContext.SyncJsonOperationContext syncContext, BlittableJsonTextWriter writer, DynamicJsonValue json)
        {
            syncContext.EnsureNotDisposed();

            WriteInternal(syncContext, writer, json);
        }

        internal static void Write(this JsonOperationContext.SyncJsonOperationContext syncContext, BlittableJsonTextWriter writer, BlittableJsonReaderObject json)
        {
            syncContext.EnsureNotDisposed();

            WriteInternal(syncContext, writer, json);
        }

        private static void WriteInternal(JsonOperationContext.SyncJsonOperationContext syncContext, BlittableJsonTextWriter writer, object json)
        {
            syncContext.JsonParserState.Reset();
            syncContext.ObjectJsonParser.Reset(json);

            syncContext.ObjectJsonParser.Read();

            WriteObject(syncContext, writer, syncContext.JsonParserState, syncContext.ObjectJsonParser);

            syncContext.ObjectJsonParser.Reset(null);
        }

        private static void WriteObject(JsonOperationContext.SyncJsonOperationContext syncContext, BlittableJsonTextWriter writer, JsonParserState state, ObjectJsonParser parser)
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

                WriteValue(syncContext, writer, state, parser);
            }

            writer.WriteEndObject();
        }

        private static void WriteValue(JsonOperationContext.SyncJsonOperationContext syncContext, BlittableJsonTextWriter writer, JsonParserState state, ObjectJsonParser parser)
        {
            switch (state.CurrentTokenType)
            {
                case JsonParserToken.Null:
                    writer.WriteNull();
                    break;

                case JsonParserToken.False:
                    writer.WriteBool(false);
                    break;

                case JsonParserToken.True:
                    writer.WriteBool(true);
                    break;

                case JsonParserToken.String:
                    if (state.CompressedSize.HasValue)
                    {
                        var lazyCompressedStringValue = new LazyCompressedStringValue(null, state.StringBuffer, state.StringSize, state.CompressedSize.Value, syncContext.Context);
                        writer.WriteString(lazyCompressedStringValue);
                    }
                    else
                    {
                        writer.WriteString(syncContext.Context.AllocateStringValue(null, state.StringBuffer, state.StringSize));
                    }
                    break;

                case JsonParserToken.Float:
                    writer.WriteDouble(new LazyNumberValue(syncContext.Context.AllocateStringValue(null, state.StringBuffer, state.StringSize)));
                    break;

                case JsonParserToken.Integer:
                    writer.WriteInteger(state.Long);
                    break;

                case JsonParserToken.StartObject:
                    WriteObject(syncContext, writer, state, parser);
                    break;

                case JsonParserToken.StartArray:
                    WriteArray(syncContext, writer, state, parser);
                    break;

                default:
                    throw new ArgumentOutOfRangeException("Could not understand " + state.CurrentTokenType);
            }
        }

        private static void WriteArray(JsonOperationContext.SyncJsonOperationContext syncContext, BlittableJsonTextWriter writer, JsonParserState state, ObjectJsonParser parser)
        {
            syncContext.EnsureNotDisposed();

            if (state.CurrentTokenType != JsonParserToken.StartArray)
                throw new InvalidOperationException("StartArray expected, but got " + state.CurrentTokenType);

            writer.WriteStartArray();
            bool first = true;
            while (true)
            {
                if (parser.Read() == false)
                    throw new InvalidOperationException("Object json parser can't return partial results");

                if (state.CurrentTokenType == JsonParserToken.EndArray)
                    break;

                if (first == false)
                    writer.WriteComma();
                first = false;

                WriteValue(syncContext, writer, state, parser);
            }

            writer.WriteEndArray();
        }
    }
}
