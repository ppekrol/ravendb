﻿using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands.MultiGet
{
    public class MultiGetCommand : RavenCommand<List<GetResponse>>
    {
        private readonly HttpCache _cache;
        private readonly List<GetRequest> _commands;

        private string _baseUrl;

        public MultiGetCommand(HttpCache cache, List<GetRequest> commands)
        {
            _cache = cache;
            _commands = commands;
            ResponseType = RavenCommandResponseType.Raw;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            _baseUrl = $"{node.Url}/databases/{node.Database}";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(stream =>
                {
                    using (var writer = new BlittableJsonTextWriter(ctx, stream))
                    {
                        writer.WriteStartObject();

                        var first = true;
                        writer.WritePropertyName("Requests");
                        writer.WriteStartArray();
                        foreach (var command in _commands)
                        {
                            if (first == false)
                                writer.WriteComma();

                            first = false;
                            var cacheKey = GetCacheKey(command, out string _);
                            using (_cache.Get(ctx, cacheKey, out string cachedChangeVector, out var _))
                            {
                                var headers = new Dictionary<string, string>();
                                if (cachedChangeVector != null)
                                    headers["If-None-Match"] = $"\"{cachedChangeVector}\"";

                                foreach (var header in command.Headers)
                                    headers[header.Key] = header.Value;

                                writer.WriteStartObject();

                                writer.WritePropertyName(nameof(GetRequest.Url));
                                writer.WriteString($"/databases/{node.Database}{command.Url}");
                                writer.WriteComma();

                                writer.WritePropertyName(nameof(GetRequest.Query));
                                writer.WriteString(command.Query);
                                writer.WriteComma();

                                writer.WritePropertyName(nameof(GetRequest.Method));
                                writer.WriteString(command.Method?.Method);
                                writer.WriteComma();

                                writer.WritePropertyName(nameof(GetRequest.Headers));
                                writer.WriteStartObject();

                                var firstInner = true;
                                foreach (var kvp in headers)
                                {
                                    if (firstInner == false)
                                        writer.WriteComma();

                                    firstInner = false;
                                    writer.WritePropertyName(kvp.Key);
                                    writer.WriteString(kvp.Value);
                                }
                                writer.WriteEndObject();
                                writer.WriteComma();

                                writer.WritePropertyName(nameof(GetRequest.Content));
                                if (command.Content != null)
                                    command.Content.WriteContent(writer, ctx);
                                else
                                    writer.WriteNull();

                                writer.WriteEndObject();
                            }
                        }
                        writer.WriteEndArray();

                        writer.WriteEndObject();
                    }
                })
            };

            url = $"{_baseUrl}/multi_get";

            return request;
        }

        private string GetCacheKey(GetRequest command, out string requestUrl)
        {
            requestUrl = $"{_baseUrl}{command.UrlAndQuery}";

            return $"{command.Method}-{requestUrl}";
        }

        public override void SetResponseRaw(HttpResponseMessage response, Stream stream, JsonOperationContext context)
        {
            var state = new JsonParserState();
            using (var parser = new UnmanagedJsonParser(context, state, "multi_get/response"))
            using (context.GetMemoryBuffer(out MemoryBuffer buffer))
            using (var peepingTomStream = new PeepingTomStream(stream, context))
            {
                if (UnmanagedJsonParserHelper.Read(peepingTomStream, parser, state, buffer) == false)
                    ThrowInvalidJsonResponse(peepingTomStream);

                if (state.CurrentTokenType != JsonParserToken.StartObject)
                    ThrowInvalidJsonResponse(peepingTomStream);

                var property = UnmanagedJsonParserHelper.ReadString(context, peepingTomStream, parser, state, buffer);
                if (property != nameof(BlittableArrayResult.Results))
                    ThrowInvalidJsonResponse(peepingTomStream);

                var i = 0;
                Result = new List<GetResponse>();
                foreach (var getResponse in ReadResponses(context, peepingTomStream, parser, state, buffer))
                {
                    var command = _commands[i];

                    MaybeSetCache(getResponse, command);
                    MaybeReadFromCache(getResponse, command, context);

                    Result.Add(getResponse);

                    i++;
                }

                if (UnmanagedJsonParserHelper.Read(peepingTomStream, parser, state, buffer) == false)
                    ThrowInvalidJsonResponse(peepingTomStream);

                if (state.CurrentTokenType != JsonParserToken.EndObject)
                    ThrowInvalidJsonResponse(peepingTomStream);
            }
        }

        private static IEnumerable<GetResponse> ReadResponses(JsonOperationContext context, PeepingTomStream peepingTomStream, UnmanagedJsonParser parser, JsonParserState state, MemoryBuffer buffer)
        {
            if (UnmanagedJsonParserHelper.Read(peepingTomStream, parser, state, buffer) == false)
                ThrowInvalidJsonResponse(peepingTomStream);

            if (state.CurrentTokenType != JsonParserToken.StartArray)
                ThrowInvalidJsonResponse(peepingTomStream);

            while (true)
            {
                if (UnmanagedJsonParserHelper.Read(peepingTomStream, parser, state, buffer) == false)
                    ThrowInvalidJsonResponse(peepingTomStream);

                if (state.CurrentTokenType == JsonParserToken.EndArray)
                    break;

                yield return ReadResponse(context, peepingTomStream, parser, state, buffer);
            }
        }

        private static unsafe GetResponse ReadResponse(JsonOperationContext context, PeepingTomStream peepingTomStream, UnmanagedJsonParser parser, JsonParserState state, MemoryBuffer buffer)
        {
            if (state.CurrentTokenType != JsonParserToken.StartObject)
                ThrowInvalidJsonResponse(peepingTomStream);

            var getResponse = new GetResponse();
            while (true)
            {
                if (UnmanagedJsonParserHelper.Read(peepingTomStream, parser, state, buffer) == false)
                    ThrowInvalidJsonResponse(peepingTomStream);

                if (state.CurrentTokenType == JsonParserToken.EndObject)
                    break;

                if (state.CurrentTokenType != JsonParserToken.String)
                    ThrowInvalidJsonResponse(peepingTomStream);

                var property = context.AllocateStringValue(null, state.StringBuffer, state.StringSize).ToString();
                switch (property)
                {
                    case nameof(GetResponse.Result):
                        if (UnmanagedJsonParserHelper.Read(peepingTomStream, parser, state, buffer) == false)
                            ThrowInvalidJsonResponse(peepingTomStream);

                        if (state.CurrentTokenType == JsonParserToken.Null)
                            continue;

                        if (state.CurrentTokenType != JsonParserToken.StartObject)
                            ThrowInvalidJsonResponse(peepingTomStream);

                        using (var builder = new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None, "multi_get/result", parser, state))
                        {
                            UnmanagedJsonParserHelper.ReadObject(builder, peepingTomStream, parser, buffer);
                            getResponse.Result = builder.CreateReader();
                        }
                        continue;
                    case nameof(GetResponse.Headers):
                        if (UnmanagedJsonParserHelper.Read(peepingTomStream, parser, state, buffer) == false)
                            ThrowInvalidJsonResponse(peepingTomStream);

                        if (state.CurrentTokenType == JsonParserToken.Null)
                            continue;

                        if (state.CurrentTokenType != JsonParserToken.StartObject)
                            ThrowInvalidJsonResponse(peepingTomStream);

                        using (var builder = new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None, "multi_get/result", parser, state))
                        {
                            UnmanagedJsonParserHelper.ReadObject(builder, peepingTomStream, parser, buffer);
                            using (var headersJson = builder.CreateReader())
                            {
                                foreach (var propertyName in headersJson.GetPropertyNames())
                                    getResponse.Headers[propertyName] = headersJson[propertyName].ToString();
                            }
                        }
                        continue;
                    case nameof(GetResponse.StatusCode):
                        if (UnmanagedJsonParserHelper.Read(peepingTomStream, parser, state, buffer) == false)
                            ThrowInvalidJsonResponse(peepingTomStream);

                        if (state.CurrentTokenType != JsonParserToken.Integer)
                            ThrowInvalidJsonResponse(peepingTomStream);

                        getResponse.StatusCode = (HttpStatusCode)state.Long;
                        continue;
                    default:
                        ThrowInvalidJsonResponse(peepingTomStream);
                        break;
                }
            }

            return getResponse;
        }

        private void MaybeReadFromCache(GetResponse getResponse, GetRequest command, JsonOperationContext context)
        {
            if (getResponse.StatusCode != HttpStatusCode.NotModified)
                return;

            var cacheKey = GetCacheKey(command, out string _);
            using (_cache.Get(context, cacheKey, out string _, out BlittableJsonReaderObject cachedResponse))
            {
                getResponse.Result = cachedResponse;
            }
        }

        private void MaybeSetCache(GetResponse getResponse, GetRequest command)
        {
            if (getResponse.StatusCode == HttpStatusCode.NotModified)
                return;

            var cacheKey = GetCacheKey(command, out string _);

            var result = getResponse.Result as BlittableJsonReaderObject;
            if (result == null)
                return;

            var changeVector = getResponse.Headers.GetEtagHeader();
            if (changeVector == null)
                return;

            _cache.Set(cacheKey, changeVector, result);
        }

        public override bool IsReadRequest => false;
    }
}
