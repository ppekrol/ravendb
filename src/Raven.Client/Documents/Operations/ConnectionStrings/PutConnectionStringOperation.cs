﻿using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.ConnectionStrings
{
    public class PutConnectionStringOperation<T> : IMaintenanceOperation<PutConnectionStringResult> where T : ConnectionString
    {
        private readonly T _connectionString;

        public PutConnectionStringOperation(T connectionString)
        {
            _connectionString = connectionString;
        }

        public RavenCommand<PutConnectionStringResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new PutConnectionStringCommand(_connectionString);
        }

        private class PutConnectionStringCommand : RavenCommand<PutConnectionStringResult>, IRaftCommand
        {
            private readonly T _connectionString;

            public PutConnectionStringCommand(T connectionString)
            {
                _connectionString = connectionString;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/connection-strings";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(stream =>
                    {
                        var config = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_connectionString, ctx);
                        ctx.Write(stream, config);
                    })
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.PutConnectionStringResult(response);
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }

    public class PutConnectionStringResult
    {
        public long RaftCommandIndex { get; set; }
    }
}
