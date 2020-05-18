﻿using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Refresh
{
    public class ConfigureRefreshOperation : IMaintenanceOperation<ConfigureRefreshOperationResult>
    {
        private readonly RefreshConfiguration _configuration;

        public ConfigureRefreshOperation(RefreshConfiguration configuration)
        {
            _configuration = configuration;
        }

        public RavenCommand<ConfigureRefreshOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new ConfigureRefreshCommand(_configuration);
        }

        private class ConfigureRefreshCommand : RavenCommand<ConfigureRefreshOperationResult>, IRaftCommand
        {
            private readonly RefreshConfiguration _configuration;

            public ConfigureRefreshCommand(RefreshConfiguration configuration)
            {
                _configuration = configuration;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/refresh/config";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(stream =>
                    {
                        var config = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_configuration, ctx);
                        ctx.Write(stream, config);
                    })
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.ConfigureRefreshOperationResult(response);
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
