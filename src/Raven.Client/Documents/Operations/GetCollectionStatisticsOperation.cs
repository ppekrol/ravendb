using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class GetCollectionStatisticsOperation : IMaintenanceOperation<CollectionStatistics>
    {
        public RavenCommand<CollectionStatistics> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetCollectionStatisticsCommand(conventions);
        }

        private class GetCollectionStatisticsCommand : RavenCommand<CollectionStatistics>
        {
            private readonly DocumentConventions _conventions;

            public GetCollectionStatisticsCommand(DocumentConventions conventions)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            }

            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/collections/stats";
                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = _conventions.Serialization.DeserializeEntityFromBlittable<CollectionStatistics>(response);
            }
        }
    }
}
