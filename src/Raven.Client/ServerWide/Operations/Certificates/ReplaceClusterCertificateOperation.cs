using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Certificates
{
    public class ReplaceClusterCertificateOperation : IServerOperation
    {
        private readonly byte[] _certBytes;
        private readonly bool _replaceImmediately;

        public ReplaceClusterCertificateOperation(byte[] certBytes, bool replaceImmediately)
        {
            _certBytes = certBytes ?? throw new ArgumentNullException(nameof(certBytes));
            _replaceImmediately = replaceImmediately;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new ReplaceClusterCertificateCommand(_certBytes, _replaceImmediately);
        }

        private class ReplaceClusterCertificateCommand : RavenCommand, IRaftCommand
        {
            private readonly byte[] _certBytes;
            private readonly bool _replaceImmediately;

            public ReplaceClusterCertificateCommand(byte[] certBytes, bool replaceImmediately)
            {
                _certBytes = certBytes ?? throw new ArgumentNullException(nameof(certBytes));
                _replaceImmediately = replaceImmediately;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/certificates/replace-cluster-cert?replaceImmediately={_replaceImmediately}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                        {
                            await writer.WriteStartObjectAsync().ConfigureAwait(false);
                            await writer.WritePropertyNameAsync(nameof(CertificateDefinition.Certificate)).ConfigureAwait(false);
                            await writer.WriteStringAsync(Convert.ToBase64String(_certBytes)).ConfigureAwait(false); // keep the private key -> this is a server cert
                            await writer.WriteEndObjectAsync().ConfigureAwait(false);
                        }
                    })
                };

                return request;
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
