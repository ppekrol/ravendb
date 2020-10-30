using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Certificates
{
    public class PutClientCertificateOperation : IServerOperation
    {
        private readonly X509Certificate2 _certificate;
        private readonly Dictionary<string, DatabaseAccess> _permissions;
        private readonly string _name;
        private readonly SecurityClearance _clearance;

        public PutClientCertificateOperation(string name, X509Certificate2 certificate, Dictionary<string, DatabaseAccess> permissions, SecurityClearance clearance)
        {
            _certificate = certificate ?? throw new ArgumentNullException(nameof(certificate));
            _permissions = permissions ?? throw new ArgumentNullException(nameof(permissions));
            _name = name;
            _clearance = clearance;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new PutClientCertificateCommand(_name, _certificate, _permissions, _clearance);
        }

        private class PutClientCertificateCommand : RavenCommand, IRaftCommand
        {
            private readonly X509Certificate2 _certificate;
            private readonly Dictionary<string, DatabaseAccess> _permissions;
            private readonly string _name;
            private readonly SecurityClearance _clearance;

            public PutClientCertificateCommand(string name, X509Certificate2 certificate, Dictionary<string, DatabaseAccess> permissions, SecurityClearance clearance)
            {
                _certificate = certificate ?? throw new ArgumentNullException(nameof(certificate));
                _permissions = permissions ?? throw new ArgumentNullException(nameof(permissions));
                _name = name;
                _clearance = clearance;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/certificates";
                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                        {
                            await writer.WriteStartObjectAsync().ConfigureAwait(false);
                            await writer.WritePropertyNameAsync(nameof(CertificateDefinition.Name)).ConfigureAwait(false);
                            await writer.WriteStringAsync(_name).ConfigureAwait(false);
                            await writer.WriteCommaAsync().ConfigureAwait(false);
                            await writer.WritePropertyNameAsync(nameof(CertificateDefinition.Certificate)).ConfigureAwait(false);
                            await writer.WriteStringAsync(Convert.ToBase64String(_certificate.Export(X509ContentType.Cert))).ConfigureAwait(false);
                            await writer.WriteCommaAsync().ConfigureAwait(false);
                            await writer.WritePropertyNameAsync(nameof(CertificateDefinition.SecurityClearance)).ConfigureAwait(false);
                            await writer.WriteStringAsync(_clearance.ToString()).ConfigureAwait(false);
                            await writer.WriteCommaAsync().ConfigureAwait(false);
                            await writer.WritePropertyNameAsync(nameof(CertificateDefinition.Permissions)).ConfigureAwait(false);
                            await writer.WriteStartObjectAsync().ConfigureAwait(false);
                            bool first = true;
                            foreach (var kvp in _permissions)
                            {
                                if (first == false)
                                    await writer.WriteCommaAsync().ConfigureAwait(false);
                                first = false;

                                await writer.WriteStringAsync(kvp.Key).ConfigureAwait(false);
                                await writer.WriteCommaAsync().ConfigureAwait(false);
                                await writer.WriteStringAsync(kvp.Value.ToString()).ConfigureAwait(false);
                            }

                            await writer.WriteEndObjectAsync().ConfigureAwait(false);
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
