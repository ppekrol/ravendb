﻿using System;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Exceptions;
using Raven.Client.Util;
using Raven.Server.Utils;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal abstract class AbstractPullReplicationHandlerProcessorForGenerateCertificate<TRequestHandler> : AbstractHandlerProcessor<TRequestHandler>
        where TRequestHandler : RequestHandler
    {
        protected AbstractPullReplicationHandlerProcessorForGenerateCertificate([NotNull] TRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected abstract void AssertCanExecute();

        public override async ValueTask ExecuteAsync()
        {
            AssertCanExecute();

            var validMonths = RequestHandler.GetIntValueQueryString("validMonths", required: false);
            var validYears = RequestHandler.GetIntValueQueryString("validYears", required: false);

            if (validMonths.HasValue && validYears.HasValue)
            {
                throw new BadRequestException("Please provide validation period in either months or years. Not both.");
            }

            var notAfter = DateTime.UtcNow.AddMonths(3);
            if (validMonths.HasValue && validMonths.Value > 0)
            {
                notAfter = DateTime.UtcNow.AddMonths(validMonths.Value);
            }
            else if (validYears.HasValue && validYears.Value > 0)
            {
                notAfter = DateTime.UtcNow.AddYears(validYears.Value);
            }

            var log = new StringBuilder();
            var commonNameValue = "PullReplicationAutogeneratedCertificate";
            CertificateUtils.CreateCertificateAuthorityCertificate(commonNameValue + " CA", out var ca, out var caSubjectName, log);
            CertificateUtils.CreateSelfSignedCertificateBasedOnPrivateKey(commonNameValue, caSubjectName, ca, false, false, notAfter, out var certBytes, log: log);
            var certificateWithPrivateKey = CertificateLoaderUtil.CreateCertificate(certBytes, null, CertificateLoaderUtil.FlagsForExport);
            certificateWithPrivateKey.Verify();

            var keyPairInfo = new PullReplicationHandler.PullReplicationCertificate
            {
                PublicKey = Convert.ToBase64String(certificateWithPrivateKey.Export(X509ContentType.Cert)),
                Thumbprint = certificateWithPrivateKey.Thumbprint,
                Certificate = Convert.ToBase64String(certificateWithPrivateKey.Export(X509ContentType.Pfx))
            };

            using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName(nameof(keyPairInfo.PublicKey));
                writer.WriteString(keyPairInfo.PublicKey);
                writer.WriteComma();

                writer.WritePropertyName(nameof(keyPairInfo.Certificate));
                writer.WriteString(keyPairInfo.Certificate);
                writer.WriteComma();

                writer.WritePropertyName(nameof(keyPairInfo.Thumbprint));
                writer.WriteString(keyPairInfo.Thumbprint);

                writer.WriteEndObject();
            }
        }
    }
}
