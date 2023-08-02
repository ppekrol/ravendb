﻿using System;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    interal class PutCertificateCommand : PutValueCommand<CertificateDefinition>
    {
        public string PublicKeyPinningHash;

        public PutCertificateCommand()
        {
            // for deserialization
        }

        public PutCertificateCommand(string name, CertificateDefinition value, string uniqueRequestId) : base(uniqueRequestId)
        {
            Name = name;
            Value = value;
            PublicKeyPinningHash = value.PublicKeyPinningHash;

            ValidateCertificateDefinition(value);
        }

        public static void ValidateCertificateDefinition(CertificateDefinition certificateDefinition)
        {
            if (string.IsNullOrEmpty(certificateDefinition.Certificate))
                throw new InvalidOperationException("Cannot store a certificate definition without the actual certificate!");
            if (string.IsNullOrEmpty(certificateDefinition.Thumbprint))
                throw new InvalidOperationException("Cannot store a certificate without a thumbprint.");
            if (string.IsNullOrEmpty(certificateDefinition.PublicKeyPinningHash))
                throw new InvalidOperationException("Cannot store a certificate without a PublicKeyPinningHash.");
            if (string.IsNullOrEmpty(certificateDefinition.Name))
                throw new InvalidOperationException("Cannot store a certificate without a name.");
            if (certificateDefinition.NotAfter == null)
                throw new InvalidOperationException("Cannot store a certificate without an expiration date.");
        }

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var djv = base.ToJson(context);
            djv[nameof(Name)] = Name;
            djv[nameof(Value)] = ValueToJson();
            djv[nameof(PublicKeyPinningHash)] = PublicKeyPinningHash;
            return djv;
        }

        public override DynamicJsonValue ValueToJson()
        {
            return Value?.ToJson();
        }

        public override void VerifyCanExecuteCommand(ServerStore store, TransactionOperationContext context, bool isClusterAdmin)
        {
            AssertClusterAdmin(isClusterAdmin);
        }
    }
}
