﻿// -----------------------------------------------------------------------
//  <copyright file="DocumentsCompressionHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Net;
using System.Threading.Tasks;
using Raven.Client.ServerWide;
using Raven.Server.Config.Categories;
using Raven.Server.Exceptions;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class DocumentsCompressionHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/documents-compression/config", "GET", AuthorizationStatus.ValidUser)]
        public Task GetDocumentsCompressionConfig()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                DocumentsCompressionConfiguration compressionConfig;
                using (var recordRaw = Server.ServerStore.Cluster.ReadRawDatabaseRecord(context, Database.Name))
                {
                    compressionConfig = recordRaw?.DocumentsCompressionConfiguration;
                }

                if (compressionConfig != null)
                {
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.WriteAsync(writer, compressionConfig.ToJson());
                    }
                }
                else
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                }
            }
            return Task.CompletedTask;
        }
        
        [RavenAction("/databases/*/admin/documents-compression/config", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task ConfigDocumentsCompression()
        {
            if (Server.Configuration.Core.FeaturesAvailability != FeaturesAvailability.Experimental)
                FeaturesAvailabilityException.Throw("Documents Compression");

            await DatabaseConfigurations(ServerStore.ModifyDocumentsCompression, "write-compression-config", GetRaftRequestIdFromQuery());
        }
    }
}
