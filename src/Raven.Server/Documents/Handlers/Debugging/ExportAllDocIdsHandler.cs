﻿using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Extensions;

namespace Raven.Server.Documents.Handlers.Debugging
{
    internal sealed class AllDocumentIdsDebugHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/documents/export-all-ids", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task ExportAllDocIds()
        {
            var fileName = $"ids-for-{Uri.EscapeDataString(Database.Name)}-{Database.Time.GetUtcNow().GetDefaultRavenFormat(isUtc: true)}.txt";
            HttpContext.Response.Headers[Constants.Headers.ContentDisposition] = $"attachment; filename={fileName}";

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new StreamWriter(ResponseBodyStream(), Encoding.UTF8, 4096))
            using (context.OpenReadTransaction())
            {
                foreach (var id in context.DocumentDatabase.DocumentsStorage.GetAllIds(context))
                    await writer.WriteAsync($"{id}{Environment.NewLine}");

                await writer.FlushAsync();
            }
        }
    }
}
