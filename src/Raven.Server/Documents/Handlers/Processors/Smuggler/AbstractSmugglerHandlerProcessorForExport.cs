﻿using System;
using System.Globalization;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Util;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Smuggler
{
    internal abstract class AbstractSmugglerHandlerProcessorForExport<TRequestHandler, TOperationContext> : AbstractSmugglerHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
        where TOperationContext : JsonOperationContext
    {
        protected AbstractSmugglerHandlerProcessorForExport([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract long GetNextOperationId();

        protected abstract ValueTask<IOperationResult> ExportAsync(JsonOperationContext context, IDisposable returnToContextPool, long operationId,
            DatabaseSmugglerOptionsServerSide options, long startDocumentEtag,
            long startRaftIndex, OperationCancelToken token);

        public override async ValueTask ExecuteAsync()
        {
            var result = new SmugglerResult();

            var returnContextToPool = ContextPool.AllocateOperationContext(out JsonOperationContext context);

            var operationId = RequestHandler.GetLongQueryString("operationId", required: false) ?? GetNextOperationId();

            try
            {
                var startDocumentEtag = RequestHandler.GetLongQueryString("startEtag", false) ?? 0;
                var startRaftIndex = RequestHandler.GetLongQueryString("startRaftIndex", false) ?? 0;
                var stream = RequestHandler.TryGetRequestFromStream("DownloadOptions") ?? RequestHandler.RequestBodyStream();
                DatabaseSmugglerOptionsServerSide options;
                using (context.GetMemoryBuffer(out var buffer))
                {
                    var firstRead = await stream.ReadAsync(buffer.Memory.Memory);
                    buffer.Used = 0;
                    buffer.Valid = firstRead;
                    if (firstRead != 0)
                    {
                        var blittableJson = await context.ParseToMemoryAsync(stream, "DownloadOptions", BlittableJsonDocumentBuilder.UsageMode.None, buffer);
                        options = JsonDeserializationServer.DatabaseSmugglerOptions(blittableJson);
                    }
                    else
                    {
                        // no content, we'll use defaults
                        options = new DatabaseSmugglerOptionsServerSide();
                    }
                }

                if (string.IsNullOrWhiteSpace(options.EncryptionKey) == false)
                    ServerStore.LicenseManager.AssertCanCreateEncryptedDatabase();

                var feature = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;

                if (feature == null)
                    options.AuthorizationStatus = AuthorizationStatus.DatabaseAdmin;
                else
                    options.AuthorizationStatus = feature.CanAccess(RequestHandler.DatabaseName, requireAdmin: true, requireWrite: false)
                        ? AuthorizationStatus.DatabaseAdmin
                        : AuthorizationStatus.ValidUser;

                var fileName = options.FileName;
                if (string.IsNullOrEmpty(fileName))
                {
                    fileName = $"Dump of {RequestHandler.DatabaseName} {SystemTime.UtcNow.ToString("yyyy-MM-dd HH-mm", CultureInfo.InvariantCulture)}";
                }

                var contentDisposition = "attachment; filename=" + Uri.EscapeDataString(fileName) + ".ravendbdump";
                HttpContext.Response.Headers[Constants.Headers.ContentDisposition] = contentDisposition;
                HttpContext.Response.Headers[Constants.Headers.ContentType] = "application/octet-stream";
                ApplyBackwardCompatibility(options);

                var token = RequestHandler.CreateHttpRequestBoundOperationToken();

                await ExportAsync(context, returnContextToPool, operationId, options, startDocumentEtag, startRaftIndex, token);
            }
            catch (Exception e)
            {
                if (Logger.IsErrorEnabled)
                    Logger.Error(e, "Export failed.");

                result.AddError($"Error occurred during export. Exception: {e.Message}");
                await WriteSmugglerResultAsync(context, result, RequestHandler.ResponseBodyStream());

                HttpContext.Abort();
            }

            RequestHandler.LogTaskToAudit(Operations.OperationType.DatabaseExport.ToString(), operationId, configuration: null);
        }
    }
}
