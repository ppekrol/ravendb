using System;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using Raven.Client.ServerWide.Operations.Logs;
using Raven.Server.Json;
using Raven.Server.Logging;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server.Platform.Posix;
using Sparrow.Utils;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class AdminLogsHandler : ServerRequestHandler
    {
        [RavenAction("/admin/logs/configuration", "GET", AuthorizationStatus.Operator)]
        public async Task GetConfiguration()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var logsConfiguration = RavenLogManager.Instance.GetLogsConfiguration(Server);
                var auditLogsConfiguration = RavenLogManager.Instance.GetAuditLogsConfiguration(Server);
                var microsoftLogsConfiguration = RavenLogManager.Instance.GetMicrosoftLogsConfiguration(Server);
                var adminLogsConfiguration = RavenLogManager.Instance.GetAdminLogsConfiguration(Server);

                var djv = new DynamicJsonValue
                {
                    [nameof(GetLogsConfigurationResult.Logs)] = logsConfiguration?.ToJson(),
                    [nameof(GetLogsConfigurationResult.AuditLogs)] = auditLogsConfiguration?.ToJson(),
                    [nameof(GetLogsConfigurationResult.MicrosoftLogs)] = microsoftLogsConfiguration?.ToJson(),
                    [nameof(GetLogsConfigurationResult.AdminLogs)] = adminLogsConfiguration?.ToJson()
                };

                var json = context.ReadObject(djv, "logs/configuration");

                writer.WriteObject(json);
            }
        }

        [RavenAction("/admin/logs/configuration", "POST", AuthorizationStatus.Operator)]
        public async Task SetConfiguration()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), "logs/configuration");

                var configuration = JsonDeserializationServer.Parameters.SetLogsConfigurationParameters(json);

                RavenLogManager.Instance.ConfigureLogging(configuration);
            }

            NoContentStatus();
        }

        [RavenAction("/admin/logs/watch", "GET", AuthorizationStatus.Operator)]
        public async Task RegisterForLogs()
        {
            using (var socket = await HttpContext.WebSockets.AcceptWebSocketAsync())
                await AdminLogsTarget.RegisterAsync(socket, ServerStore.ServerShutdown);
        }

        [RavenAction("/admin/logs/download", "GET", AuthorizationStatus.Operator)]
        public async Task Download()
        {
            var contentDisposition = $"attachment; filename={DateTime.UtcNow:yyyy-MM-dd H:mm:ss} - Node [{ServerStore.NodeTag}] - Logs.zip";
            HttpContext.Response.Headers["Content-Disposition"] = contentDisposition;
            HttpContext.Response.Headers["Content-Type"] = "application/zip";

            var adminLogsFileName = $"admin.logs.download.{Guid.NewGuid():N}";
            var adminLogsFilePath = ServerStore._env.Options.DataPager.Options.TempPath.Combine(adminLogsFileName);

            var from = GetDateTimeQueryString("from", required: false);
            var to = GetDateTimeQueryString("to", required: false);

            await using (var stream = SafeFileStream.Create(adminLogsFilePath.FullPath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.ReadWrite, 4096,
                             FileOptions.DeleteOnClose | FileOptions.SequentialScan))
            {
                using (var archive = new ZipArchive(stream, ZipArchiveMode.Create, true))
                {
                    foreach (var file in RavenLogManager.Instance.GetLogFiles(Server, from, to))
                    {

                        try
                        {
                            var entry = archive.CreateEntry(file.Name);
                            entry.LastWriteTime = file.LastWriteTime;

                            await using (var fs = File.Open(file.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                            {
                                entry.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

                                await using (var entryStream = entry.Open())
                                {
                                    await fs.CopyToAsync(entryStream);
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            await DebugInfoPackageUtils.WriteExceptionAsZipEntryAsync(e, archive, file.Name);
                        }
                    }
                }

                stream.Position = 0;
                await stream.CopyToAsync(ResponseBodyStream());
            }
        }
    }
}
