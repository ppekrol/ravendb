using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Web.System
{
    public sealed class BackupDatabaseHandler : RequestHandler
    {
        [RavenAction("/periodic-backup", "GET", AuthorizationStatus.ValidUser)]
        public Task GetPeriodicBackup()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            if (TryGetAllowedDbs(name, out var _, requireAdmin: false) == false)
                return Task.CompletedTask;

            var taskId = GetLongQueryString("taskId", required: true).Value;
            if (taskId == 0)
                throw new ArgumentException("Task ID cannot be 0");

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            using (var rawRecord = ServerStore.Cluster.ReadRawDatabaseRecord(context, name))
            {
                var periodicBackup = rawRecord.GetPeriodicBackupConfiguration(taskId);
                if (periodicBackup == null)
                    throw new InvalidOperationException($"Periodic backup task ID: {taskId} doesn't exist");

                context.WriteAsync(writer, periodicBackup.ToJson());
                writer.FlushAsync();
            }

            return Task.CompletedTask;
        }

        [RavenAction("/periodic-backup/status", "GET", AuthorizationStatus.ValidUser)]
        public Task GetPeriodicBackupStatus()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            if (TryGetAllowedDbs(name, out var _, requireAdmin: false) == false)
                return Task.CompletedTask;

            var taskId = GetLongQueryString("taskId", required: true);
            if (taskId == 0)
                throw new ArgumentException("Task ID cannot be 0");

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            using (var statusBlittable = ServerStore.Cluster.Read(context, PeriodicBackupStatus.GenerateItemName(name, taskId.Value)))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObjectAsync();
                writer.WritePropertyNameAsync(nameof(GetPeriodicBackupStatusOperationResult.Status));
                writer.WriteObjectAsync(statusBlittable);
                writer.WriteEndObjectAsync();
                writer.FlushAsync();
            }

            return Task.CompletedTask;
        }
    }
}
