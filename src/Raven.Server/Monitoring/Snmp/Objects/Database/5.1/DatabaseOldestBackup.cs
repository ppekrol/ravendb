using System;
using Lextm.SharpSnmpLib;
using Raven.Client.Util;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class DatabaseOldestBackup : ScalarObjectBase<TimeTicks>
    {
        private readonly ServerStore _serverStore;

        public DatabaseOldestBackup(ServerStore serverStore)
            : base(SnmpOids.Databases.General.TimeSinceOldestBackup)
        {
            _serverStore = serverStore;
        }

        protected override TimeTicks GetData()
        {
            return new TimeTicks(GetTimeSinceOldestBackup(_serverStore));
        }

        private static TimeSpan GetTimeSinceOldestBackup(ServerStore serverStore)
        {
            using (serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var now = SystemTime.UtcNow;
                DateTime oldestBackup = now;

                foreach (var databaseName in serverStore.Cluster.GetDatabaseNames(context))
                {
                    var lastBackup = GetLastBackup(context, serverStore, databaseName);
                    if (lastBackup == null)
                        continue;

                    if (lastBackup < oldestBackup)
                        oldestBackup = lastBackup.Value;
                }

                return now <= oldestBackup
                    ? TimeSpan.Zero
                    : now - oldestBackup;
            }
        }

        private static DateTime? GetLastBackup(TransactionOperationContext context, ServerStore serverStore, string databaseName)
        {
            using (var databaseRecord = serverStore.Cluster.ReadDatabaseRecord(context, databaseName, out _))
            {
                if (databaseRecord == null)
                    return null; // should not happen

                var periodicBackupTaskIds = databaseRecord.GetPeriodicBackupsTaskIds();
                if (periodicBackupTaskIds == null || periodicBackupTaskIds.Count == 0)
                    return null; // no backup

                var lastBackup = DateTime.MinValue;

                foreach (var periodicBackupTaskId in periodicBackupTaskIds)
                {
                    var status = PeriodicBackupRunner.GetBackupStatusFromCluster(serverStore, context, databaseName, periodicBackupTaskId);
                    if (status == null)
                        continue; // we have a backup task but no backup was ever done

                    var currentLatestBackup = LastBackupDate(status.LastFullBackup, status.LastIncrementalBackup);
                    if (currentLatestBackup > lastBackup)
                        lastBackup = currentLatestBackup;
                }

                return lastBackup;

                static DateTime LastBackupDate(DateTime? fullBackup, DateTime? incrementalBackup)
                {
                    if (fullBackup == null)
                        return DateTime.MinValue; // we never did a full backup

                    if (incrementalBackup == null)
                        return fullBackup.Value; // no incremental backup

                    return incrementalBackup > fullBackup ? incrementalBackup.Value : fullBackup.Value;
                }
            }
        }
    }
}
