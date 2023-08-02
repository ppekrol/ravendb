using System;
using Lextm.SharpSnmpLib;
using Raven.Client.Exceptions.Database;
using Raven.Client.Extensions;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    internal sealed class DatabaseFaultedCount : DatabaseBase<Integer32>
    {
        public DatabaseFaultedCount(ServerStore serverStore)
            : base(serverStore, SnmpOids.Databases.General.FaultedCount)
        {
        }

        protected override Integer32 GetData()
        {
            return new Integer32(GetCount());
        }

        private int GetCount()
        {
            var count = 0;
            foreach (var kvp in ServerStore.DatabasesLandlord.DatabasesCache)
            {
                var databaseTask = kvp.Value;

                if (databaseTask == null)
                    continue;

                if (databaseTask.IsFaulted == false) 
                    continue;
                
                var e = databaseTask.Exception?.ExtractSingleInnerException();
                if (e is DatabaseDisabledException)
                    continue;

                count++;
            }

            return count;
        }
    }
}
