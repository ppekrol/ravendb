using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    internal sealed class DatabaseNodeCount : DatabaseBase<Integer32>
    {
        public DatabaseNodeCount(ServerStore serverStore)
            : base(serverStore, SnmpOids.Databases.General.NodeCount)
        {
        }

        protected override Integer32 GetData()
        {
            return new Integer32(GetCount());
        }

        private int GetCount()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                return GetDatabases(context).Count(x => x.Topology.RelevantFor(ServerStore.NodeTag));
            }
        }
    }
}
