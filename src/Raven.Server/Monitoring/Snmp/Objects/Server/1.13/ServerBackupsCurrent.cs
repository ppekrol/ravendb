using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    internal sealed class ServerBackupsCurrent : ScalarObjectBase<Integer32>
    {
        private readonly ServerStore _serverStore;

        public ServerBackupsCurrent(ServerStore serverStore)
            : base(SnmpOids.Server.ServerBackupsCurrent)
        {
            _serverStore = serverStore;
        }

        protected override Integer32 GetData()
        {
            return new Integer32(_serverStore.ConcurrentBackupsCounter.CurrentNumberOfRunningBackups);
        }
    }
}
