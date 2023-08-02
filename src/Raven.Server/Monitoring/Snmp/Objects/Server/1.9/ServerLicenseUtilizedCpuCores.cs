using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    internal sealed class ServerLicenseUtilizedCpuCores : ScalarObjectBase<Integer32>
    {
        private readonly ServerStore _store;

        public ServerLicenseUtilizedCpuCores(ServerStore store)
            : base(SnmpOids.Server.ServerLicenseUtilizedCpuCores)
        {
            _store = store;
        }

        protected override Integer32 GetData()
        {
            var cores = _store.LicenseManager.GetCoresLimitForNode(out _);
            return new Integer32(cores);
        }
    }
}
