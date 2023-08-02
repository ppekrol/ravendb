using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    internal sealed class WellKnownAdminIssuers : ScalarObjectBase<OctetString>
    {
        private readonly OctetString _wellKnownAdminCertificates;

        public WellKnownAdminIssuers(ServerStore store)
            : base(SnmpOids.Server.WellKnownAdminCertificates)
        {
            var wellKnownIssuersThumbprints = store.Server.WellKnownIssuersThumbprints;
            if (wellKnownIssuersThumbprints == null || wellKnownIssuersThumbprints.Length == 0)
                return;

            _wellKnownAdminCertificates = new OctetString(string.Join(";", wellKnownIssuersThumbprints));
        }

        protected override OctetString GetData()
        {
            return _wellKnownAdminCertificates;
        }
    }
}
