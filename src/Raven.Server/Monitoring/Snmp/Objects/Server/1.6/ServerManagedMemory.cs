﻿using Lextm.SharpSnmpLib;
using Sparrow;
using Sparrow.LowMemory;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    internal sealed class ServerManagedMemory : ScalarObjectBase<Gauge32>
    {
        public ServerManagedMemory() : base(SnmpOids.Server.ManagedMemory)
        {
        }

        protected override Gauge32 GetData()
        {
            var managedMemoryInBytes = AbstractLowMemoryMonitor.GetManagedMemoryInBytes();

            return new Gauge32(new Size(managedMemoryInBytes, SizeUnit.Bytes).GetValue(SizeUnit.Megabytes));
        }
    }
}
