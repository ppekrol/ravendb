﻿using System;
using Lextm.SharpSnmpLib;
using Raven.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    internal sealed class ServerGcPauseTimePercentage : ServerGcBase<Gauge32>
    {
        public ServerGcPauseTimePercentage(MetricCacher metricCacher, GCKind gcKind)
            : base(metricCacher, gcKind, SnmpOids.Server.GcPauseTimePercentage)
        {
        }

        protected override Gauge32 GetData()
        {
            var pauseTimePercentage = GetGCMemoryInfo().PauseTimePercentage;
            return new Gauge32((int)pauseTimePercentage);
        }
    }
}
