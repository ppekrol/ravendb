using Lextm.SharpSnmpLib;
using Raven.Server.Utils;
using Raven.Server.Utils.Cpu;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ProcessCpu : ScalarObjectBase<Gauge32>
    {
        private readonly MetricCacher _metricCacher;
        private readonly ICpuUsageCalculator _calculator;

        public ProcessCpu(MetricCacher metricCacher, ICpuUsageCalculator calculator)
            : base(SnmpOids.Server.ProcessCpu)
        {
            _metricCacher = metricCacher;
            _calculator = calculator;
        }

        protected override Gauge32 GetData()
        {
            return new Gauge32((int)_metricCacher.GetValue(MetricCacher.Keys.Server.CpuUsage, _calculator.Calculate).ProcessCpuUsage);
        }
    }
}
