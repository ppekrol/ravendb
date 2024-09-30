using System;
using JetBrains.Annotations;
using Raven.Server.Utils;
using Sparrow.LowMemory;
using Sparrow.Server.LowMemory;

namespace Raven.Server.ServerWide.Memory;

public class CachingLowMemoryMonitor : LowMemoryMonitor
{
    private readonly RavenServer _server;

    public CachingLowMemoryMonitor([NotNull] RavenServer server)
    {
        _server = server ?? throw new ArgumentNullException(nameof(server));
    }

    public override MemoryInfoResult GetMemoryInfo(bool extended = false)
    {
        return _server.MetricCacher.GetValue<MemoryInfoResult>(MetricCacher.Keys.Server.MemoryInfoExtended.RefreshRate15Seconds);
    }
}
