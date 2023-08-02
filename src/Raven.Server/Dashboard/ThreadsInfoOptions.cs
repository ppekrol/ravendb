using System;

namespace Raven.Server.Dashboard;

internal sealed class ThreadsInfoOptions
{
    public TimeSpan ThreadsInfoThrottle { get; set; } = TimeSpan.FromSeconds(1);
}
