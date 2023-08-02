using System;

namespace Raven.Server.NotificationCenter
{
    internal sealed class NotificationCenterOptions
    {
        public TimeSpan DatabaseStatsThrottle { get; set; } = TimeSpan.FromSeconds(5);
    }
}