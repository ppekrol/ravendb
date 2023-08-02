﻿using Raven.Server.ServerWide.BackgroundTasks;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    internal sealed class NewVersionAvailableDetails : INotificationDetails
    {
        public NewVersionAvailableDetails(LatestVersionCheck.VersionInfo versionInfo)
        {
            VersionInfo = versionInfo;
        }

        public LatestVersionCheck.VersionInfo VersionInfo { get; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                [nameof(VersionInfo)] = VersionInfo.ToJson()
            };
        }
    }
}