﻿using Raven.Client.Properties;
using Raven.Server.Smuggler.Documents.Processors;

namespace Raven.Server.ServerWide
{
    internal sealed class ServerVersion
    {
        private static int? _buildVersion;
        private static BuildVersionType? _buildType;
        private static string _commitHash;
        private static string _version;
        private static string _fullVersion;

        public static string Version => 
            _version ?? (_version = RavenVersionAttribute.Instance.Version);

        public static int Build =>  
            _buildVersion ?? (_buildVersion = RavenVersionAttribute.Instance.BuildVersion).Value;
        public static BuildVersionType BuildType =>
            _buildType ?? (_buildType = BuildVersion.Type(Build)).Value;
        public static string CommitHash => 
            _commitHash ?? (_commitHash = RavenVersionAttribute.Instance.CommitHash);
        public static string FullVersion => 
            _fullVersion ?? (_fullVersion = RavenVersionAttribute.Instance.FullVersion);

        public const int DevBuildNumber = 60;

        public static bool IsNightlyOrDev(long buildVersion)
        {
            return buildVersion >= 60 && buildVersion < 70;
        }
    }
}
