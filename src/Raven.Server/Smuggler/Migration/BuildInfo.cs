﻿using System.Collections.Generic;

namespace Raven.Server.Smuggler.Migration
{
    internal class BuildInfo
    {
        public int BuildVersion { get; set; }

        public string ProductVersion { get; set; }

        public MajorVersion MajorVersion { get; set; }

        public string FullVersion { get; set; }
    }

    internal sealed class BuildInfoWithResourceNames : BuildInfo
    {
        public List<string> DatabaseNames { get; set; }

        public List<string> FileSystemNames { get; set; }

        public bool Authorized { get; set; }

        public bool IsLegacyOAuthToken { get; set; }
    }
}
