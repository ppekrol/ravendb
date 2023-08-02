using System.Collections.Generic;
using System.ComponentModel;
using Microsoft.Extensions.Configuration;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Raven.Server.ServerWide;
using Sparrow;
using Sparrow.Logging;

namespace Raven.Server.Config.Categories
{
    [ConfigurationCategory(ConfigurationCategoryType.Logs)]
    internal sealed class LogsConfiguration : ConfigurationCategory
    {
        [DefaultValue("Logs")]
        [ConfigurationEntry("Logs.Path", ConfigurationEntryScope.ServerWideOnly)]
        public PathSetting Path { get; set; }

        [DefaultValue(LogMode.Operations)]
        [ConfigurationEntry("Logs.Mode", ConfigurationEntryScope.ServerWideOnly)]
        public LogMode Mode { get; set; }

        [DefaultValue(true)]
        [ConfigurationEntry("Logs.UseUtcTime", ConfigurationEntryScope.ServerWideOnly)]
        public bool UseUtcTime { get; set; }

        [DefaultValue(128)]
        [MinValue(16)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Logs.MaxFileSizeInMb", ConfigurationEntryScope.ServerWideOnly)]
        public Size MaxFileSize { get; set; }

        [Description("How far back we should retain log entries in hours")]
        [DefaultValue(3 * 24)]
        [MinValue(24)]
        [TimeUnit(TimeUnit.Hours)]
        [ConfigurationEntry("Logs.RetentionTimeInHrs", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting? RetentionTime { get; set; }

        [Description("The maximum size of the log after which the old files will be deleted")]
        [DefaultValue(null)]
        [MinValue(256)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Logs.RetentionSizeInMb", ConfigurationEntryScope.ServerWideOnly)]
        public Size? RetentionSize { get; set; }

        [Description("Will determine whether to compress the log files")]
        [DefaultValue(false)]
        [ConfigurationEntry("Logs.Compress", ConfigurationEntryScope.ServerWideOnly)]
        public bool Compress { get; set; }

        #region Microsoft Logs
        [Description("Will determine whether to disable the Microsoft logs")]
        [DefaultValue(true)]
        [ConfigurationEntry("Logs.Microsoft.Disable", ConfigurationEntryScope.ServerWideOnly)]
        public bool DisableMicrosoftLogs { get; set; }
        
        [Description("The path to json configuration file of Microsoft logs")]
        [ReadOnlyPath]
        [DefaultValue("settings.logs.microsoft.json")]
        [ConfigurationEntry("Logs.Microsoft.ConfigurationPath", ConfigurationEntryScope.ServerWideOnly)]
        public PathSetting MicrosoftLogsConfigurationPath { get; set; }
   
        #endregion
        
    }
}
