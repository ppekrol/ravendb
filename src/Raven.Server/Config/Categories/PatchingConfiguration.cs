using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;

namespace Raven.Server.Config.Categories
{
    public class PatchingConfiguration : ConfigurationCategory
    {
        [DefaultValue(5)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Raven/Patching/Timeout")]
        public TimeSetting Timeout { get; set; }
    }
}