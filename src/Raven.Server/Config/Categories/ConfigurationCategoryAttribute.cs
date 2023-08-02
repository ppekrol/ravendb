using System;

namespace Raven.Server.Config.Categories
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    internal sealed class ConfigurationCategoryAttribute : Attribute
    {
        public ConfigurationCategoryType Type { get; set; }

        public ConfigurationCategoryAttribute(ConfigurationCategoryType type)
        {
            Type = type;
        }
    }
}
