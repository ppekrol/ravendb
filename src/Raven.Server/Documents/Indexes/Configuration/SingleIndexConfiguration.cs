﻿using System.Reflection;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Config.Settings;

namespace Raven.Server.Documents.Indexes.Configuration
{
    internal class SingleIndexConfiguration : IndexingConfiguration
    {
        private readonly RavenConfiguration _databaseConfiguration;

        public SingleIndexConfiguration(IndexConfiguration clientConfiguration, RavenConfiguration databaseConfiguration)
            : base(databaseConfiguration)
        {
            _databaseConfiguration = databaseConfiguration;

            Initialize(
                key =>
                    new SettingValue(
                        clientConfiguration.GetValue(key) ?? databaseConfiguration.GetSetting(key),
                        clientConfiguration.GetValue(key) != null || databaseConfiguration.DoesKeyExistInSettings(key),
                        databaseConfiguration.GetServerWideSetting(key),
                        databaseConfiguration.DoesKeyExistInSettings(key, true)),
                databaseConfiguration.GetServerWideSetting(RavenConfiguration.GetKey(x => x.Core.DataDirectory)),
                databaseConfiguration.ResourceType, 
                databaseConfiguration.ResourceName, 
                throwIfThereIsNoSetMethod: false);
        }

        public override bool Disabled => _databaseConfiguration.Indexing.Disabled;

        public override bool RunInMemory => _databaseConfiguration.Indexing.RunInMemory;

        public override PathSetting TempPath => _databaseConfiguration.Indexing.TempPath;

        public IndexUpdateType CalculateUpdateType(SingleIndexConfiguration newConfiguration)
        {
            var result = IndexUpdateType.None;
            foreach (var property in GetConfigurationProperties())
            {
                var currentValue = property.Info.GetValue(this);
                var newValue = property.Info.GetValue(newConfiguration);

                if (Equals(currentValue, newValue))
                    continue;

                var updateTypeAttribute = property.Info.GetCustomAttribute<IndexUpdateTypeAttribute>();

                if (updateTypeAttribute.UpdateType == IndexUpdateType.Reset)
                    return IndexUpdateType.Reset; // worst case, we do not need to check further

                result = updateTypeAttribute.UpdateType;
            }

            return result;
        }
    }
}
