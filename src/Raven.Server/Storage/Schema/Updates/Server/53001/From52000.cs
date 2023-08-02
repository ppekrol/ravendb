﻿namespace Raven.Server.Storage.Schema.Updates.Server
{
    internal sealed class From52000 : ISchemaUpdate
    {
        public int From => 52_000;

        public int To => 53_001;

        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Server;

        public bool Update(UpdateStep step)
        {
            return true;
        }
    }
}
