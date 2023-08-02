namespace Raven.Server.Storage.Schema
{
    internal interface ISchemaUpdate
    {
        int From { get; }

        int To { get; }

        SchemaUpgrader.StorageType StorageType { get; }

        bool Update(UpdateStep step);
    }
}
