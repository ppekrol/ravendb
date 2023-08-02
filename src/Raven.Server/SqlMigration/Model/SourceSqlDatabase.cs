namespace Raven.Server.SqlMigration.Model
{
    internal sealed class SourceSqlDatabase
    {
        public MigrationProvider Provider { get; set; }

        public string ConnectionString { get; set; }

        public string[] Schemas { get; set; }
    }
}
