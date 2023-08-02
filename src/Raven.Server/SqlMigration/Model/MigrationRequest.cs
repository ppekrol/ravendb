namespace Raven.Server.SqlMigration.Model
{
    internal sealed class MigrationRequest
    {
        public MigrationSettings Settings { get; set; }
        public SourceSqlDatabase Source { get; set; }
    }
}
