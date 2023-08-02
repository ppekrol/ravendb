namespace Raven.Server.SqlMigration.Model
{
    internal sealed class MigrationTestRequest
    {
        public SourceSqlDatabase Source { get; set; }
        public MigrationTestSettings Settings { get; set; }
    }
    
}
