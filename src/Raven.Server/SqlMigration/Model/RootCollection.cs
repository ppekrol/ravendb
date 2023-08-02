namespace Raven.Server.SqlMigration.Model
{
    internal sealed class RootCollection : CollectionWithReferences
    {
        public string SourceTableQuery { get; set; }
        public string Patch { get; set; }

        public RootCollection(string sourceTableSchema, string sourceTableName, string name) : base(sourceTableSchema, sourceTableName, name)
        {
        }
    }
}
