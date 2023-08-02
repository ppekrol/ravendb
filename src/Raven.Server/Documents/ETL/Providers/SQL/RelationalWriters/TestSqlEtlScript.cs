using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Server.Documents.ETL.Test;

namespace Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters
{
    internal sealed class TestSqlEtlScript : TestEtlScript<SqlEtlConfiguration, SqlConnectionString>
    {
        public bool PerformRolledBackTransaction;

        public SqlConnectionString Connection;
    }
}
