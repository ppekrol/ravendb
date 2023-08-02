using System.Linq;

namespace Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters
{
    internal class RelationalDatabaseWriterBase
    {
        protected readonly bool IsSqlServerFactoryType;

        private static readonly string[] SqlServerFactoryNames =
        {
            "System.Data.SqlClient",
            "System.Data.SqlServerCe.4.0",
            "MySql.Data.MySqlClient",
            "System.Data.SqlServerCe.3.5"
        };

        public RelationalDatabaseWriterBase(string factoryName)
        {
            if (SqlServerFactoryNames.Contains(factoryName))
            {
                IsSqlServerFactoryType = true;
            }
        }
    }
}
