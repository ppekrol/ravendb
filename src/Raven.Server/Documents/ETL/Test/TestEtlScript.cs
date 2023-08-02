using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;

namespace Raven.Server.Documents.ETL.Test
{
    internal abstract class TestEtlScript<TConfiguration, TConnectionString> 
        where TConfiguration : EtlConfiguration<TConnectionString> 
        where TConnectionString : ConnectionString
    {
        public string DocumentId;

        public bool IsDelete;

        public TConfiguration Configuration;
    }
}
