using System.Collections.Generic;
using Raven.Server.Documents.ETL.Test;

namespace Raven.Server.Documents.ETL.Providers.ElasticSearch.Test
{
    internal sealed class ElasticSearchEtlTestScriptResult : TestEtlScriptResult
    {
        public List<IndexSummary> Summary { get; set; }
    }
}
