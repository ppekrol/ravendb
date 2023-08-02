namespace Raven.Server.Documents.ETL.Providers.ElasticSearch.Test
{
    internal sealed class IndexSummary
    {
        public string IndexName { get; set; }

        public string[] Commands { get; set; }
    }
}
