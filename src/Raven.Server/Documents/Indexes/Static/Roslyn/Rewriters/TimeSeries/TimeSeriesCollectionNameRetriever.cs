namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters.TimeSeries
{
    internal sealed class TimeSeriesCollectionNameRetriever : CollectionNameRetrieverBase
    {
        public static CollectionNameRetrieverBase QuerySyntax => new QuerySyntaxRewriter("timeSeries", "TimeSeries");

        public static CollectionNameRetrieverBase MethodSyntax => new MethodSyntaxRewriter("timeSeries", "TimeSeries");
    }
}
