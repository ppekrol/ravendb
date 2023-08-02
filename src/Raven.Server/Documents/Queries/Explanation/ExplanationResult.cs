namespace Raven.Server.Documents.Queries.Explanation
{
    internal sealed class ExplanationResult
    {
        public string Key;

        public Lucene.Net.Search.Explanation Explanation;
    }
}
