namespace Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters
{
    internal sealed class DbCommandBuilder
    {
        public string Start, End;

        public string QuoteIdentifier(string unquotedIdentifier)
        {
            return Start + unquotedIdentifier + End;
        }
      
    }
}