using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Analyzers
{
    internal sealed class DeleteAnalyzerCommand : UpdateDatabaseCommand
    {
        public string AnalyzerName;

        public DeleteAnalyzerCommand()
        {
            // for deserialization
        }

        public DeleteAnalyzerCommand(string name, string databaseName, string uniqueRequestId)
            : base(databaseName, uniqueRequestId)
        {
            AnalyzerName = name;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.DeleteAnalyzer(AnalyzerName);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(AnalyzerName)] = AnalyzerName;
        }
    }
}
