using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Sorters
{
    internal sealed class DeleteSorterCommand : UpdateDatabaseCommand
    {
        public string SorterName;

        public DeleteSorterCommand()
        {
            // for deserialization
        }

        public DeleteSorterCommand(string name, string databaseName, string uniqueRequestId)
            : base(databaseName, uniqueRequestId)
        {
            SorterName = name;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.DeleteSorter(SorterName);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(SorterName)] = SorterName;
        }
    }
}
