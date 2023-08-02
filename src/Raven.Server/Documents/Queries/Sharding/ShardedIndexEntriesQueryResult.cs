namespace Raven.Server.Documents.Queries.Sharding;

internal sealed class ShardedIndexEntriesQueryResult : IndexEntriesQueryResult
{

    public ShardedIndexEntriesQueryResult() : base(indexDefinitionRaftIndex: null)
    {
    }
}
