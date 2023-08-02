namespace Raven.Server.Documents.Sharding.Streaming;

internal class ShardStreamItem<T>
{
    public T Item;
    public int ShardNumber;
}
