namespace Raven.Server.Documents.Sharding.Streaming;

interal class ShardStreamItem<T>
{
    public T Item;
    public int ShardNumber;
}
