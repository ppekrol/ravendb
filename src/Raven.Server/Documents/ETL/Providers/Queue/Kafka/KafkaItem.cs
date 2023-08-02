namespace Raven.Server.Documents.ETL.Providers.Queue.Kafka;

internal sealed class KafkaItem : QueueItem
{
    public KafkaItem(QueueItem item) : base(item)
    {
    }
}
