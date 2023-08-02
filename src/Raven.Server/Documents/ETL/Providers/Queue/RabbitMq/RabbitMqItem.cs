namespace Raven.Server.Documents.ETL.Providers.Queue.RabbitMq;

internal sealed class RabbitMqItem : QueueItem
{
    public RabbitMqItem(QueueItem item) : base(item)
    {
    }

    public string RoutingKey { get; set; }
}
