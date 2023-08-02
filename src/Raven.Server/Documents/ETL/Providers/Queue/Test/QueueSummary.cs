using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.Queue.Test
{
    internal sealed class QueueSummary
    {
        public string QueueName { get; set; }

        public List<MessageSummary> Messages { get; set; }
    }

    internal sealed class MessageSummary
    {
        public string Body { get; set; }

        public CloudEventAttributes Attributes { get; set; }

        public string RoutingKey { get; set; }
    }
}
