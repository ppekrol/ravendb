﻿using System.Collections.Generic;
using Raven.Server.Documents.ETL.Providers.Queue.RabbitMq;
using Raven.Server.Documents.ETL.Providers.Queue.Test;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Queue;

internal sealed class QueueWriterSimulator
{
    public List<MessageSummary> SimulateExecuteMessages<T>(QueueWithItems<T> queueMessages, DocumentsOperationContext context)
        where T : QueueItem
    {
        List<MessageSummary> result = new();
        if (queueMessages.Items.Count <= 0) return result;

        
        foreach (var message in queueMessages.Items)
        {
            var messageSummary = new MessageSummary()
            {
                Body = message.TransformationResult.ToString(),
                Attributes = message.Attributes,
                RoutingKey = (message as RabbitMqItem)?.RoutingKey
            };
            
            result.Add(messageSummary);
        }
        
        return result;
    }
}
