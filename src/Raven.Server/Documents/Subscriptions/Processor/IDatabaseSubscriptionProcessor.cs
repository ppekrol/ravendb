using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Subscriptions.Processor;

internal interface IDatabaseSubscriptionProcessor
{
    public SubscriptionPatchDocument Patch { get; set; }

    long GetLastItemEtag(DocumentsOperationContext context, string collection);
}
