using Raven.Client.Documents.Changes;

namespace Raven.Server.Documents.Sharding.Changes;

internal interface IAggressiveCacheChanges<out TChange>
{
    IChangesObservable<TChange> ForAggressiveCaching();
}
