namespace Raven.Server.Documents;

internal sealed class CompareExchangeStorage : AbstractCompareExchangeStorage
{
    public CompareExchangeStorage(DocumentDatabase database)
        : base(database.ServerStore)
    {
    }
}
