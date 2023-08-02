using System;

namespace Raven.Server.ServerWide.Context
{
    internal interface IDocumentsContextPool : IMemoryContextPool
    {
        IDisposable AllocateOperationContext(out DocumentsOperationContext context);
    }
}