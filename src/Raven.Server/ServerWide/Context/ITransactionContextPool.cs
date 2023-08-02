using System;
using Sparrow.Json;

namespace Raven.Server.ServerWide.Context
{
    internal interface ITransactionContextPool<TOperationContext> : IMemoryContextPool
        where TOperationContext : JsonOperationContext
    {
        IDisposable AllocateOperationContext(out TOperationContext context);
    }
}
