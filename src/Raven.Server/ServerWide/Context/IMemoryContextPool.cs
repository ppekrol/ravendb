using System;
using Sparrow.Json;

namespace Raven.Server.ServerWide.Context
{
    internal interface IMemoryContextPool : IDisposable
    {
        IDisposable AllocateOperationContext(out JsonOperationContext context);
    }
}