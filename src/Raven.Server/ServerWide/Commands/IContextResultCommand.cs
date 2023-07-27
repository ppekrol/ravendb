using System;
using System.Diagnostics;
using System.Threading;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Threading;

namespace Raven.Server.ServerWide.Commands;

public interface IContextResultCommand
{
    object CloneResult(JsonOperationContext context, object result);
}

public class ContextResult : IDisposable
{
    private readonly JsonOperationContext _context;
    private readonly IDisposable _returnContext;
    private readonly SingleUseFlag _disposed = new SingleUseFlag();
    private readonly Func<JsonOperationContext, object, object> _copyAction;

    public object Result { get; private set; }


    public static ContextResult CreateContextResult<T>(JsonContextPoolBase<T> contextPool, Func<JsonOperationContext, object, object> copyAction)
        where T : JsonOperationContext
    {
        var returnContext = contextPool.AllocateOperationContext(out JsonOperationContext context);
        if(context._arenaAllocator._isDisposed.IsRaised())
            Console.WriteLine();
        return new ContextResult(context, returnContext, copyAction);
    }
    
    private ContextResult(JsonOperationContext context, IDisposable returnContext, Func<JsonOperationContext, object, object> copyAction)
    {
        _copyAction = copyAction;
        _returnContext = returnContext;
        _context = context;
    }
    
    public ContextResult(object result)
    {
        //In case the result was written on an appropriate context 
        Result = result;
    }
            
    ~ContextResult()
    {
        try
        {
            Dispose(false);
        }
        //TODO to check the catch
        // catch (ObjectDisposedException)
        catch
        {
            //TODO to check the comment
            // This is expected, we might be calling the finalizer on an object that
            // was already disposed, we don't want to error here because of this
        }
    }

    public void Dispose(bool disposing)
    {
        if (_disposed.Raise() == false)
            return;
        
        if (disposing)
            Monitor.Enter(this);

        try
        {
            GC.SuppressFinalize(this);
            _returnContext?.Dispose();
        }
        finally
        {
            if (disposing)
                Monitor.Exit(this);
        }
    }
    
    public void Dispose()
    {
        Dispose(true);
    }

    public void Copy(object result)
    {
        Debug.Assert(_context != null);
        if(_context != null)
            Result = _copyAction.Invoke(_context, result);
    }
}

