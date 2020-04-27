﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Server;

namespace Raven.Server.Documents.Replication
{
    public class InterruptibleRead : IDisposable
    {
        private Task<Result> _prevCall;
        private readonly Dictionary<AsyncManualResetEvent, Task<Task>> _previousWait = new Dictionary<AsyncManualResetEvent, Task<Task>>();

        private readonly DocumentsContextPool _contextPool;
        private readonly Stream _stream;

        public struct Result : IDisposable
        {
            public BlittableJsonReaderObject Document;
            public IDisposable ReturnContext;
            public DocumentsOperationContext Context;
            public bool Timeout;
            public bool Interrupted;
            public void Dispose()
            {
                Document?.Dispose();
                ReturnContext?.Dispose();
            }
        }

        public InterruptibleRead(DocumentsContextPool contextPool, Stream stream)
        {
            _contextPool = contextPool;
            _stream = stream;
        }

        public Result ParseToMemory(
            AsyncManualResetEvent interrupt,
            string debugTag,
            int timeout,
            JsonOperationContext.MemoryBuffer buffer,
            CancellationToken token)
        {
            if (_prevCall == null)
            {
                _prevCall = ReadNextObject(debugTag, buffer, token);
                _previousWait.Clear();
            }

            if (_prevCall.IsCompleted)
            {
                return ReturnAndClearValue();
            }

            if (_previousWait.TryGetValue(interrupt, out Task<Task> task) == false)
            {
                _previousWait[interrupt] = task = Task.WhenAny(_prevCall, interrupt.WaitAsync());
            }

            if (task.Wait(timeout, token) == false)
                return new Result { Timeout = true };

            if (task.Result != _prevCall)
            {
                _previousWait.Remove(interrupt);
                return new Result { Interrupted = true };
            }

            return ReturnAndClearValue();
        }

        private Result ReturnAndClearValue()
        {
            try
            {
                return _prevCall.Result;
            }
            catch (ObjectDisposedException)
            {
                //we are disposing, so don't care about this exception.
                //this is thrown from inside ParseToMemoryAsync() call 
                //from inside of ReadNextObject() when disposing (thrown from disposed stream basically)
                return new Result
                {
                    Interrupted = true
                };
            }
            finally
            {
                _prevCall = null;
                _previousWait.Clear();
            }
        }

        private async Task<Result> ReadNextObject(string debugTag, JsonOperationContext.MemoryBuffer buffer, CancellationToken token)
        {
            var retCtx = _contextPool.AllocateOperationContext(out DocumentsOperationContext context);
            try
            {
                var jsonReaderObject = await context.ParseToMemoryAsync(_stream, debugTag, BlittableJsonDocumentBuilder.UsageMode.None, buffer, token);
                return new Result
                {
                    Document = jsonReaderObject,
                    ReturnContext = retCtx,
                    Context = context
                };
            }
            catch (Exception)
            {
                retCtx.Dispose();
                throw;
            }
        }

        public void Dispose()
        {
            if (_prevCall == null)
                return;

            try
            {
                _stream.Dispose();// need to dispose the current stream to abort the operation
                using (_prevCall.Result)
                {

                }
            }
            catch (Exception)
            {
                // explicitly ignoring this
            }
            finally
            {
                _prevCall = null;
            }
        }
    }
}
