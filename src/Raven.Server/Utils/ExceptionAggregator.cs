using System;
using System.Threading.Tasks;
using NLog;
using Sparrow.Collections;

namespace Raven.Server.Utils
{
    public class ExceptionAggregator
    {
        private readonly Logger _logger;
        private readonly string _errorMsg;
        private readonly ConcurrentSet<Exception> _list = new ConcurrentSet<Exception>();

        public ExceptionAggregator(string errorMsg)
            : this(null, errorMsg)
        {
        }

        public ExceptionAggregator(Logger logger, string errorMsg)
        {
            _logger = logger;
            _errorMsg = errorMsg;
        }

        public void Execute(IDisposable d)
        {
            try
            {
                d?.Dispose();
            }
            catch (Exception e)
            {
                _list.Add(e);
            }
        }

        public void Execute(Action action)
        {
            try
            {
                action();
            }
            catch (Exception e)
            {
                _list.Add(e);
            }
        }

        public async Task ExecuteAsync(Task task)
        {
            try
            {
                await task;
            }
            catch (Exception e)
            {
                _list.Add(e);
            }
        }

        public async Task ExecuteAsync(ValueTask task)
        {
            try
            {
                await task;
            }
            catch (Exception e)
            {
                _list.Add(e);
            }
        }

        public AggregateException GetAggregateException()
        {
            if (_list.IsEmpty)
                return null;

            return new AggregateException(_errorMsg, _list);
        }

        public void ThrowIfNeeded()
        {
            if (_list.IsEmpty)
                return;

            var aggregateException = GetAggregateException();

            if (_logger is { IsInfoEnabled: true })
                _logger.Info(aggregateException, _errorMsg);

            throw aggregateException;
        }
    }
}
