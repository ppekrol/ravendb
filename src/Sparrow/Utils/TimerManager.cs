using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Timers;
using Sparrow.Collections;

namespace Sparrow.Utils;

internal static class TimerManager
{
    private class TimerHolder
    {
        private readonly ConcurrentSet<WeakReference<ITimerManagerWatcher>> _subscribers = new();

        private readonly Timer _timer;

        private readonly object _locker = new();

        public TimerHolder(TimeSpan frequency)
        {
            _timer = new Timer(frequency.TotalMilliseconds);
            _timer.Elapsed += NotifySubscribers;
        }

        private void NotifySubscribers(object sender, ElapsedEventArgs e)
        {
            lock (_locker)
            {
                if (_subscribers.IsEmpty)
                {
                    _timer.Stop();
                    return;
                }
            }

            List<WeakReference<ITimerManagerWatcher>> toDelete = null;
            foreach (var subscriber in _subscribers)
            {
                if (subscriber.TryGetTarget(out var timer) == false)
                {
                    toDelete ??= new List<WeakReference<ITimerManagerWatcher>>();
                    toDelete.Add(subscriber);
                    continue;
                }

                _ = Task.Run(timer.ExecuteTimer)
                    .ContinueWith(t => GC.KeepAlive(t.Exception), TaskContinuationOptions.ExecuteSynchronously | TaskContinuationOptions.OnlyOnFaulted);
            }

            if (toDelete != null)
            {
                foreach (var subscriber in toDelete)
                    _subscribers.TryRemove(subscriber);
            }
        }

        public void Register(ITimerManagerWatcher watcher)
        {
            lock (_locker)
            {
                _subscribers.Add(new WeakReference<ITimerManagerWatcher>(watcher));
                if (_subscribers.Count == 1)
                    _timer.Start();
            }
        }
    }

    private static readonly ConcurrentDictionary<TimeSpan, TimerHolder> Timers = new();

    public static void Register(ITimerManagerWatcher watcher, TimeSpan interval)
    {
        var timers = Timers.GetOrAdd(interval, ts => new TimerHolder(ts));

        timers.Register(watcher);
    }
}

internal interface ITimerManagerWatcher
{
    void ExecuteTimer();
}
