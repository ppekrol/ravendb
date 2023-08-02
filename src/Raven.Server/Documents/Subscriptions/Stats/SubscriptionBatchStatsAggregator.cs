// -----------------------------------------------------------------------
//  <copyright file="SubscriptionBatchStatsAggregator.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Diagnostics;
using Raven.Server.Utils.Stats;

namespace Raven.Server.Documents.Subscriptions.Stats
{
    internal sealed class SubscriptionBatchStatsAggregator : StatsAggregator<SubscriptionBatchRunStats, SubscriptionBatchStatsScope>
    {
        private volatile SubscriptionBatchPerformanceStats _batchPerformanceStats;
        
        public SubscriptionBatchStatsAggregator(int id, SubscriptionBatchStatsAggregator lastStats) : base(id, lastStats)
        {
        }

        public override SubscriptionBatchStatsScope CreateScope()
        {
            Debug.Assert(Scope == null);
            return Scope = new SubscriptionBatchStatsScope(Stats);
        }
        
        public SubscriptionBatchPerformanceStats ToBatchPerformanceStats()
        {
            if (_batchPerformanceStats != null)
                return _batchPerformanceStats;
        
            lock (Stats)
            {
                if (_batchPerformanceStats != null)
                    return _batchPerformanceStats;
        
                return _batchPerformanceStats = CreateBatchPerformanceStats(completed: true);
            }
        }
        
        private SubscriptionBatchPerformanceStats CreateBatchPerformanceStats(bool completed) 
        {
            return new SubscriptionBatchPerformanceStats(Scope.Duration)
            {
                Started = StartTime,
                Completed = completed ? StartTime.Add(Scope.Duration) : (DateTime?)null,
                
                ConnectionId = Stats.ConnectionId, 
                BatchId = Id,
                
                NumberOfDocuments = Stats.NumberOfDocuments,
                SizeOfDocumentsInBytes = Stats.SizeOfDocumentsInBytes,
                
                NumberOfIncludedDocuments = Stats.NumberOfIncludedDocuments,
                SizeOfIncludedDocumentsInBytes = Stats.SizeOfIncludedDocumentsInBytes,
                
                NumberOfIncludedCounters = Stats.NumberOfIncludedCounters,
                SizeOfIncludedCountersInBytes = Stats.SizeOfIncludedCountersInBytes,

                NumberOfIncludedTimeSeriesEntries = Stats.NumberOfIncludedTimeSeriesEntries,
                SizeOfIncludedTimeSeriesInBytes = Stats.SizeOfIncludedTimeSeriesInBytes,
                
                Exception = Stats.Exception,
                
                Details = Scope.ToPerformanceOperation("Batch")
            };
        }
        
        public SubscriptionBatchPerformanceStats ToBatchPerformanceLiveStatsWithDetails()
        {
            if (_batchPerformanceStats != null)
                return _batchPerformanceStats;

            if (Scope == null || Stats == null)
                return null;

            if (Completed)
                return ToBatchPerformanceStats();

            return CreateBatchPerformanceStats(completed: false);
        }
    }
}
