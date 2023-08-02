// -----------------------------------------------------------------------
//  <copyright file="SubscriptionBatchPerformanceStats.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;

namespace Raven.Server.Documents.Subscriptions.Stats
{
    internal sealed class SubscriptionBatchPerformanceStats
    {
        public long BatchId { get; set; }
        public long ConnectionId { get; set; }
        
        public long NumberOfDocuments { get; set; }
        public long SizeOfDocumentsInBytes { get; set; }
        
        public long NumberOfIncludedDocuments { get; set; }
        public long SizeOfIncludedDocumentsInBytes { get; set; }
        
        public long NumberOfIncludedCounters { get; set; }
        public long SizeOfIncludedCountersInBytes { get; set; }

        public long NumberOfIncludedTimeSeriesEntries { get; set; }
        public long SizeOfIncludedTimeSeriesInBytes { get; set; }

        public DateTime Started { get; set; }
        public DateTime? Completed { get; set; }
        
        public string Exception { get; set; }
        
        public double DurationInMs { get; }
        
        public SubscriptionBatchPerformanceOperation Details { get; set; }
        
        public SubscriptionBatchPerformanceStats(TimeSpan duration)
        {
            DurationInMs = Math.Round(duration.TotalMilliseconds, 2);
        }
    }
}
