// -----------------------------------------------------------------------
//  <copyright file="SubscriptionBatchRunStats.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

namespace Raven.Server.Documents.Subscriptions.Stats
{
    internal sealed class SubscriptionBatchRunStats
    {
        public long TaskId { get; set; }
        public string TaskName { get; set; }
        
        public long ConnectionId { get; set; }
        public long BatchId { get; set; }
        
        public long NumberOfDocuments { get; set; }
        public long SizeOfDocumentsInBytes { get; set; }
        
        public long NumberOfIncludedDocuments { get; set; }
        public long SizeOfIncludedDocumentsInBytes { get; set; }
                
        public long NumberOfIncludedCounters { get; set; }
        public long SizeOfIncludedCountersInBytes { get; set; }

        public long NumberOfIncludedTimeSeriesEntries { get; set; }
        public long SizeOfIncludedTimeSeriesInBytes { get; set; }

        public string Exception { get; set; }
    }
}
