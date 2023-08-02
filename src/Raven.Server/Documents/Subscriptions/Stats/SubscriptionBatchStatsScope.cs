// -----------------------------------------------------------------------
//  <copyright file="SubscriptionBatchStatsScope.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using Raven.Server.Utils.Stats;

namespace Raven.Server.Documents.Subscriptions.Stats
{
    internal sealed class SubscriptionBatchStatsScope : StatsScope<SubscriptionBatchRunStats, SubscriptionBatchStatsScope>
    {
        private readonly SubscriptionBatchRunStats _stats;
        
        public SubscriptionBatchStatsScope(SubscriptionBatchRunStats stats, bool start = true) : base(stats, start)
        {
            _stats = stats;
        }

        protected override SubscriptionBatchStatsScope OpenNewScope(SubscriptionBatchRunStats stats, bool start)
        {
            return new SubscriptionBatchStatsScope(stats, start);
        }
        
        public void RecordBatchInfo(long taskId, string taskName, long connectionId, long batchId)
        {
            _stats.TaskId = taskId;
            _stats.TaskName = taskName;
            
            _stats.ConnectionId = connectionId;
            _stats.BatchId = batchId;
        }
        
        public void RecordDocumentInfo(long documentSize)
        {
            _stats.NumberOfDocuments++;
            _stats.SizeOfDocumentsInBytes += documentSize;
        }
        
        public void RecordIncludedDocumentsInfo(long includedDocumentsCount, long includedDocumentsSize)
        {
            _stats.NumberOfIncludedDocuments += includedDocumentsCount;
            _stats.SizeOfIncludedDocumentsInBytes += includedDocumentsSize;
        }
        
        public void RecordIncludedCountersInfo(long includedCountersCount, long includedCountersSize)
        {
            _stats.NumberOfIncludedCounters += includedCountersCount;
            _stats.SizeOfIncludedCountersInBytes += includedCountersSize;
        }
        
        public void RecordIncludedTimeSeriesInfo(long includedTimeSeriesEntriesCount, long includedTimeSeriesSize)
        {
            _stats.NumberOfIncludedTimeSeriesEntries += includedTimeSeriesEntriesCount;
            _stats.SizeOfIncludedTimeSeriesInBytes += includedTimeSeriesSize;
        }

        public void RecordException(string exceptionMsg)
        {
            _stats.Exception = exceptionMsg;
        }
        
        public SubscriptionBatchPerformanceOperation ToPerformanceOperation(string name)
        {
            var operation = new SubscriptionBatchPerformanceOperation(Duration)
            {
                Name = name
            };

            if (Scopes != null)
            {
                operation.Operations = Scopes
                    .Select(x => x.Value.ToPerformanceOperation(x.Key))
                    .ToArray();
            }

            return operation;
        }

        public long GetBatchSize()
        {
            return _stats.SizeOfDocumentsInBytes;
        }
    }
}
