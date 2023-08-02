﻿using System;
using System.Diagnostics;
using System.Linq;
using Lucene.Net.Index;
using Raven.Client.Documents.Indexes;
using Raven.Client.Util;
using Raven.Server.Documents.Indexes.MapReduce.Exceptions;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Exceptions;
using Raven.Server.Utils.Stats;
using Sparrow.Server.Exceptions;

namespace Raven.Server.Documents.Indexes
{
    internal sealed class IndexingStatsAggregator : StatsAggregator<IndexingRunStats, IndexingStatsScope>
    {
        private volatile IndexingPerformanceStats _performanceStats;

        public IndexingStatsAggregator(int id, IndexingStatsAggregator lastStats) : base(id, lastStats)
        {
        }

        public override IndexingStatsScope CreateScope()
        {
            Debug.Assert(Scope == null);

            return Scope = new IndexingStatsScope(Stats);
        }

        public IndexingPerformanceBasicStats ToIndexingPerformanceLiveStats()
        {
            if (_performanceStats != null)
                return _performanceStats;

            if (Scope == null || Stats == null)
                return null;

            return new IndexingPerformanceBasicStats(Scope.Duration)
            {
                Started = StartTime,
                InputCount = Stats.MapAttempts + Stats.MapReferenceAttempts,
                SuccessCount = Stats.MapSuccesses + Stats.MapReferenceSuccesses,
                FailedCount = Stats.MapErrors + Stats.MapReferenceErrors,
                OutputCount = Stats.IndexingOutputs,
                AllocatedManagedBytes = Stats.AllocatedManagedBytes,
                AllocatedUnmanagedBytes = Stats.AllocatedUnmanagedBytes,
                DocumentsSize = new Size(Stats.DocumentsSize)
            };
        }

        public IndexingPerformanceStats ToIndexingPerformanceLiveStatsWithDetails()
        {
            if (_performanceStats != null)
                return _performanceStats;

            if (Scope == null || Stats == null)
                return null;

            if (Completed)
                return ToIndexingPerformanceStats();

            return CreateIndexingPerformanceStats(completed: false);
        }

        public IndexingPerformanceStats ToIndexingPerformanceStats()
        {
            if (_performanceStats != null)
                return _performanceStats;

            lock (Stats)
            {
                if (_performanceStats != null)
                    return _performanceStats;

                return _performanceStats = CreateIndexingPerformanceStats(completed: true);
            }
        }

        private IndexingPerformanceStats CreateIndexingPerformanceStats(bool completed)
        {
            return new IndexingPerformanceStats(Scope.Duration)
            {
                Id = Id,
                Started = StartTime,
                Completed = completed ? StartTime.Add(Scope.Duration) : (DateTime?)null,
                Details = Scope.ToIndexingPerformanceOperation("Indexing"),
                InputCount = Stats.MapAttempts + Stats.MapReferenceAttempts,
                SuccessCount = Stats.MapSuccesses + Stats.MapReferenceSuccesses,
                FailedCount = Stats.MapErrors + Stats.MapReferenceErrors,
                OutputCount = Stats.IndexingOutputs,
                DocumentsSize = new Size(Stats.DocumentsSize),
                AllocatedManagedBytes = Stats.AllocatedManagedBytes,
                AllocatedUnmanagedBytes = Stats.AllocatedUnmanagedBytes
            };
        }
    }

    internal sealed class IndexingStatsScope : StatsScope<IndexingRunStats, IndexingStatsScope>
    {
        private readonly IndexingRunStats _stats;

        public IndexingStatsScope(IndexingRunStats stats, bool start = true) : base(stats, start)
        {
            _stats = stats;
        }

        protected override IndexingStatsScope OpenNewScope(IndexingRunStats stats, bool start)
        {
            return new IndexingStatsScope(stats, start);
        }

        public long MapAttempts => _stats.MapAttempts;

        public long MapReferenceAttempts => _stats.MapReferenceAttempts;

        public long MapErrors => _stats.MapErrors;

        public long ReduceAttempts => _stats.ReduceAttempts;

        public long ReduceErrors => _stats.ReduceErrors;

        public int ErrorsCount => _stats.Errors?.Count ?? 0;

        public int NumberOfKeptReduceErrors => _stats.NumberOfKeptReduceErrors;

        public long TombstoneDeleteSuccesses => _stats.TombstoneDeleteSuccesses;

        public void SetAllocatedManagedBytes(long sizeInBytes)
        {
            _stats.AllocatedManagedBytes = new Size(sizeInBytes);
        }
        
        public void SetAllocatedUnmanagedBytes(long sizeInBytes)
        {
            _stats.AllocatedUnmanagedBytes = new Size(sizeInBytes);
        }

        public void AddCorruptionError(Exception e)
        {
            _stats.AddCorruptionError(e);
        }

        public void AddWriteError(IndexWriteException iwe)
        {
            _stats.AddWriteError(iwe);
        }

        public void AddUnexpectedError(Exception e)
        {
            _stats.AddUnexpectedError(e);
        }

        public void AddCriticalError(Exception e)
        {
            _stats.AddCriticalError(e);
        }

        public void AddMemoryError(Exception oome)
        {
            _stats.AddMemoryError(oome);
        }

        public void AddAnalyzerError(IndexAnalyzerException iae)
        {
            _stats.AddAnalyzerError(iae);
        }

        public void AddExcessiveNumberOfReduceErrors(ExcessiveNumberOfReduceErrorsException e)
        {
            _stats.AddExcessiveNumberOfReduceErrors(e);
        }

        public void AddDiskFullError(DiskFullException e)
        {
            _stats.AddDiskFullError(e);
        }

        public void AddMapError(string key, string message)
        {
            _stats.AddMapError(key, message);
        }

        public void AddMapReferenceError(string key, string message)
        {
            _stats.AddMapReferenceError(key, message);
        }

        public void AddReduceError(string message, string reduceKey = null)
        {
            _stats.AddReduceError(message, reduceKey);
        }

        public void RecordMapAttempt()
        {
            _stats.MapAttempts++;
        }

        public void RecordMapReferenceAttempt()
        {
            _stats.MapReferenceAttempts++;

            _stats.ReferenceDetails ??= new ReferenceRunDetails();

            _stats.ReferenceDetails.ReferenceAttempts++;
        }

        public void RecordMapSuccess()
        {
            _stats.MapSuccesses++;
        }

        public void RecordMapReferenceSuccess()
        {
            _stats.MapReferenceSuccesses++;

            _stats.ReferenceDetails ??= new ReferenceRunDetails();

            _stats.ReferenceDetails.ReferenceSuccesses++;
        }

        public void RecordTombstoneDeleteSuccess()
        {
            _stats.TombstoneDeleteSuccesses++;

            _stats.CleanupDetails ??= new CleanupRunDetails();

            _stats.CleanupDetails.DeleteSuccesses++;
        }

        public void RecordMapError()
        {
            _stats.MapErrors++;
        }

        public void RecordMapReferenceError()
        {
            _stats.MapReferenceErrors++;

            _stats.ReferenceDetails ??= new ReferenceRunDetails();

            _stats.ReferenceDetails.ReferenceErrors++;
        }

        public void RecordIndexingOutput()
        {
            _stats.IndexingOutputs++;
        }

        public void RecordBatchCompletedReason(IndexingWorkType type, string reason)
        {
            switch (type)
            {
                case IndexingWorkType.Cleanup:
                    RecordCleanupCompletedReason(reason);
                    break;
                case IndexingWorkType.References:
                    RecordReferenceCompletedReason(reason);
                    break;
                case IndexingWorkType.Map:
                    RecordMapCompletedReason(reason);
                    break;
                case IndexingWorkType.None:
                case IndexingWorkType.Reduce:
                    throw new ArgumentOutOfRangeException(nameof(type) + " doesn't support stopping batch.", type, null);
                default:
                    throw new ArgumentOutOfRangeException(nameof(type), type, null);
            }
        }

        private void RecordMapCompletedReason(string reason)
        {
            if (_stats.MapDetails == null)
                _stats.MapDetails = new MapRunDetails();

            _stats.MapDetails.BatchCompleteReason = reason;
        }

        private void RecordReferenceCompletedReason(string reason)
        {
            if (_stats.ReferenceDetails == null)
                _stats.ReferenceDetails = new ReferenceRunDetails();

            _stats.ReferenceDetails.BatchCompleteReason = reason;
        }

        private void RecordCleanupCompletedReason(string reason)
        {
            _stats.CleanupDetails ??= new CleanupRunDetails();

            _stats.CleanupDetails.BatchCompleteReason = reason;
        }

        public void RecordReduceTreePageModified(bool isLeaf)
        {
            if (_stats.ReduceDetails == null)
                _stats.ReduceDetails = new ReduceRunDetails();

            if (_stats.ReduceDetails.TreesReduceDetails == null)
                _stats.ReduceDetails.TreesReduceDetails = new TreesReduceDetails();

            if (isLeaf)
                _stats.ReduceDetails.TreesReduceDetails.NumberOfModifiedLeafs++;
            else
                _stats.ReduceDetails.TreesReduceDetails.NumberOfModifiedBranches++;
        }

        public void RecordCompressedLeafPage()
        {
            if (_stats.ReduceDetails == null)
                _stats.ReduceDetails = new ReduceRunDetails();

            if (_stats.ReduceDetails.TreesReduceDetails == null)
                _stats.ReduceDetails.TreesReduceDetails = new TreesReduceDetails();

            _stats.ReduceDetails.TreesReduceDetails.NumberOfCompressedLeafs++;
        }

        public void RecordReduceAttempts(int numberOfEntries)
        {
            _stats.ReduceAttempts += numberOfEntries;
        }

        public void RecordReduceSuccesses(int numberOfEntries)
        {
            _stats.ReduceSuccesses += numberOfEntries;
        }

        public void RecordReduceErrors(int numberOfEntries)
        {
            _stats.ReduceErrors += numberOfEntries;
        }

        public IndexingPerformanceOperation ToIndexingPerformanceOperation(string name)
        {
            var operation = new IndexingPerformanceOperation(Duration)
            {
                Name = name
            };

            if (_stats.ReduceDetails != null && name == "Reduce")
            {
                operation.ReduceDetails = new ReduceRunDetails
                {
                    ProcessWorkingSet = _stats.ReduceDetails.ProcessWorkingSet,
                    ProcessPrivateMemory = _stats.ReduceDetails.ProcessPrivateMemory,
                    CurrentlyAllocated = _stats.ReduceDetails.CurrentlyAllocated,
                    ReduceAttempts = _stats.ReduceAttempts,
                    ReduceSuccesses = _stats.ReduceSuccesses,
                    ReduceErrors = _stats.ReduceErrors,
                };
            }

            if (_stats.ReduceDetails != null && name == IndexingOperation.Reduce.TreeScope)
            {
                operation.ReduceDetails = new ReduceRunDetails
                {
                    TreesReduceDetails = _stats.ReduceDetails.TreesReduceDetails
                };
            }

            if (_stats.ReferenceDetails != null && name == "References")
                operation.ReferenceDetails = _stats.ReferenceDetails;

            if (_stats.MapDetails != null && name == "Map")
                operation.MapDetails = _stats.MapDetails;

            if (_stats.CleanupDetails != null && name == "Cleanup")
                operation.CleanupDetails = _stats.CleanupDetails;

            if (_stats.LuceneMergeDetails != null && name == IndexingOperation.Lucene.Merge)
                operation.LuceneMergeDetails = _stats.LuceneMergeDetails;

            if (_stats.CommitDetails != null && name == IndexingOperation.Storage.Commit)
                operation.CommitDetails = _stats.CommitDetails;

            if (Scopes != null)
            {
                operation.Operations = Scopes
                    .Select(x => x.Value.ToIndexingPerformanceOperation(x.Key))
                    .ToArray();
            }

            return operation;
        }

        public void RecordMapMemoryStats(long currentProcessWorkingSet, long currentProcessPrivateMemorySize, long currentBudget)
        {
            if (_stats.MapDetails == null)
                _stats.MapDetails = new MapRunDetails();

            _stats.MapDetails.AllocationBudget = currentBudget;
            _stats.MapDetails.ProcessPrivateMemory = currentProcessPrivateMemorySize;
            _stats.MapDetails.ProcessWorkingSet = currentProcessWorkingSet;
        }

        public void RecordMapAllocations(long allocations)
        {
            if (_stats.MapDetails == null)
                _stats.MapDetails = new MapRunDetails();

            _stats.MapDetails.CurrentlyAllocated = allocations;
        }

        public void RecordReduceMemoryStats(long currentProcessWorkingSet, long currentProcessPrivateMemorySize)
        {
            if (_stats.ReduceDetails == null)
                _stats.ReduceDetails = new ReduceRunDetails();

            _stats.ReduceDetails.ProcessWorkingSet = currentProcessWorkingSet;
            _stats.ReduceDetails.ProcessPrivateMemory = currentProcessPrivateMemorySize;
        }

        public void RecordReduceAllocations(long allocations)
        {
            if (_stats.ReduceDetails == null)
                _stats.ReduceDetails = new ReduceRunDetails();

            _stats.ReduceDetails.CurrentlyAllocated = allocations;
        }

        public void RecordCommitStats(int numberOfModifiedPages, int numberOf4KbsWrittenToDisk)
        {
            if (_stats.CommitDetails == null)
                _stats.CommitDetails = new StorageCommitDetails();

            _stats.CommitDetails.NumberOfModifiedPages = numberOfModifiedPages;
            _stats.CommitDetails.NumberOf4KbsWrittenToDisk = numberOf4KbsWrittenToDisk;
        }

        public void RecordNumberOfProducedOutputs(int numberOfOutputs)
        {
            if (numberOfOutputs > _stats.MaxNumberOfOutputsPerDocument)
                _stats.MaxNumberOfOutputsPerDocument = numberOfOutputs;
        }

        public void RecordDocumentSize(int size)
        {
            _stats.DocumentsSize += size;
        }

        public void RecordEntriesCountAfterTxCommit(long entriesCount)
        {
            _stats.EntriesCount = entriesCount;
        }

        public void RecordPendingMergesCount(int pendingMergesCount)
        {
            _stats.LuceneMergeDetails ??= new LuceneMergeDetails();
            _stats.LuceneMergeDetails.TotalMergesCount += pendingMergesCount;
        }

        public void RecordMergeStats(MergePolicy.OneMerge.OneMergeStats mergeStats)
        {
            _stats.LuceneMergeDetails ??= new LuceneMergeDetails();
            _stats.LuceneMergeDetails.MergedFilesCount += mergeStats.NumberOfFiles;
            _stats.LuceneMergeDetails.MergedDocumentsCount += mergeStats.TotalDocuments;
            _stats.LuceneMergeDetails.ExecutedMergesCount++;
        }

        public void RecordReferenceAllocations(long allocated)
        {
            _stats.ReferenceDetails ??= new ReferenceRunDetails();

            _stats.ReferenceDetails.CurrentlyAllocated = allocated;
        }

        public void RecordReferenceMemoryStats(long currentProcessWorkingSet, long currentProcessPrivateMemorySize, long currentBudget)
        {
            _stats.ReferenceDetails ??= new ReferenceRunDetails();

            _stats.ReferenceDetails.AllocationBudget = currentBudget;
            _stats.ReferenceDetails.ProcessPrivateMemory = currentProcessPrivateMemorySize;
            _stats.ReferenceDetails.ProcessWorkingSet = currentProcessWorkingSet;
        }
    }
}
