﻿// -----------------------------------------------------------------------
//  <copyright file="PrefetchingBehavior.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Config;
using Raven.Database.Indexing;

namespace Raven.Database.Prefetching
{
	using Util;

	public class PrefetchingBehavior : IDisposable, ILowMemoryHandler
	{
		private class DocAddedAfterCommit
		{
			public Etag Etag;
			public DateTime AddedAt;
		}

		private static readonly ILog log = LogManager.GetCurrentClassLogger();
		private readonly BaseBatchSizeAutoTuner autoTuner;
		private readonly WorkContext context;
		private readonly ConcurrentDictionary<string, HashSet<Etag>> documentsToRemove =
			new ConcurrentDictionary<string, HashSet<Etag>>(StringComparer.InvariantCultureIgnoreCase);

		private readonly ReaderWriterLockSlim updatedDocumentsLock = new ReaderWriterLockSlim();
		private readonly SortedKeyList<Etag> updatedDocuments = new SortedKeyList<Etag>();

		private readonly ConcurrentDictionary<Etag, FutureIndexBatch> futureIndexBatches =
			new ConcurrentDictionary<Etag, FutureIndexBatch>();

		private readonly ConcurrentJsonDocumentSortedList prefetchingQueue = new ConcurrentJsonDocumentSortedList();

		private DocAddedAfterCommit lowestInMemoryDocumentAddedAfterCommit;
		private int currentIndexingAge;

		public PrefetchingBehavior(PrefetchingUser prefetchingUser, WorkContext context, BaseBatchSizeAutoTuner autoTuner)
		{
			this.context = context;
			this.autoTuner = autoTuner;
			PrefetchingUser = prefetchingUser;
			MemoryStatistics.RegisterLowMemoryHandler(this);
		}

		public PrefetchingUser PrefetchingUser { get; private set; }

		public string AdditionalInfo { get; set; }

		public bool DisableCollectingDocumentsAfterCommit { get; set; }
		public bool ShouldHandleUnusedDocumentsAddedAfterCommit { get; set; }

		public int InMemoryIndexingQueueSize
		{
			get { return prefetchingQueue.Count; }
		}

		#region IDisposable Members

		public void Dispose()
		{
			Task.WaitAll(futureIndexBatches.Values.Select(ObserveDiscardedTask).ToArray());
			futureIndexBatches.Clear();
		}

		#endregion

		public IDisposable DocumentBatchFrom(Etag etag, out List<JsonDocument> documents)
		{
			documents = GetDocumentsBatchFrom(etag);
			return UpdateCurrentlyUsedBatches(documents);
		}

		public List<JsonDocument> GetDocumentsBatchFrom(Etag etag, int? take = null)
		{
			HandleCollectingDocumentsAfterCommit(etag);

			var results = GetDocsFromBatchWithPossibleDuplicates(etag, take);
			// a single doc may appear multiple times, if it was updated while we were fetching things, 
			// so we have several versions of the same doc loaded, this will make sure that we will only  
			// take one of them.
			var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
			for (int i = results.Count - 1; i >= 0; i--)
			{
				if(CanBeConsideredAsDuplicate(results[i]) && ids.Add(results[i].Key) == false)
				{
					results.RemoveAt(i);
				}
			}
			return results;
		}

		private void HandleCollectingDocumentsAfterCommit(Etag reqestedEtag)
		{
			if(ShouldHandleUnusedDocumentsAddedAfterCommit == false)
				return;

			if (DisableCollectingDocumentsAfterCommit)
			{
				if (lowestInMemoryDocumentAddedAfterCommit != null && reqestedEtag.CompareTo(lowestInMemoryDocumentAddedAfterCommit.Etag) > 0)
				{
					lowestInMemoryDocumentAddedAfterCommit = null;
					DisableCollectingDocumentsAfterCommit = false;
				}
			}
			else
			{
				if (lowestInMemoryDocumentAddedAfterCommit != null && SystemTime.UtcNow - lowestInMemoryDocumentAddedAfterCommit.AddedAt > TimeSpan.FromMinutes(10))
				{
					DisableCollectingDocumentsAfterCommit = true;
				}
			}
		}

		private void HandleCleanupOfUnusedDocumentsInQueue()
		{
			if (ShouldHandleUnusedDocumentsAddedAfterCommit == false)
				return;

			if(DisableCollectingDocumentsAfterCommit == false)
				return;

			if(lowestInMemoryDocumentAddedAfterCommit == null)
				return;

			prefetchingQueue.RemoveAfter(lowestInMemoryDocumentAddedAfterCommit.Etag);
		}

		private bool CanBeConsideredAsDuplicate(JsonDocument document)
		{
			if (document.Metadata[Constants.RavenReplicationConflict] != null)
				return false;

			return true;
		}

		public bool CanUsePrefetcherToLoadFrom(Etag fromEtag)
		{
			var nextEtagToIndex = GetNextDocEtag(fromEtag);
			var firstEtagInQueue = prefetchingQueue.NextDocumentETag();

			if (firstEtagInQueue == null) // queue is empty, let it use this prefetcher
				return true;

			if (nextEtagToIndex == firstEtagInQueue) // docs for requested etag are already in queue
				return true;

			if (CanLoadDocumentsFromFutureBatches(nextEtagToIndex))
				return true;

			return false;
		}

		private List<JsonDocument> GetDocsFromBatchWithPossibleDuplicates(Etag etag, int? take)
		{
			var result = new List<JsonDocument>();
			bool docsLoaded;
			int prefetchingQueueSizeInBytes;
			var prefetchingDurationTimer = Stopwatch.StartNew();
			do
			{
				var nextEtagToIndex = GetNextDocEtag(etag);
				var firstEtagInQueue = prefetchingQueue.NextDocumentETag();

				if (nextEtagToIndex != firstEtagInQueue)
				{
					if (TryLoadDocumentsFromFutureBatches(nextEtagToIndex) == false)
					{
						LoadDocumentsFromDisk(etag, firstEtagInQueue); // here we _intentionally_ use the current etag, not the next one
					}
				}

				docsLoaded = TryGetDocumentsFromQueue(nextEtagToIndex, result, take);

				if (docsLoaded)
					etag = result[result.Count - 1].Etag;

				prefetchingQueueSizeInBytes = prefetchingQueue.LoadedSize;
			 } while (result.Count < autoTuner.NumberOfItemsToProcessInSingleBatch && (take.HasValue == false || result.Count < take.Value) && docsLoaded &&
						prefetchingDurationTimer.ElapsedMilliseconds <= context.Configuration.PrefetchingDurationLimit &&
						((prefetchingQueueSizeInBytes + autoTuner.CurrentlyUsedBatchSizesInBytes.Values.Sum()) < (context.Configuration.MemoryLimitForProcessingInMb * 1024 * 1024)));
			
			return result;
		}

		private void LoadDocumentsFromDisk(Etag etag, Etag untilEtag)
		{
			var jsonDocs = GetJsonDocsFromDisk(etag, untilEtag);
			
            using(prefetchingQueue.EnterWriteLock())
			foreach (var jsonDocument in jsonDocs)
				prefetchingQueue.Add(jsonDocument);
			}

		private bool TryGetDocumentsFromQueue(Etag nextDocEtag, List<JsonDocument> items, int? take)
		{
			JsonDocument result;

			nextDocEtag = HandleEtagGapsIfNeeded(nextDocEtag);
			bool hasDocs = false;

			while (items.Count < autoTuner.NumberOfItemsToProcessInSingleBatch && 
				prefetchingQueue.TryPeek(out result) && 
				// we compare to current or _smaller_ so we will remove from the queue old versions
				// of documents that we have already loaded
				nextDocEtag.CompareTo(result.Etag) >= 0)
			{
				// safe to do peek then dequeue because we are the only one doing the dequeues
				// and here we are single threaded, but still, better to check
				if (prefetchingQueue.TryDequeue(out result) == false)
					continue;

                // this shouldn't happen, but... 
                if(result == null)
                    continue;

				if (result.Etag != nextDocEtag)
					continue;

				items.Add(result);
				hasDocs = true;

				if (take.HasValue && items.Count >= take.Value)
					break;

				nextDocEtag = Abstractions.Util.EtagUtil.Increment(nextDocEtag, 1);
				nextDocEtag = HandleEtagGapsIfNeeded(nextDocEtag);
			}

			return hasDocs;
		}

		public IEnumerable<JsonDocument> DebugGetDocumentsInPrefetchingQueue()
		{
			return prefetchingQueue.Clone();
		}

		public List<object> DebugGetDocumentsInFutureBatches()
		{
			var result = new List<object>();

			foreach (var futureBatch in futureIndexBatches)
			{
				if (futureBatch.Value.Task.IsCompleted == false)
				{
					result.Add(new
					{
						FromEtag = futureBatch.Key,
						Docs = "Loading documents from disk in progress"
					});

					continue;
				}

				var docs = futureBatch.Value.Task.Result;

				var take = Math.Min(5, docs.Count);

				var etagsWithKeysTail = Enumerable.Range(0, take).Select(
					i => docs[docs.Count - take + i]).ToDictionary(x => x.Etag, x => x.Key);

				result.Add(new
				{
					FromEtag = futureBatch.Key,
					EtagsWithKeysHead = docs.Take(5).ToDictionary(x => x.Etag, x => x.Key),
					EtagsWithKeysTail = etagsWithKeysTail,
					TotalDocsCount = docs.Count
				});
			}

			return result;
		}

		private bool CanLoadDocumentsFromFutureBatches(Etag nextDocEtag)
		{
			if (context.Configuration.DisableDocumentPreFetching)
				return false;

			FutureIndexBatch batch;
			if (futureIndexBatches.TryGetValue(nextDocEtag, out batch) == false)
				return false;

			if (Task.CurrentId == batch.Task.Id)
				return false;

			return true;
		}

		private bool TryLoadDocumentsFromFutureBatches(Etag nextDocEtag)
		{
			try
			{
				if (CanLoadDocumentsFromFutureBatches(nextDocEtag) == false)
					return false;

				FutureIndexBatch nextBatch;
				if (futureIndexBatches.TryRemove(nextDocEtag, out nextBatch) == false) // here we need to remove the batch
					return false;

                List<JsonDocument> jsonDocuments = nextBatch.Task.Result;
                using (prefetchingQueue.EnterWriteLock())
                {
                    foreach (var jsonDocument in jsonDocuments)
                        prefetchingQueue.Add(jsonDocument);
                }

			    return true;
			}
			catch (Exception e)
			{
				log.WarnException("Error when getting next batch value asynchronously, will try in sync manner", e);
				return false;
			}
		}

		private List<JsonDocument> GetJsonDocsFromDisk(Etag etag, Etag untilEtag)
		{
			List<JsonDocument> jsonDocs = null;

			context.TransactionalStorage.Batch(actions =>
			{
			    //limit how much data we load from disk --> better adhere to memory limits
			    var totalSizeAllowedToLoadInBytes =
			        (context.Configuration.MemoryLimitForProcessingInMb*1024*1024) -
			        (prefetchingQueue.LoadedSize + autoTuner.CurrentlyUsedBatchSizesInBytes.Values.Sum());

                // at any rate, we will load a min of 512Kb docs
			    var maxSize = Math.Max(
			        Math.Min(totalSizeAllowedToLoadInBytes, autoTuner.MaximumSizeAllowedToFetchFromStorageInBytes),
			        1024*512);

				jsonDocs = actions.Documents
					.GetDocumentsAfter(
						etag,
						autoTuner.NumberOfItemsToProcessInSingleBatch,
						context.CancellationToken,
						maxSize,
						untilEtag,
						autoTuner.FetchingDocumentsFromDiskTimeout
					)
					.Where(x => x != null)
					.Select(doc =>
					{
                        JsonDocument.EnsureIdInMetadata(doc);
						return doc;
					})
					.ToList();
			});

			if (untilEtag == null)
			{
				MaybeAddFutureBatch(jsonDocs);
			}
			return jsonDocs;
		}

		private void MaybeAddFutureBatch(List<JsonDocument> past)
		{
			if (context.Configuration.DisableDocumentPreFetching || context.RunIndexing == false)
				return;
			if (context.Configuration.MaxNumberOfParallelProcessingTasks == 1)
				return;
			if (past.Count == 0)
				return;
		    if (prefetchingQueue.LoadedSize > autoTuner.MaximumSizeAllowedToFetchFromStorageInBytes)
		        return; // already have too much in memory
            // don't keep _too_ much in memory
		    if (prefetchingQueue.Count > context.Configuration.MaxNumberOfItemsToProcessInSingleBatch * 2)
		        return;

		    var size = 1024;
		    var count = context.LastActualIndexingBatchInfo.Count;
		    if (count > 0)
		    {
		        size = context.LastActualIndexingBatchInfo.Aggregate(0, (o, c) => o + c.TotalDocumentCount)/count;
		    }
			var alreadyLoadedSize = futureIndexBatches.Values.Sum(x =>
			{
				if (x.Task.IsCompleted)
					return x.Task.Result.Sum(doc => doc.SerializedSizeOnDisk);

			    return size;
			});

			if (alreadyLoadedSize > context.Configuration.AvailableMemoryForRaisingBatchSizeLimit)
				return;

			if(MemoryStatistics.IsLowMemory)
				return;
			if (futureIndexBatches.Count > 5) // we limit the number of future calls we do
			{
				int alreadyLoaded = futureIndexBatches.Values.Sum(x =>
				{
					if (x.Task.IsCompleted)
						return x.Task.Result.Count;
					return autoTuner.NumberOfItemsToProcessInSingleBatch / 4 * 3;
				});

				if (alreadyLoaded > autoTuner.NumberOfItemsToProcessInSingleBatch)
					return;
			}

			// ensure we don't do TOO much future caching
			if (MemoryStatistics.AvailableMemory <
				context.Configuration.AvailableMemoryForRaisingBatchSizeLimit)
				return;

			// we loaded the maximum amount, there are probably more items to read now.
			Etag highestLoadedEtag = GetHighestEtag(past);
			Etag nextEtag = GetNextDocumentEtagFromDisk(highestLoadedEtag);

			if (nextEtag == highestLoadedEtag)
				return; // there is nothing newer to do 

			if (futureIndexBatches.ContainsKey(nextEtag)) // already loading this
				return;

			var futureBatchStat = new FutureBatchStats
			{
				Timestamp = SystemTime.UtcNow,
				PrefetchingUser = PrefetchingUser
			};
			Stopwatch sp = Stopwatch.StartNew();
			context.AddFutureBatch(futureBatchStat);
			futureIndexBatches.TryAdd(nextEtag, new FutureIndexBatch
			{
				StartingEtag = nextEtag,
				Age = Interlocked.Increment(ref currentIndexingAge),
				Task = Task.Factory.StartNew(() =>
				{
					List<JsonDocument> jsonDocuments = null;
					int localWork = 0;
					while (context.RunIndexing)
					{
						jsonDocuments = GetJsonDocsFromDisk(Abstractions.Util.EtagUtil.Increment(nextEtag, -1), null);
						if (jsonDocuments.Count > 0)
							break;

						futureBatchStat.Retries++;

						context.WaitForWork(TimeSpan.FromMinutes(10), ref localWork, "PreFetching");
					}
					futureBatchStat.Duration = sp.Elapsed;
					futureBatchStat.Size = jsonDocuments == null ? 0 : jsonDocuments.Count;
					if (jsonDocuments != null)
					{
						MaybeAddFutureBatch(jsonDocuments);
					}
					return jsonDocuments;
				})
			});
		}

		private Etag GetNextDocEtag(Etag etag)
		{
			var oneUpEtag = Abstractions.Util.EtagUtil.Increment(etag, 1);

			// no need to go to disk to find the next etag if we already have it in memory
			if (prefetchingQueue.NextDocumentETag() == oneUpEtag)
				return oneUpEtag;

			return GetNextDocumentEtagFromDisk(etag);
		}

		private Etag GetNextDocumentEtagFromDisk(Etag etag)
		{
			Etag nextDocEtag = null;
			context.TransactionalStorage.Batch(
				accessor => { nextDocEtag = accessor.Documents.GetBestNextDocumentEtag(etag); });

			return nextDocEtag;
		}

		private static Etag GetHighestEtag(List<JsonDocument> past)
		{
			JsonDocument jsonDocument = GetHighestJsonDocumentByEtag(past);
			if (jsonDocument == null)
				return Etag.Empty;
			return jsonDocument.Etag ?? Etag.Empty;
		}

		public static JsonDocument GetHighestJsonDocumentByEtag(List<JsonDocument> past)
		{
			var highest = Etag.Empty;
			JsonDocument highestDoc = null;
			for (int i = past.Count - 1; i >= 0; i--)
			{
				Etag etag = past[i].Etag;
				if (highest.CompareTo(etag) > 0)
				{
					continue;
				}
				highest = etag;
				highestDoc = past[i];
			}
			return highestDoc;
		}

		private static Task ObserveDiscardedTask(FutureIndexBatch source)
		{
			return source.Task.ContinueWith(task =>
			{
				if (task.Exception != null)
				{
					log.WarnException("Error happened on discarded future work batch", task.Exception);
				}
				else
				{
					log.Warn("WASTE: Discarding future work item without using it, to reduce memory usage");
				}
			});
		}

		public void BatchProcessingComplete()
		{
			int indexingAge = Interlocked.Increment(ref currentIndexingAge);

			// make sure that we don't have too much "future cache" items
			const int numberOfIndexingGenerationsAllowed = 64;
			foreach (FutureIndexBatch source in futureIndexBatches.Values.Where(x => (indexingAge - x.Age) > numberOfIndexingGenerationsAllowed).ToList())
			{
				ObserveDiscardedTask(source);
				FutureIndexBatch batch;
				futureIndexBatches.TryRemove(source.StartingEtag, out batch);
			}
		}

		public void AfterStorageCommitBeforeWorkNotifications(JsonDocument[] docs)
		{
			if (context.Configuration.DisableDocumentPreFetching || docs.Length == 0 || DisableCollectingDocumentsAfterCommit)
				return;

			if (prefetchingQueue.Count >= // don't use too much, this is an optimization and we need to be careful about using too much mem
				context.Configuration.MaxNumberOfItemsToPreFetch ||
                prefetchingQueue.LoadedSize > context.Configuration.AvailableMemoryForRaisingBatchSizeLimit)
				return;

			Etag lowestEtag = null;

		    using (prefetchingQueue.EnterWriteLock())
		    {
		        foreach (var jsonDocument in docs)
		        {
		            JsonDocument.EnsureIdInMetadata(jsonDocument);
		            prefetchingQueue.Add(jsonDocument);

		            if (ShouldHandleUnusedDocumentsAddedAfterCommit && (lowestEtag == null || jsonDocument.Etag.CompareTo(lowestEtag) < 0))
		            {
		                lowestEtag = jsonDocument.Etag;
		            }
		        }
		    }

		    if (ShouldHandleUnusedDocumentsAddedAfterCommit && lowestEtag != null)
			{
				if (lowestInMemoryDocumentAddedAfterCommit == null || lowestEtag.CompareTo(lowestInMemoryDocumentAddedAfterCommit.Etag) < 0)
				{
					lowestInMemoryDocumentAddedAfterCommit = new DocAddedAfterCommit
					{
						Etag = lowestEtag,
						AddedAt = SystemTime.UtcNow
					};
				}
			}
		}

		public void CleanupDocuments(Etag lastIndexedEtag)
		{
			foreach (var docToRemove in documentsToRemove)
			{
				if (docToRemove.Value.All(etag => lastIndexedEtag.CompareTo(etag) > 0) == false)
					continue;

				HashSet<Etag> _;
				documentsToRemove.TryRemove(docToRemove.Key, out _);
			}

			updatedDocumentsLock.EnterWriteLock();
			try
			{
				updatedDocuments.RemoveSmallerOrEqual(lastIndexedEtag);
			}
			finally
			{
				updatedDocumentsLock.ExitWriteLock();
			}

			JsonDocument result;
			while (prefetchingQueue.TryPeek(out result) && lastIndexedEtag.CompareTo(result.Etag) >= 0)
			{
				prefetchingQueue.TryDequeue(out result);
			}

			HandleCleanupOfUnusedDocumentsInQueue();
		}

		public bool FilterDocuments(JsonDocument document)
		{
			HashSet<Etag> etags;
			return (documentsToRemove.TryGetValue(document.Key, out etags) && etags.Any(x => x.CompareTo(document.Etag) >= 0)) == false;
		}

		public void AfterDelete(string key, Etag deletedEtag)
		{
			documentsToRemove.AddOrUpdate(key, s => new HashSet<Etag> { deletedEtag },
										  (s, set) => new HashSet<Etag>(set) { deletedEtag });
		}

		public void AfterUpdate(string key, Etag etagBeforeUpdate)
		{
			updatedDocumentsLock.EnterWriteLock();
			try
			{
			    updatedDocuments.Add(etagBeforeUpdate);
			}
			finally
			{
				updatedDocumentsLock.ExitWriteLock();
			}
		}

		public bool ShouldSkipDeleteFromIndex(JsonDocument item)
		{
			if (item.SkipDeleteFromIndex == false)
				return false;
			return documentsToRemove.ContainsKey(item.Key) == false;
		}

		private Etag HandleEtagGapsIfNeeded(Etag nextEtag)
		{
			if (nextEtag != prefetchingQueue.NextDocumentETag())
			{
				var etag = SkipDeletedEtags(nextEtag);
				etag = SkipUpdatedEtags(etag);

				if (etag == nextEtag)
					etag = GetNextDocumentEtagFromDisk(nextEtag.IncrementBy(-1));

				nextEtag = etag;
			}

			return nextEtag;
		}

		private Etag SkipDeletedEtags(Etag nextEtag)
		{
			while (documentsToRemove.Any(x => x.Value.Contains(nextEtag)))
			{
				nextEtag = Abstractions.Util.EtagUtil.Increment(nextEtag, 1);
			}

			return nextEtag;
		}

		private Etag SkipUpdatedEtags(Etag nextEtag)
		{
			updatedDocumentsLock.EnterReadLock();
			try
			{
				var enumerator = updatedDocuments.GetEnumerator();

				// here we relay on the fact that the updated docs collection is sorted
				while (enumerator.MoveNext() && enumerator.Current.CompareTo(nextEtag) == 0)
				{
					nextEtag = Abstractions.Util.EtagUtil.Increment(nextEtag, 1);
				}
			}
			finally
			{
				updatedDocumentsLock.ExitReadLock();
			}

			return nextEtag;
		}

		#region Nested type: FutureIndexBatch

		private class FutureIndexBatch
		{
			public int Age;
			public Etag StartingEtag;
			public Task<List<JsonDocument>> Task;

		}

		#endregion

		public IDisposable UpdateCurrentlyUsedBatches(List<JsonDocument> docBatch)
		{
			var batchId = Guid.NewGuid();

			autoTuner.CurrentlyUsedBatchSizesInBytes.TryAdd(batchId, docBatch.Sum(x => x.SerializedSizeOnDisk));
			return new DisposableAction(() =>
			{
				long _;
                autoTuner.CurrentlyUsedBatchSizesInBytes.TryRemove(batchId, out _);
			});
		}

		public void UpdateAutoThrottler(List<JsonDocument> jsonDocs, TimeSpan indexingDuration)
		{
			int currentBatchLength = autoTuner.NumberOfItemsToProcessInSingleBatch;
			int futureLen = futureIndexBatches.Values.Sum(x =>
			{
				if (x.Task.IsCompleted)
				{
					return x.Task.Result.Count;
				}
				return currentBatchLength / 15;
			});

			long futureSize = futureIndexBatches.Values.Sum(x =>
			{
				if (x.Task.IsCompleted)
				{
					var jsonResults = x.Task.Result;
					return jsonResults.Sum(s => (long)s.SerializedSizeOnDisk);
				}
				return currentBatchLength * 256;
			});

			autoTuner.AutoThrottleBatchSize(
				jsonDocs.Count + futureLen, 
				futureSize + jsonDocs.Sum(x => (long)x.SerializedSizeOnDisk),
			    indexingDuration);
		}

        public void OutOfMemoryExceptionHappened()
	    {
	        autoTuner.HandleOutOfMemory();
	    }

		public void HandleLowMemory()
		{
			ClearQueueAndFutureBatches();
		}

		public void ClearQueueAndFutureBatches()
		{
			futureIndexBatches.Clear();
			prefetchingQueue.Clear();
		}
	}
}
