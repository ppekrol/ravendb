﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Json;
using Raven.Database.Linq;
using Raven.Database.Storage;
using Raven.Database.Tasks;
using Raven.Database.Util;
using Sparrow.Collections;

namespace Raven.Database.Indexing
{
	public class ReducingExecuter : AbstractIndexingExecuter
	{
		public ReducingExecuter(WorkContext context, IndexReplacer indexReplacer)
			: base(context, indexReplacer)
		{
			autoTuner = new ReduceBatchSizeAutoTuner(context);
		}

		protected ReducingPerformanceStats[] HandleReduceForIndex(IndexToWorkOn indexToWorkOn, CancellationToken token)
		{
			var viewGenerator = context.IndexDefinitionStorage.GetViewGenerator(indexToWorkOn.IndexId);
			if (viewGenerator == null)
				return null;

			bool operationCanceled = false;
			var itemsToDelete = new ConcurrentSet<object>();

			var singleStepReduceKeys = new List<string>();
			var multiStepsReduceKeys = new List<string>();

			transactionalStorage.Batch(actions =>
			{
                var mappedResultsInfo = actions.MapReduce.GetReduceTypesPerKeys(indexToWorkOn.IndexId, 
                                                                    context.CurrentNumberOfItemsToReduceInSingleBatch, 
                                                                    context.NumberOfItemsToExecuteReduceInSingleStep, token);

			    foreach (var key in mappedResultsInfo)
			    {
				    token.ThrowIfCancellationRequested();

				    switch (key.OperationTypeToPerform)
				    {
					    case ReduceType.SingleStep:
						    singleStepReduceKeys.Add(key.ReduceKey);
						    break;
					    case ReduceType.MultiStep:
						    multiStepsReduceKeys.Add(key.ReduceKey);
						    break;
				    }
			    }
			});

			currentlyProcessedIndexes.TryAdd(indexToWorkOn.IndexId, indexToWorkOn.Index);

			var performanceStats = new List<ReducingPerformanceStats>();

			try
			{
				if (singleStepReduceKeys.Count > 0)
				{
                    if ( Log.IsDebugEnabled )
                        Log.Debug("SingleStep reduce for keys: {0}", String.Join(", ", singleStepReduceKeys));
                    
					var singleStepStats = SingleStepReduce(indexToWorkOn, singleStepReduceKeys, viewGenerator, itemsToDelete, token);

					performanceStats.Add(singleStepStats);
				}

				if (multiStepsReduceKeys.Count > 0)
				{
                    if ( Log.IsDebugEnabled )
                        Log.Debug("MultiStep reduce for keys: {0}", String.Join(", ", multiStepsReduceKeys));

					var multiStepStats = MultiStepReduce(indexToWorkOn, multiStepsReduceKeys, viewGenerator, itemsToDelete, token);

					performanceStats.Add(multiStepStats);
				}
			}
			catch (OperationCanceledException)
			{
				operationCanceled = true;
			}
			catch (AggregateException e)
			{
				var anyOperationsCanceled = e
					.InnerExceptions
					.OfType<OperationCanceledException>()
					.Any();

				if (anyOperationsCanceled == false) 
					throw;

				operationCanceled = true;
			}
			finally
			{
				var postReducingOperations = new ReduceLevelPeformanceStats
				{
					Level = -1,
					Started = SystemTime.UtcNow
				};

				if (operationCanceled == false)
				{
					var deletingScheduledReductionsDuration = new Stopwatch();
					var storageCommitDuration = new Stopwatch();

					// whatever we succeeded in indexing or not, we have to update this
					// because otherwise we keep trying to re-index failed mapped results
					transactionalStorage.Batch(actions =>
					{
						actions.BeforeStorageCommit += storageCommitDuration.Start;
						actions.AfterStorageCommit += storageCommitDuration.Stop;

						ScheduledReductionInfo latest;

						using (StopwatchScope.For(deletingScheduledReductionsDuration))
						{
							latest = actions.MapReduce.DeleteScheduledReduction(itemsToDelete);
						}

						if (latest == null)
							return;
						actions.Indexing.UpdateLastReduced(indexToWorkOn.IndexId, latest.Etag, latest.Timestamp);
					});

					postReducingOperations.Operations.Add(PerformanceStats.From(IndexingOperation.Reduce_DeleteScheduledReductions, deletingScheduledReductionsDuration.ElapsedMilliseconds));
					postReducingOperations.Operations.Add(PerformanceStats.From(IndexingOperation.StorageCommit, storageCommitDuration.ElapsedMilliseconds));
				}

				postReducingOperations.Completed = SystemTime.UtcNow;
				postReducingOperations.Duration = postReducingOperations.Completed - postReducingOperations.Started;

				performanceStats.Add(new ReducingPerformanceStats(ReduceType.None)
				{
					LevelStats = new List<ReduceLevelPeformanceStats> { postReducingOperations }
				});

				Index _;
				currentlyProcessedIndexes.TryRemove(indexToWorkOn.IndexId, out _);
			}

			return performanceStats.ToArray();
		}

		protected override void UpdateStalenessMetrics(int staleCount)
		{
			context.MetricsCounters.StaleIndexReduces.Update(staleCount);
		}

		protected override bool ShouldSkipIndex(Index index)
		{
			return false;
		}

		private ReducingPerformanceStats MultiStepReduce(IndexToWorkOn index, List<string> keysToReduce, AbstractViewGenerator viewGenerator, ConcurrentSet<object> itemsToDelete, CancellationToken token)
		{
			var needToMoveToMultiStep = new HashSet<string>();
			transactionalStorage.Batch(actions =>
			{
				foreach (var localReduceKey in keysToReduce)
				{
					token.ThrowIfCancellationRequested();

					var lastPerformedReduceType = actions.MapReduce.GetLastPerformedReduceType(index.IndexId, localReduceKey);

					if (lastPerformedReduceType != ReduceType.MultiStep)
						needToMoveToMultiStep.Add(localReduceKey);

					if (lastPerformedReduceType != ReduceType.SingleStep)
						continue;

					// we exceeded the limit of items to reduce in single step
					// now we need to schedule reductions at level 0 for all map results with given reduce key
					var mappedItems = actions.MapReduce.GetMappedBuckets(index.IndexId, localReduceKey, token).ToList();
					foreach (var result in mappedItems.Select(x => new ReduceKeyAndBucket(x, localReduceKey)))
					{
						actions.MapReduce.ScheduleReductions(index.IndexId, 0, result);
					}
				}
			});

			var reducePerformance = new ReducingPerformanceStats(ReduceType.MultiStep);


			for (int i = 0; i < 3; i++)
			{
				var level = i;

				var reduceLevelStats = new ReduceLevelPeformanceStats()
				{
					Level = level,
					Started = SystemTime.UtcNow,
				};

				var reduceParams = new GetItemsToReduceParams(
					index.IndexId,
                    new HashSet<string>(keysToReduce),
					level,
					true,
					itemsToDelete);

				var gettingItemsToReduceDuration = new Stopwatch();
				var scheduleReductionsDuration = new Stopwatch();
				var removeReduceResultsDuration = new Stopwatch();
				var storageCommitDuration = new Stopwatch();

				bool retry = true;
				while (retry && reduceParams.ReduceKeys.Count > 0)
				{
					var reduceBatchAutoThrottlerId = Guid.NewGuid();
					try
					{
						transactionalStorage.Batch(actions =>
						{
							token.ThrowIfCancellationRequested();

							actions.BeforeStorageCommit += storageCommitDuration.Start;
							actions.AfterStorageCommit += storageCommitDuration.Stop;

							var batchTimeWatcher = Stopwatch.StartNew();

							reduceParams.Take = context.CurrentNumberOfItemsToReduceInSingleBatch;

                            int size = 0;                  
          
                            IList<MappedResultInfo> persistedResults;
                            var reduceKeys = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);
							using (StopwatchScope.For(gettingItemsToReduceDuration))
							{
                                persistedResults = actions.MapReduce.GetItemsToReduce(reduceParams, token);                                

                                foreach (var item in persistedResults)
                                {
                                    reduceKeys.Add(item.ReduceKey);
                                    size += item.Size;
                                }
							}

                            if (persistedResults.Count == 0)
							{
								retry = false;
								return;
							}

                            var count = persistedResults.Count;

							autoTuner.CurrentlyUsedBatchSizesInBytes.GetOrAdd(reduceBatchAutoThrottlerId, size);

							if (Log.IsDebugEnabled)
							{
                                if (persistedResults.Count > 0)
                                {
                                    Log.Debug(() => string.Format("Found {0} results for keys [{1}] for index {2} at level {3} in {4}",
                                        persistedResults.Count,
                                        string.Join(", ", persistedResults.Select(x => x.ReduceKey).Distinct()),
                                        index.Index.PublicName, level, batchTimeWatcher.Elapsed));
                                }
								else
                                {
                                    Log.Debug("No reduce keys found for {0}", index.Index.PublicName);
                                }									
							}

							token.ThrowIfCancellationRequested();


                            var requiredReduceNextTimeSet = new HashSet<ReduceKeyAndBucket>(persistedResults.Select(x => new ReduceKeyAndBucket(x.Bucket, x.ReduceKey)), ReduceKeyAndBucketEqualityComparer.Instance);

							using (StopwatchScope.For(removeReduceResultsDuration))
							{
                                foreach (var mappedResultInfo in requiredReduceNextTimeSet)
								{
									token.ThrowIfCancellationRequested();

									actions.MapReduce.RemoveReduceResults(index.IndexId, level + 1, mappedResultInfo.ReduceKey, mappedResultInfo.Bucket);
								}
							}

							if (level != 2)
							{
                                var reduceKeysAndBucketsSet = new HashSet<ReduceKeyAndBucket>(requiredReduceNextTimeSet.Select(x => new ReduceKeyAndBucket(x.Bucket / 1024, x.ReduceKey)), ReduceKeyAndBucketEqualityComparer.Instance);

								using (StopwatchScope.For(scheduleReductionsDuration))
								{
                                    foreach (var reduceKeysAndBucket in reduceKeysAndBucketsSet)
									{
										token.ThrowIfCancellationRequested();

										actions.MapReduce.ScheduleReductions(index.IndexId, level + 1, reduceKeysAndBucket);
									}
								}
							}

							token.ThrowIfCancellationRequested();

							var reduceTimeWatcher = Stopwatch.StartNew();

						    var results = persistedResults.Where(x => x.Data != null)
						        .GroupBy(x => x.Bucket, x => JsonToExpando.Convert(x.Data))
						        .ToList();                            

                            var performance = context.IndexStorage.Reduce(index.IndexId, viewGenerator, results, level, context, actions, reduceKeys, persistedResults.Count);

                            context.MetricsCounters.ReducedPerSecond.Mark(results.Count());

							reduceLevelStats.Add(performance);

							var batchDuration = batchTimeWatcher.Elapsed;

                            if ( Log.IsDebugEnabled )
                            {
                                Log.Debug("Indexed {0} reduce keys in {1} with {2} results for index {3} in {4} on level {5}", reduceKeys.Count, batchDuration, performance.ItemsCount, index.Index.PublicName, reduceTimeWatcher.Elapsed, level);
                            }

							autoTuner.AutoThrottleBatchSize(count, size, batchDuration);
						});
					}
					finally
					{
						long _;
						autoTuner.CurrentlyUsedBatchSizesInBytes.TryRemove(reduceBatchAutoThrottlerId, out _);
					}
				}

				reduceLevelStats.Completed = SystemTime.UtcNow;
				reduceLevelStats.Duration = reduceLevelStats.Completed - reduceLevelStats.Started;

				reduceLevelStats.Operations.Add(PerformanceStats.From(IndexingOperation.Reduce_GetItemsToReduce, gettingItemsToReduceDuration.ElapsedMilliseconds));
				reduceLevelStats.Operations.Add(PerformanceStats.From(IndexingOperation.Reduce_ScheduleReductions, scheduleReductionsDuration.ElapsedMilliseconds));
				reduceLevelStats.Operations.Add(PerformanceStats.From(IndexingOperation.Reduce_RemoveReduceResults, removeReduceResultsDuration.ElapsedMilliseconds));
				reduceLevelStats.Operations.Add(PerformanceStats.From(IndexingOperation.StorageCommit, storageCommitDuration.ElapsedMilliseconds));

				reducePerformance.LevelStats.Add(reduceLevelStats);
			}

			foreach (var reduceKey in needToMoveToMultiStep)
			{
				token.ThrowIfCancellationRequested();

				string localReduceKey = reduceKey;
				transactionalStorage.Batch(actions =>
										   actions.MapReduce.UpdatePerformedReduceType(index.IndexId, localReduceKey,
																					   ReduceType.MultiStep));
			}

			return reducePerformance;
		}

		private ReducingPerformanceStats SingleStepReduce(IndexToWorkOn index, List<string> keysToReduce, AbstractViewGenerator viewGenerator,
												          ConcurrentSet<object> itemsToDelete, CancellationToken token)
		{
			var needToMoveToSingleStepQueue = new ConcurrentQueue<HashSet<string>>();

            if ( Log.IsDebugEnabled )
			    Log.Debug(() => string.Format("Executing single step reducing for {0} keys [{1}]", keysToReduce.Count, string.Join(", ", keysToReduce)));

			var batchTimeWatcher = Stopwatch.StartNew();

			var reducingBatchThrottlerId = Guid.NewGuid();
			var reducePerformanceStats = new ReducingPerformanceStats(ReduceType.SingleStep);
			var reduceLevelStats = new ReduceLevelPeformanceStats
			{
				Started = SystemTime.UtcNow,
				Level = 2
			};

			try
			{
				var parallelOperations = new ConcurrentQueue<ParallelBatchStats>();

				var parallelProcessingStart = SystemTime.UtcNow;

				BackgroundTaskExecuter.Instance.ExecuteAllBuffered(context, keysToReduce, enumerator =>
				{
					var parallelStats = new ParallelBatchStats
					{
						StartDelay = (long)(SystemTime.UtcNow - parallelProcessingStart).TotalMilliseconds
					};

					var localNeedToMoveToSingleStep = new HashSet<string>();
					needToMoveToSingleStepQueue.Enqueue(localNeedToMoveToSingleStep);
					var localKeys = new HashSet<string>();
					while (enumerator.MoveNext())
					{
						token.ThrowIfCancellationRequested();

						localKeys.Add(enumerator.Current);
					}

					transactionalStorage.Batch(actions =>
					{
						var getItemsToReduceParams = new GetItemsToReduceParams(index: index.IndexId, reduceKeys: localKeys, level: 0, loadData: false, itemsToDelete: itemsToDelete)
						{
							Take = int.MaxValue // just get all, we do the rate limit when we load the number of keys to reduce, anyway
						};

						var getItemsToReduceDuration = Stopwatch.StartNew();

                        int scheduledItemsSum = 0;
                        int scheduledItemsCount = 0;
                        List<int> scheduledItemsMappedBuckets = new List<int>();
						using (StopwatchScope.For(getItemsToReduceDuration))
						{
                            foreach (var item in actions.MapReduce.GetItemsToReduce(getItemsToReduceParams, token))
                            {
                                scheduledItemsMappedBuckets.Add(item.Bucket);
                                scheduledItemsSum += item.Size;
                                scheduledItemsCount++;
                            }
						}

						parallelStats.Operations.Add(PerformanceStats.From(IndexingOperation.Reduce_GetItemsToReduce, getItemsToReduceDuration.ElapsedMilliseconds));

						autoTuner.CurrentlyUsedBatchSizesInBytes.GetOrAdd(reducingBatchThrottlerId, scheduledItemsSum);

                        if (scheduledItemsCount == 0)
						{						    
							// Here we have an interesting issue. We have scheduled reductions, because GetReduceTypesPerKeys() returned them
							// and at the same time, we don't have any at level 0. That probably means that we have them at level 1 or 2.
							// They shouldn't be here, and indeed, we remove them just a little down from here in this function.
							// That said, they might have smuggled in between versions, or something happened to cause them to be here.
							// In order to avoid that, we forcibly delete those extra items from the scheduled reductions, and move on

                            Log.Warn("Found single reduce items ({0}) that didn't have any items to reduce. Deleting level 1 & level 2 items for those keys. (If you can reproduce this, please contact support@ravendb.net)", string.Join(", ", keysToReduce));

							var deletingScheduledReductionsDuration = Stopwatch.StartNew();

							using (StopwatchScope.For(deletingScheduledReductionsDuration))
							{
								foreach (var reduceKey in keysToReduce)
								{
									token.ThrowIfCancellationRequested();

									actions.MapReduce.DeleteScheduledReduction(index.IndexId, 1, reduceKey);
									actions.MapReduce.DeleteScheduledReduction(index.IndexId, 2, reduceKey);
								}
							}

							parallelStats.Operations.Add(PerformanceStats.From(IndexingOperation.Reduce_DeleteScheduledReductions, deletingScheduledReductionsDuration.ElapsedMilliseconds));
						}

						var removeReduceResultsDuration = new Stopwatch();

						foreach (var reduceKey in localKeys)
						{
							token.ThrowIfCancellationRequested();

							var lastPerformedReduceType = actions.MapReduce.GetLastPerformedReduceType(index.IndexId, reduceKey);

							if (lastPerformedReduceType != ReduceType.SingleStep)
								localNeedToMoveToSingleStep.Add(reduceKey);

							if (lastPerformedReduceType != ReduceType.MultiStep)
								continue;

                            if ( Log.IsDebugEnabled )
                            {
                                Log.Debug("Key {0} was moved from multi step to single step reduce, removing existing reduce results records", reduceKey);
                            }

							using (StopwatchScope.For(removeReduceResultsDuration))
							{
                                // now we are in single step but previously multi step reduce was performed for the given key
                                var mappedBuckets = actions.MapReduce.GetMappedBuckets(index.IndexId, reduceKey, token);        

                                // add scheduled items too to be sure we will delete reduce results of already deleted documents
								foreach (var mappedBucket in mappedBuckets.Union(scheduledItemsMappedBuckets))
								{
									actions.MapReduce.RemoveReduceResults(index.IndexId, 1, reduceKey, mappedBucket);
									actions.MapReduce.RemoveReduceResults(index.IndexId, 2, reduceKey, mappedBucket / 1024);
								}
							}
						}

						parallelStats.Operations.Add(PerformanceStats.From(IndexingOperation.Reduce_RemoveReduceResults, removeReduceResultsDuration.ElapsedMilliseconds));

						parallelOperations.Enqueue(parallelStats);
					});
				});

				reduceLevelStats.Operations.Add(new ParallelPerformanceStats
				{
					NumberOfThreads = parallelOperations.Count,
					DurationMs = (long)(SystemTime.UtcNow - parallelProcessingStart).TotalMilliseconds,
					BatchedOperations = parallelOperations.ToList()
				});

				var getMappedResultsDuration = new Stopwatch();				

				var reductionPerformanceStats = new List<IndexingPerformanceStats>();
                
                var keysLeftToReduce = new HashSet<string>(keysToReduce);                                              
				while (keysLeftToReduce.Count > 0)
				{
                    var keysReturned = new HashSet<string>();       

                    // Try to diminish the allocations happening because of .Resize()
                    var mappedResults = new List<MappedResultInfo>(keysLeftToReduce.Count);                             
                    
                    context.TransactionalStorage.Batch(actions =>
					{
						var take = context.CurrentNumberOfItemsToReduceInSingleBatch;

						using (StopwatchScope.For(getMappedResultsDuration))
						{
                            mappedResults = actions.MapReduce.GetMappedResults(index.IndexId, keysLeftToReduce, true, take, keysReturned, token, mappedResults);
						}
					});

					var count = mappedResults.Count;

                    int size = 0;
                    foreach ( var item in mappedResults )
                    {
                        item.Bucket = 0;
                        size += item.Size;
                    }
                        
                    var results = mappedResults.GroupBy(x => x.Bucket, x => JsonToExpando.Convert(x.Data)).ToArray();

					context.MetricsCounters.ReducedPerSecond.Mark(results.Length);

					token.ThrowIfCancellationRequested();

                    var performance = context.IndexStorage.Reduce(index.IndexId, viewGenerator, results, 2, context, null, keysReturned, count);

					reductionPerformanceStats.Add(performance);

					autoTuner.AutoThrottleBatchSize(count, size, batchTimeWatcher.Elapsed);
				}

				var needToMoveToSingleStep = new HashSet<string>();

				HashSet<string> set;
				while (needToMoveToSingleStepQueue.TryDequeue(out set))
				{
					needToMoveToSingleStep.UnionWith(set);
				}

				foreach (var reduceKey in needToMoveToSingleStep)
				{
					string localReduceKey = reduceKey;
					transactionalStorage.Batch(actions =>
						actions.MapReduce.UpdatePerformedReduceType(index.IndexId, localReduceKey, ReduceType.SingleStep));
				}

				reduceLevelStats.Completed = SystemTime.UtcNow;
				reduceLevelStats.Duration = reduceLevelStats.Completed - reduceLevelStats.Started;
				reduceLevelStats.Operations.Add(PerformanceStats.From(IndexingOperation.Reduce_GetMappedResults, getMappedResultsDuration.ElapsedMilliseconds));
				reduceLevelStats.Operations.Add(PerformanceStats.From(IndexingOperation.StorageCommit, 0)); // in single step we write directly to Lucene index

				foreach (var stats in reductionPerformanceStats)
				{
					reduceLevelStats.Add(stats);
				}

				reducePerformanceStats.LevelStats.Add(reduceLevelStats);
			}
			finally
			{
				long _;
				autoTuner.CurrentlyUsedBatchSizesInBytes.TryRemove(reducingBatchThrottlerId, out _);
			}

			return reducePerformanceStats;
		}

		protected override bool IsIndexStale(IndexStats indexesStat, IStorageActionsAccessor actions, bool isIdle, Reference<bool> onlyFoundIdleWork)
		{
			onlyFoundIdleWork.Value = false;
			var isReduceStale = actions.Staleness.IsReduceStale(indexesStat.Id);

			if (isReduceStale == false)
				return false;

			if (indexesStat.Priority.HasFlag(IndexingPriority.Error))
				return false;

			return true;
		}

	    public override bool ShouldRun
	    {
	        get { return context.RunReducing; }
	    }

	    protected override DatabaseTask GetApplicableTask(IStorageActionsAccessor actions)
		{
			return null;
		}

		protected override void FlushAllIndexes()
		{
			context.IndexStorage.FlushReduceIndexes();
		}

		protected override IndexToWorkOn GetIndexToWorkOn(IndexStats indexesStat)
		{
			return new IndexToWorkOn
			{
				IndexId = indexesStat.Id,
				LastIndexedEtag = Etag.Empty
			};
		}

		protected override void ExecuteIndexingWork(IList<IndexToWorkOn> indexesToWorkOn)
		{
			ReducingBatchInfo reducingBatchInfo = null;

			try
			{
				reducingBatchInfo = context.ReportReducingBatchStarted(indexesToWorkOn.Select(x => x.Index.PublicName).ToList());

				BackgroundTaskExecuter.Instance.ExecuteAllInterleaved(context, indexesToWorkOn, index =>
				{
					var performanceStats = HandleReduceForIndex(index, context.CancellationToken);

					reducingBatchInfo.PerformanceStats.TryAdd(index.Index.PublicName, performanceStats);
				});
			}
			finally
			{
				if (reducingBatchInfo != null)
					context.ReportReducingBatchCompleted(reducingBatchInfo);
			}
		}

		protected override bool IsValidIndex(IndexStats indexesStat)
		{
			var indexDefinition = context.IndexDefinitionStorage.GetIndexDefinition(indexesStat.Id);
			return indexDefinition != null && indexDefinition.IsMapReduce;
		}
	}
}
