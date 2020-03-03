﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Exceptions.Documents.Patching;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Documents.ETL.Metrics;
using Raven.Server.Documents.ETL.Providers.Raven;
using Raven.Server.Documents.ETL.Providers.Raven.Test;
using Raven.Server.Documents.ETL.Providers.SQL;
using Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.ETL.Test;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.ETL;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Memory;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Threading;
using Sparrow.Utils;
using Size = Sparrow.Size;

namespace Raven.Server.Documents.ETL
{
    public abstract class EtlProcess : IDisposable
    {
        public string Tag { get; protected set; }

        public abstract EtlType EtlType { get; }
        
        public abstract long TaskId { get; }

        public EtlProcessStatistics Statistics { get; protected set; }

        public EtlMetricsCountersManager Metrics { get; protected set; }

        public string Name { get; protected set; }

        public string ConfigurationName { get; protected set; }

        public string TransformationName { get; protected set; }

        public TimeSpan? FallbackTime { get; protected set; }

        public abstract void Start();

        public abstract void Stop(string reason);

        public abstract void Dispose();

        public abstract void Reset();

        public abstract void NotifyAboutWork(DocumentChange documentChange, CounterChange counterChange);

        public abstract bool ShouldTrackCounters();

        public abstract EtlPerformanceStats[] GetPerformanceStats();

        public abstract EtlStatsAggregator GetLatestPerformanceStats();

        public abstract OngoingTaskConnectionStatus GetConnectionStatus();

        public abstract EtlProcessProgress GetProgress(DocumentsOperationContext documentsContext);

        public static EtlProcessState GetProcessState(DocumentDatabase database, string configurationName, string transformationName)
        {
            using (database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var stateBlittable = database.ServerStore.Cluster.Read(context, EtlProcessState.GenerateItemName(database.Name, configurationName, transformationName));

                if (stateBlittable != null)
                {
                    return JsonDeserializationClient.EtlProcessState(stateBlittable);
                }

                return new EtlProcessState();
            }
        }
    }

    public abstract class EtlProcess<TExtracted, TTransformed, TConfiguration, TConnectionString> : EtlProcess, ILowMemoryHandler where TExtracted : ExtractedItem
        where TConfiguration : EtlConfiguration<TConnectionString>
        where TConnectionString : ConnectionString
    {
        private static readonly Size DefaultMaximumMemoryAllocation = new Size(32, SizeUnit.Megabytes);
        internal const int MinBatchSize = 64;

        private readonly ManualResetEventSlim _waitForChanges = new ManualResetEventSlim();
        private readonly CancellationTokenSource _cts;
        private readonly HashSet<string> _collections;

        private readonly ConcurrentQueue<EtlStatsAggregator> _lastEtlStats =
            new ConcurrentQueue<EtlStatsAggregator>();

        private Size _currentMaximumAllowedMemory = DefaultMaximumMemoryAllocation;
        private NativeMemory.ThreadStats _threadAllocations;
        private PoolOfThreads.LongRunningWork _longRunningWork;
        private readonly MultipleUseFlag _lowMemoryFlag = new MultipleUseFlag();
        private EtlStatsAggregator _lastStats;
        private EtlProcessState _lastProcessState;
        private int _statsId;

        private TestMode _testMode;

        protected readonly Transformation Transformation;
        protected readonly Logger Logger;
        protected readonly DocumentDatabase Database;
        private readonly ServerStore _serverStore;

        public readonly TConfiguration Configuration;

        protected EtlProcess(Transformation transformation, TConfiguration configuration, DocumentDatabase database, ServerStore serverStore, string tag)
        {
            Transformation = transformation;
            Configuration = configuration;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(database.DatabaseShutdown);
            Tag = tag;
            ConfigurationName = Configuration.Name;
            TransformationName = Transformation.Name;
            Name = $"{Configuration.Name}/{Transformation.Name}";
            Logger = LoggingSource.Instance.GetLogger(database.Name, GetType().FullName);
            Database = database;
            _serverStore = serverStore;
            Statistics = new EtlProcessStatistics(Tag, Name, Database.NotificationCenter);

            if (transformation.ApplyToAllDocuments == false)
                _collections = new HashSet<string>(Transformation.Collections, StringComparer.OrdinalIgnoreCase);

            _lastProcessState = GetProcessState(Database, Configuration.Name, Transformation.Name);
        }

        protected CancellationToken CancellationToken => _cts.Token;

        protected abstract IEnumerator<TExtracted> ConvertDocsEnumerator(IEnumerator<Document> docs, string collection);

        protected abstract IEnumerator<TExtracted> ConvertTombstonesEnumerator(IEnumerator<Tombstone> tombstones, string collection, EtlItemType type);

        protected abstract IEnumerator<TExtracted> ConvertCountersEnumerator(IEnumerator<CounterGroupDetail> counters, string collection);

        protected abstract bool ShouldTrackAttachmentTombstones();
        
        public override long TaskId => Configuration.TaskId;

        public virtual IEnumerable<TExtracted> Extract(DocumentsOperationContext context, long fromEtag, EtlItemType type, EtlStatsScope stats)
        {
            using (var scope = new DisposableScope())
            using (var merged = new ExtractedItemsEnumerator<TExtracted>(stats, type))
            {
                switch (type)
                {
                    case EtlItemType.Document:
                        var enumerators = new List<(IEnumerator<Document> Docs, IEnumerator<Tombstone> Tombstones, string Collection)>(Transformation.Collections.Count);

                        if (Transformation.ApplyToAllDocuments)
                        {
                            var docs = Database.DocumentsStorage.GetDocumentsFrom(context, fromEtag, 0, int.MaxValue).GetEnumerator();
                            scope.EnsureDispose(docs);

                            var tombstones = Database.DocumentsStorage.GetTombstonesFrom(context, fromEtag, 0, int.MaxValue).GetEnumerator();
                            scope.EnsureDispose(tombstones);

                            tombstones = new FilterTombstonesEnumerator(tombstones, stats, Tombstone.TombstoneType.Document, context);

                            enumerators.Add((docs, tombstones, null));
                        }
                        else
                        {
                            foreach (var collection in Transformation.Collections)
                            {
                                var docs = Database.DocumentsStorage.GetDocumentsFrom(context, collection, fromEtag, 0, int.MaxValue).GetEnumerator();
                                scope.EnsureDispose(docs);

                                var tombstones = Database.DocumentsStorage.GetTombstonesFrom(context, collection, fromEtag, 0, int.MaxValue).GetEnumerator();
                                scope.EnsureDispose(tombstones);

                                enumerators.Add((docs, tombstones, collection));
                            }
                        }

                        foreach (var en in enumerators)
                        {
                            merged.AddEnumerator(ConvertDocsEnumerator(en.Docs, en.Collection));
                            merged.AddEnumerator(ConvertTombstonesEnumerator(en.Tombstones, en.Collection, EtlItemType.Document));
                        }

                        if (ShouldTrackAttachmentTombstones())
                        {
                            var attachmentTombstones = Database.DocumentsStorage
                                .GetTombstonesFrom(context, AttachmentsStorage.AttachmentsTombstones, fromEtag, 0, int.MaxValue).GetEnumerator();
                            scope.EnsureDispose(attachmentTombstones);

                            attachmentTombstones = new FilterTombstonesEnumerator(attachmentTombstones, stats, Tombstone.TombstoneType.Attachment, context,
                                fromCollections: Transformation.ApplyToAllDocuments ? null : Transformation.Collections);

                            merged.AddEnumerator(ConvertTombstonesEnumerator(attachmentTombstones, null, EtlItemType.Document));
                        }

                        break;
                    case EtlItemType.CounterGroup:

                        var lastDocEtag = stats.GetLastTransformedOrFilteredEtag(EtlItemType.Document);

                        if (Transformation.ApplyToAllDocuments)
                        {
                            var counters = Database.DocumentsStorage.CountersStorage.GetCountersFrom(context, fromEtag, 0, int.MaxValue).GetEnumerator();
                            scope.EnsureDispose(counters);

                            counters = new FilterCountersEnumerator(counters, stats, Database.DocumentsStorage, context, lastDocEtag);

                            merged.AddEnumerator(ConvertCountersEnumerator(counters, null));

                        }
                        else
                        {
                            foreach (var collection in Transformation.Collections)
                            {
                                var counters = Database.DocumentsStorage.CountersStorage.GetCountersFrom(context, collection, fromEtag, 0, int.MaxValue).GetEnumerator();
                                scope.EnsureDispose(counters);

                                counters = new FilterCountersEnumerator(counters, stats, Database.DocumentsStorage, context, lastDocEtag);

                                merged.AddEnumerator(ConvertCountersEnumerator(counters, collection));
                            }
                        }

                        break;
                    default:
                        throw new NotSupportedException($"Invalid ETL item type: {type}");
                }

                while (merged.MoveNext())
                {
                    yield return merged.Current;
                }
            }
        }

        protected abstract EtlTransformer<TExtracted, TTransformed> GetTransformer(DocumentsOperationContext context);

        public List<TTransformed> Transform(IEnumerable<TExtracted> items, DocumentsOperationContext context, EtlStatsScope stats, EtlProcessState state)
        {
            using (var transformer = GetTransformer(context))
            {
                transformer.Initialize(debugMode: _testMode != null);

                var batchSize = 0;

                var batchStopped = false;

                foreach (var item in items)
                {
                    stats.RecordLastExtractedEtag(item.Etag, item.Type);

                    if (AlreadyLoadedByDifferentNode(item, state))
                    {
                        stats.RecordChangeVector(item.ChangeVector);
                        stats.RecordLastFilteredOutEtag(item.Etag, item.Type);

                        continue;
                    }

                    if (Transformation.ApplyToAllDocuments &&
                        item.Type == EtlItemType.Document &&
                        CollectionName.IsHiLoCollection(item.CollectionFromMetadata) &&
                        ShouldFilterOutHiLoDocument())
                    {
                        stats.RecordChangeVector(item.ChangeVector);
                        stats.RecordLastFilteredOutEtag(item.Etag, item.Type);

                        continue;
                    }

                    using (stats.For(EtlOperations.Transform))
                    {
                        CancellationToken.ThrowIfCancellationRequested();

                        try
                        {
                            if (CanContinueBatch(stats, item, batchSize, context) == false)
                            {
                                batchStopped = true;
                                break;
                            }

                            transformer.Transform(item, stats);

                            Statistics.TransformationSuccess();

                            stats.RecordTransformedItem(item.Type, item.IsDelete);
                            stats.RecordLastTransformedEtag(item.Etag, item.Type);
                            stats.RecordChangeVector(item.ChangeVector);

                            batchSize++;
                            
                        }
                        catch (JavaScriptParseException e)
                        {
                            var message = $"[{Name}] Could not parse transformation script. Stopping ETL process.";

                            if (Logger.IsOperationsEnabled)
                                Logger.Operations(message, e);

                            var alert = AlertRaised.Create(
                                Database.Name,
                                Tag,
                                message,
                                AlertType.Etl_InvalidScript,
                                NotificationSeverity.Error,
                                key: Name,
                                details: new ExceptionDetails(e));

                            Database.NotificationCenter.Add(alert);

                            stats.RecordBatchCompleteReason(message);

                            Stop(reason: message);

                            break;
                        }
                        catch (Exception e)
                        {
                            Statistics.RecordTransformationError(e, item.DocumentId);

                            stats.RecordTransformationError();

                            if (Logger.IsOperationsEnabled)
                                Logger.Operations($"Could not process SQL ETL script for '{Name}', skipping document: {item.DocumentId}", e);
                        }
                    }
                }

                if (batchStopped == false && stats.HasBatchCompleteReason() == false)
                    stats.RecordBatchCompleteReason("No more items to process");

                _testMode?.DebugOutput.AddRange(transformer.GetDebugOutput());

                return transformer.GetTransformedResults();
            }
        }

        public void Load(IEnumerable<TTransformed> items, DocumentsOperationContext context, EtlStatsScope stats)
        {
            using (stats.For(EtlOperations.Load))
            {
                try
                {
                    var count = LoadInternal(items, context);

                    stats.RecordLastLoadedEtag(stats.LastTransformedEtags.Values.Max());

                    Statistics.LoadSuccess(stats.NumberOfTransformedItems.Sum(x => x.Value));

                    stats.RecordLoadSuccess(count);
                }
                catch (Exception e)
                {
                    if (Logger.IsOperationsEnabled)
                        Logger.Operations($"Failed to load transformed data for '{Name}'", e);

                    stats.RecordLoadFailure();

                    EnterFallbackMode();

                    Statistics.RecordLoadError(e.ToString(), documentId: null, count: stats.NumberOfExtractedItems.Sum(x => x.Value));
                }
            }
        }

        private void EnterFallbackMode()
        {
            if (Statistics.LastLoadErrorTime == null)
                FallbackTime = TimeSpan.FromSeconds(5);
            else
            {
                // double the fallback time (but don't cross Etl.MaxFallbackTime)
                var secondsSinceLastError = (Database.Time.GetUtcNow() - Statistics.LastLoadErrorTime.Value).TotalSeconds;

                FallbackTime = TimeSpan.FromSeconds(Math.Min(Database.Configuration.Etl.MaxFallbackTime.AsTimeSpan.TotalSeconds, Math.Max(5, secondsSinceLastError * 2)));
            }
        }

        protected abstract int LoadInternal(IEnumerable<TTransformed> items, DocumentsOperationContext context);

        public bool CanContinueBatch(EtlStatsScope stats, TExtracted currentItem, int batchSize, DocumentsOperationContext ctx)
        {
            if (currentItem.Type == EtlItemType.CounterGroup)
            {
                // we have special counters enumerator which ensures that we iterate counters up to last processed doc etag

                if (stats.GetLastTransformedOrFilteredEtag(EtlItemType.Document) > 0)
                {
                    // we had some documents processed in current batch
                    // as long as the counters enumerator returns items we'll ETL all of them as we track
                    // the ETL processing state by a single last processed etag

                    return true;
                }

                // we had no documents in current batch we can send all counters that we have
                // although need to respect below criteria
            }

            if (_serverStore.Server.CpuCreditsBalance.BackgroundTasksAlertRaised.IsRaised())
            {
                var reason = $"Stopping the batch after {stats.Duration} because the CPU credits balance is almost completely used";

                if (Logger.IsInfoEnabled)
                    Logger.Info($"[{Name}] {reason}");

                stats.RecordBatchCompleteReason(reason);

                return false;
            }

            if (currentItem.Type == EtlItemType.Document &&
                stats.NumberOfExtractedItems[EtlItemType.Document] > Database.Configuration.Etl.MaxNumberOfExtractedDocuments ||
                stats.NumberOfExtractedItems.Sum(x => x.Value) > Database.Configuration.Etl.MaxNumberOfExtractedItems)
            {
                var reason = $"Stopping the batch because it has already processed max number of items ({string.Join(',', stats.NumberOfExtractedItems.Select(x => $"{x.Key} - {x.Value}"))})";

                if (Logger.IsInfoEnabled)
                    Logger.Info($"[{Name}] {reason}");

                stats.RecordBatchCompleteReason(reason);

                return false;
            }

            if (stats.Duration >= Database.Configuration.Etl.ExtractAndTransformTimeout.AsTimeSpan)
            {
                var reason = $"Stopping the batch after {stats.Duration} due to extract and transform processing timeout";

                if (Logger.IsInfoEnabled)
                    Logger.Info($"[{Name}] {reason}");

                stats.RecordBatchCompleteReason(reason);

                return false;
            }

            if (_lowMemoryFlag.IsRaised() && batchSize >= MinBatchSize)
            {
                var reason = $"The batch was stopped after processing {batchSize:#,#;;0} items because of low memory";

                if (Logger.IsInfoEnabled)
                    Logger.Info($"[{Name}] {reason}");

                stats.RecordBatchCompleteReason(reason);
                return false;
            }

            var totalAllocated = new Size(_threadAllocations.TotalAllocated + ctx.Transaction.InnerTransaction.LowLevelTransaction.TotalEncryptionBufferSize.GetValue(SizeUnit.Bytes), SizeUnit.Bytes);
            _threadAllocations.CurrentlyAllocatedForProcessing = totalAllocated.GetValue(SizeUnit.Bytes);

            stats.RecordCurrentlyAllocated(totalAllocated.GetValue(SizeUnit.Bytes) + GC.GetAllocatedBytesForCurrentThread());

            if (totalAllocated > _currentMaximumAllowedMemory)
            {
                if (MemoryUsageGuard.TryIncreasingMemoryUsageForThread(_threadAllocations, ref _currentMaximumAllowedMemory,
                        totalAllocated,
                        Database.DocumentsStorage.Environment.Options.RunningOn32Bits, Logger, out var memoryUsage) == false)
                {
                    var reason = $"Stopping the batch because cannot budget additional memory. Current budget: {totalAllocated}.";
                    if (memoryUsage != null)
                    {
                        reason += " Current memory usage: " +
                                   $"{nameof(memoryUsage.WorkingSet)} = {memoryUsage.WorkingSet}," +
                                   $"{nameof(memoryUsage.PrivateMemory)} = {memoryUsage.PrivateMemory}";
                    }

                    if (Logger.IsInfoEnabled)
                        Logger.Info($"[{Name}] {reason}");

                    stats.RecordBatchCompleteReason(reason);

                    ctx.DoNotReuse = true;

                    return false;
                }
            }

            var maxBatchSize = Database.Configuration.Etl.MaxBatchSize;

            if (maxBatchSize != null && stats.BatchSize >= maxBatchSize)
            {
                var reason = $"Stopping the batch because maximum batch size limit was reached ({stats.BatchSize})";

                if (Logger.IsInfoEnabled)
                    Logger.Info($"[{Name}] {reason}");

                stats.RecordBatchCompleteReason(reason);

                return false;
            }

            return true;
        }

        protected void UpdateMetrics(DateTime startTime, EtlStatsScope stats)
        {
            var batchSize = stats.NumberOfExtractedItems.Sum(x => x.Value);

            Metrics.BatchSizeMeter.MarkSingleThreaded(batchSize);
            Metrics.UpdateProcessedPerSecondRate(batchSize, stats.Duration);
        }

        public override void Reset()
        {
            Statistics.Reset();

            if (_longRunningWork == null)
                return;

            _waitForChanges.Set();
        }

        public override void NotifyAboutWork(DocumentChange documentChange, CounterChange counterChange)
        {
            if (documentChange != null && (Transformation.ApplyToAllDocuments || _collections.Contains(documentChange.CollectionName)))
                _waitForChanges.Set();

            if (counterChange != null && ShouldTrackCounters())
                _waitForChanges.Set();
        }

        public override void Start()
        {
            if (_longRunningWork != null)
                return;

            if (Transformation.Disabled || Configuration.Disabled)
                return;

            var threadName = $"{Tag} process: {Name}";
            _longRunningWork = PoolOfThreads.GlobalRavenThreadPool.LongRunning(x =>
            {
                try
                {
                    // This has lower priority than request processing, so we let the OS
                    // schedule this appropriately
                    Thread.CurrentThread.Priority = ThreadPriority.BelowNormal;
                    NativeMemory.EnsureRegistered();
                    Run();
                }
                catch (Exception e)
                {
                    if (Logger.IsOperationsEnabled)
                        Logger.Operations($"Failed to run ETL {Name}", e);
                }
            }, null, threadName);

            if (Logger.IsOperationsEnabled)
                Logger.Operations($"Starting {Tag} process: '{Name}'.");

        }

        public override void Stop(string reason)
        {
            if (_longRunningWork == null)
                return;

            if (Logger.IsOperationsEnabled)
                Logger.Operations($"Stopping {Tag} process: '{Name}'. Reason: {reason}");

            _cts.Cancel();

            var longRunningWork = _longRunningWork;
            _longRunningWork = null;

            if (longRunningWork != PoolOfThreads.LongRunningWork.Current) // prevent a deadlock
                longRunningWork.Join(int.MaxValue);
        }

        public void Run()
        {
            while (true)
            {
                try
                {
                    if (CancellationToken.IsCancellationRequested)
                        return;
                }
                catch (ObjectDisposedException)
                {
                    return;
                }

                try
                {
                    _waitForChanges.Reset();

                    var startTime = Database.Time.GetUtcNow();

                    var didWork = false;

                    var state  = _lastProcessState = GetProcessState(Database, Configuration.Name, Transformation.Name);

                    var loadLastProcessedEtag = state.GetLastProcessedEtagForNode(_serverStore.NodeTag);

                    using (Statistics.NewBatch())
                    using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var statsAggregator = _lastStats = new EtlStatsAggregator(Interlocked.Increment(ref _statsId), _lastStats);
                        AddPerformanceStats(statsAggregator);

                        using (var stats = statsAggregator.CreateScope())
                        {
                            try
                            {
                                EnsureThreadAllocationStats();

                                using (context.OpenReadTransaction())
                                {
                                    List<TTransformed> transformations = null;

                                    var typesToWorkOn = new List<EtlItemType>()
                                    {
                                        EtlItemType.Document
                                    };

                                    if (ShouldTrackCounters())
                                    {
                                        typesToWorkOn.Add(EtlItemType.CounterGroup);
                                    }

                                    foreach (var type in typesToWorkOn)
                                    {
                                        var extracted = Extract(context, loadLastProcessedEtag + 1, type, stats);

                                        var transformed = Transform(extracted, context, stats, state);

                                        if (transformations == null)
                                            transformations = transformed;
                                        else
                                            transformations.AddRange(transformed);
                                    }

                                    if (transformations.Count > 0)
                                        Load(transformations, context, stats);

                                    var lastProcessed = Math.Max(stats.LastLoadedEtag, stats.LastFilteredOutEtags.Values.Max());

                                    if (lastProcessed > Statistics.LastProcessedEtag)
                                    {
                                        didWork = true;
                                        Statistics.LastProcessedEtag = lastProcessed;
                                        Statistics.LastChangeVector = stats.ChangeVector;
                                    }

                                    if (didWork)
                                    {
                                        UpdateMetrics(startTime, stats);

                                        if (Logger.IsInfoEnabled)
                                            LogSuccessfulBatchInfo(stats);
                                    }
                                }
                            }
                            catch (OperationCanceledException)
                            {
                                return;
                            }
                            catch (Exception e)
                            {
                                var message = $"Exception in ETL process '{Name}'";

                                if (Logger.IsOperationsEnabled)
                                    Logger.Operations($"{Tag} {message}", e);
                            }
                        }

                        statsAggregator.Complete();
                    }

                    if (didWork)
                    {
                        var command = new UpdateEtlProcessStateCommand(Database.Name, Configuration.Name, Transformation.Name, Statistics.LastProcessedEtag,
                            ChangeVectorUtils.MergeVectors(Statistics.LastChangeVector, state.ChangeVector), _serverStore.NodeTag,
                            _serverStore.LicenseManager.HasHighlyAvailableTasks(), RaftIdGenerator.NewId());

                        try
                        {
                            var sendToLeaderTask = _serverStore.SendToLeaderAsync(command);

                            sendToLeaderTask.Wait(CancellationToken);
                            var (etag, _) = sendToLeaderTask.Result;

                            Database.RachisLogIndexNotifications.WaitForIndexNotification(etag, _serverStore.Engine.OperationTimeout).Wait(CancellationToken);
                        }
                        catch (OperationCanceledException)
                        {
                            return;
                        }

                        if (CancellationToken.IsCancellationRequested == false)
                        {
                            Database.EtlLoader.OnBatchCompleted(ConfigurationName, TransformationName, Statistics);
                        }

                        continue;
                    }

                    try
                    {
                        PauseIfCpuCreditsBalanceIsTooLow();

                        if (FallbackTime == null)
                        {
                            _waitForChanges.Wait(CancellationToken);
                        }
                        else
                        {
                            var sp = Stopwatch.StartNew();

                            if (_waitForChanges.Wait(FallbackTime.Value, CancellationToken))
                            {
                                // we are in the fallback mode but got new docs to process
                                // let's wait full time and retry the process then

                                var timeLeftToWait = FallbackTime.Value - sp.Elapsed;

                                if (timeLeftToWait > TimeSpan.Zero)
                                {
                                    Thread.Sleep(timeLeftToWait);
                                }
                            }

                            FallbackTime = null;
                        }
                    }
                    catch (ObjectDisposedException)
                    {
                        return;
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }
                }
                catch (Exception e)
                {
                    if (Logger.IsOperationsEnabled)
                        Logger.Operations($"Unexpected error in {Tag} process: '{Name}'", e);
                }
                finally
                {
                    _threadAllocations.CurrentlyAllocatedForProcessing = 0;
                    _currentMaximumAllowedMemory = new Size(32, SizeUnit.Megabytes);
                }
            }
        }

        private void PauseIfCpuCreditsBalanceIsTooLow()
        {
            AlertRaised alert = null;
            int numberOfTimesSlept = 0;
            while (_serverStore.Server.CpuCreditsBalance.BackgroundTasksAlertRaised.IsRaised() &&
                Database.DatabaseShutdown.IsCancellationRequested == false)
            {
                // give us a bit more than a measuring cycle to gain more CPU credits
                Thread.Sleep(1250);
                if (alert == null && numberOfTimesSlept++ > 5)
                {
                    alert = AlertRaised.Create(
                       Database.Name,
                       Tag,
                       "Etl process paused because the CPU credits balance is almost completely used, will be resumed when there are enough CPU credits to use.",
                       AlertType.Throttling_CpuCreditsBalance,
                       NotificationSeverity.Warning,
                       key: Name);
                    Database.NotificationCenter.Add(alert);
                }
            }
            if (alert != null)
            {
                Database.NotificationCenter.Dismiss(alert.Id);
            }
        }

        protected abstract bool ShouldFilterOutHiLoDocument();

        private static bool AlreadyLoadedByDifferentNode(ExtractedItem item, EtlProcessState state)
        {
            var conflictStatus = ChangeVectorUtils.GetConflictStatus(
                remoteAsString: item.ChangeVector,
                localAsString: state.ChangeVector);

            return conflictStatus == ConflictStatus.AlreadyMerged;
        }

        protected void EnsureThreadAllocationStats()
        {
            _threadAllocations = NativeMemory.CurrentThreadStats;
        }

        private void AddPerformanceStats(EtlStatsAggregator stats)
        {
            _lastEtlStats.Enqueue(stats);

            while (_lastEtlStats.Count > 25)
                _lastEtlStats.TryDequeue(out stats);
        }

        public override EtlPerformanceStats[] GetPerformanceStats()
        {
            var lastStats = _lastStats;

            return _lastEtlStats
                .Select(x => x == lastStats ? x.ToPerformanceLiveStatsWithDetails() : x.ToPerformanceStats())
                .ToArray();
        }

        public override EtlStatsAggregator GetLatestPerformanceStats()
        {
            return _lastStats;
        }

        private void LogSuccessfulBatchInfo(EtlStatsScope stats)
        {
            var message = new StringBuilder();

            message.Append(
                $"{Tag} process '{Name}' processed the following number of items: ");

            foreach (var extracted in stats.NumberOfExtractedItems)
            {
                if (extracted.Value > 0)
                    message.Append($"{extracted.Key} - {extracted.Value} (last transformed etag: {stats.LastTransformedEtags[extracted.Key]}");

                if (stats.LastFilteredOutEtags[extracted.Key] > 0)
                    message.Append($", last filtered etag: {stats.LastFilteredOutEtags[extracted.Key]}");

                message.Append("), ");
            }

            message.Append($" in {stats.Duration} (last loaded etag: {stats.LastLoadedEtag})");

            if (stats.BatchCompleteReason != null)
                message.Append($"Batch completion reason: {stats.BatchCompleteReason}");

            Logger.Info(message.ToString());
        }

        public override OngoingTaskConnectionStatus GetConnectionStatus()
        {
            if (Configuration.Disabled || _cts.IsCancellationRequested)
                return OngoingTaskConnectionStatus.NotActive;

            if (FallbackTime != null)
                return OngoingTaskConnectionStatus.Reconnect;

            if (Statistics.WasLatestLoadSuccessful || Statistics.LoadErrors == 0)
                return OngoingTaskConnectionStatus.Active;

            return OngoingTaskConnectionStatus.NotActive;
        }

        public static TestEtlScriptResult TestScript(TestEtlScript<TConfiguration, TConnectionString> testScript, DocumentDatabase database, ServerStore serverStore,
            DocumentsOperationContext context)
        {
            using (testScript.IsDelete ? context.OpenWriteTransaction() : context.OpenReadTransaction()) // we open write tx to test deletion but we won't commit it
            {
                var document = database.DocumentsStorage.Get(context, testScript.DocumentId);

                if (document == null)
                    throw new InvalidOperationException($"Document {testScript.DocumentId} does not exist");

                TConnectionString connection = null;

                var sqlTestScript = testScript as TestSqlEtlScript;

                if (sqlTestScript != null)
                {
                    // we need to have connection string when testing SQL ETL because we need to have the factory name
                    // and if PerformRolledBackTransaction = true is specified then we need make a connection to SQL

                    var csErrors = new List<string>();

                    if (sqlTestScript.Connection != null)
                    {
                        if (sqlTestScript.Connection.Validate(ref csErrors) == false)
                            throw new InvalidOperationException($"Invalid connection string due to {string.Join(";", csErrors)}");

                        connection = sqlTestScript.Connection as TConnectionString;
                    }
                    else
                    {
                        Dictionary<string, SqlConnectionString> sqlConnectionStrings;
                        using (serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                        using (ctx.OpenReadTransaction())
                        using (var rawRecord = serverStore.Cluster.ReadDatabaseRecord(ctx, database.Name))
                        {
                            sqlConnectionStrings = rawRecord.GetSqlConnectionStrings();
                            if (sqlConnectionStrings == null)
                                throw new InvalidOperationException($"{nameof(DatabaseRecord.SqlConnectionStrings)} was not found in the database record");
                        }

                        if (sqlConnectionStrings.TryGetValue(testScript.Configuration.ConnectionStringName, out var sqlConnection) == false)
                        {
                            throw new InvalidOperationException(
                                $"Connection string named '{testScript.Configuration.ConnectionStringName}' was not found in the database record");
                        }

                        if (sqlConnection.Validate(ref csErrors) == false)
                            throw new InvalidOperationException(
                                $"Invalid '{testScript.Configuration.ConnectionStringName}' connection string due to {string.Join(";", csErrors)}");

                        connection = sqlConnection as TConnectionString;
                    }
                }

                testScript.Configuration.Initialize(connection);

                testScript.Configuration.TestMode = true;

                if (testScript.Configuration.Validate(out List<string> errors) == false)
                {
                    throw new InvalidOperationException($"Invalid ETL configuration for '{testScript.Configuration.Name}'. " +
                                                        $"Reason{(errors.Count > 1 ? "s" : string.Empty)}: {string.Join(";", errors)}.");
                }

                if (testScript.Configuration.Transforms.Count != 1)
                {
                    throw new InvalidOperationException($"Invalid number of transformations. You have provided {testScript.Configuration.Transforms.Count} " +
                                                        "while ETL test expects to get exactly 1 transformation script");
                }

                var docCollection = database.DocumentsStorage.ExtractCollectionName(context, document.Data).Name;

                if (testScript.Configuration.Transforms[0].ApplyToAllDocuments == false &&
                    testScript.Configuration.Transforms[0].Collections.Contains(docCollection, StringComparer.OrdinalIgnoreCase) == false)
                {
                    throw new InvalidOperationException($"Document '{document.Id}' belongs to {docCollection} collection " +
                                                        $"while tested ETL script works on the following collections: {string.Join(", ", testScript.Configuration.Transforms[0].Collections)}");
                }

                if (testScript.Configuration.Transforms[0].ApplyToAllDocuments)
                {
                    // when ETL script has ApplyToAllDocuments then it extracts docs without
                    // providing collection name to ExtractedItem
                    // it is retrieved from metadata then
                    // let's do the same to ensure we have the same behavior in test mode

                    docCollection = null;
                }

                Tombstone tombstone = null;

                if (testScript.IsDelete)
                {
                    var deleteResult = database.DocumentsStorage.Delete(context, testScript.DocumentId, null);

                    tombstone = database.DocumentsStorage.GetTombstoneByEtag(context, deleteResult.Value.Etag);
                }

                List<string> debugOutput;

                switch (testScript.Configuration.EtlType)
                {
                    case EtlType.Sql:
                        using (var sqlEtl = new SqlEtl(testScript.Configuration.Transforms[0], testScript.Configuration as SqlEtlConfiguration, database, database.ServerStore))
                        using (sqlEtl.EnterTestMode(out debugOutput))
                        {
                            sqlEtl.EnsureThreadAllocationStats();

                            var sqlItem = testScript.IsDelete ? new ToSqlItem(tombstone, docCollection) : new ToSqlItem(document, docCollection);

                            var transformed = sqlEtl.Transform(new[] {sqlItem}, context, new EtlStatsScope(new EtlRunStats()),
                                new EtlProcessState());

                            Debug.Assert(sqlTestScript != null);

                            var result = sqlEtl.RunTest(context, transformed, sqlTestScript.PerformRolledBackTransaction);
                            result.DebugOutput = debugOutput;

                            return result;
                        }
                    case EtlType.Raven:
                        using (var ravenEtl = new RavenEtl(testScript.Configuration.Transforms[0], testScript.Configuration as RavenEtlConfiguration, database, database.ServerStore))
                        using (ravenEtl.EnterTestMode(out debugOutput))
                        {
                            ravenEtl.EnsureThreadAllocationStats();

                            var ravenEtlItem = testScript.IsDelete ? new RavenEtlItem(tombstone, docCollection, EtlItemType.Document) : new RavenEtlItem(document, docCollection);

                            var results = ravenEtl.Transform(new[] {ravenEtlItem}, context, new EtlStatsScope(new EtlRunStats()),
                                new EtlProcessState());

                            return new RavenEtlTestScriptResult
                            {
                                TransformationErrors = ravenEtl.Statistics.TransformationErrorsInCurrentBatch.Errors.ToList(),
                                Commands = results.ToList(),
                                DebugOutput = debugOutput
                            };
                        }
                    default:
                        throw new NotSupportedException($"Unknown ETL type in script test: {testScript.Configuration.EtlType}");
                }
            }
        }

        private IDisposable EnterTestMode(out List<string> debugOutput)
        {
            _testMode = new TestMode();
            var disableAlerts = Statistics.PreventFromAddingAlertsToNotificationCenter();

            debugOutput = _testMode.DebugOutput;

            return new DisposableAction(() =>
            {
                _testMode = null;
                disableAlerts.Dispose();
            });
        }

        public override EtlProcessProgress GetProgress(DocumentsOperationContext documentsContext)
        {
            var result = new EtlProcessProgress
            {
                TransformationName = TransformationName,
                Disabled = Transformation.Disabled || Configuration.Disabled,
                AverageProcessedPerSecond = Metrics.GetProcessedPerSecondRate() ?? 0.0
            };
            
            List<string> collections;

            if (Transformation.ApplyToAllDocuments)
            {
                collections = Database.DocumentsStorage.GetCollections(documentsContext).Select(x => x.Name).ToList();
            }
            else
            {
                collections = Transformation.Collections;
            }

            long docsToProcess = 0;
            long totalDocsCount = 0;

            long docsTombstonesToProcess = 0;
            long totalDocsTombstonesCount = 0;

            long counterGroupsToProcess = 0;
            long totalCounterGroupsCount = 0;

            var lastProcessedEtag = _lastProcessState.GetLastProcessedEtagForNode(_serverStore.NodeTag);

            foreach (var collection in collections)
            {
                docsToProcess += Database.DocumentsStorage.GetNumberOfDocumentsToProcess(documentsContext, collection, lastProcessedEtag, out var total);
                totalDocsCount += total;

                docsTombstonesToProcess += Database.DocumentsStorage.GetNumberOfTombstonesToProcess(documentsContext, collection, lastProcessedEtag, out total);
                totalDocsTombstonesCount += total;

                if (ShouldTrackCounters())
                {
                    counterGroupsToProcess += Database.DocumentsStorage.CountersStorage.GetNumberOfCounterGroupsToProcess(documentsContext, collection, lastProcessedEtag, out total);
                    totalCounterGroupsCount += total;
                }
            }

            result.NumberOfDocumentsToProcess = docsToProcess;
            result.TotalNumberOfDocuments = totalDocsCount;

            result.NumberOfDocumentTombstonesToProcess = docsTombstonesToProcess;
            result.TotalNumberOfDocumentTombstones = totalDocsTombstonesCount;

            result.NumberOfCounterGroupsToProcess = counterGroupsToProcess;
            result.TotalNumberOfCounterGroups = totalCounterGroupsCount;

            result.Completed = (result.NumberOfDocumentsToProcess > 0 || result.NumberOfDocumentTombstonesToProcess > 0 ||
                                result.NumberOfCounterGroupsToProcess > 0) == false;

            var performance = _lastStats?.ToPerformanceLiveStats();

            if (performance != null && performance.DurationInMs > 0 &&
                performance.SuccessfullyLoaded != false && FallbackTime != null)
            {
                var processedPerSecondInCurrentBatch = performance.NumberOfExtractedItems.Sum(x => x.Value) / (performance.DurationInMs / 1000);

                result.AverageProcessedPerSecond = (result.AverageProcessedPerSecond + processedPerSecondInCurrentBatch) / 2;

                if (result.NumberOfDocumentsToProcess > 0)
                    result.NumberOfDocumentsToProcess -= performance.NumberOfTransformedItems[EtlItemType.Document];

                if (result.NumberOfDocumentTombstonesToProcess > 0)
                    result.NumberOfDocumentTombstonesToProcess -= performance.NumberOfTransformedTombstones[EtlItemType.Document];

                if (result.NumberOfCounterGroupsToProcess > 0)
                    result.NumberOfCounterGroupsToProcess -= performance.NumberOfTransformedItems[EtlItemType.CounterGroup];

                result.Completed = (result.NumberOfDocumentsToProcess > 0 || result.NumberOfDocumentTombstonesToProcess > 0 ||
                                    result.NumberOfCounterGroupsToProcess > 0) == false;

                if (result.Completed && performance.Completed == null)
                {
                    // note the above calculations of items to process subtract _transformed_ items in current batch, they aren't loaded yet
                    // in order to indicate that load phase is still in progress we're marking that it isn't completed yet

                    result.Completed = performance.SuccessfullyLoaded ?? false;
                }
            }

            return result;
        }

        public override void Dispose()
        {
            if (CancellationToken.IsCancellationRequested)
                return;

            var exceptionAggregator = new ExceptionAggregator(Logger, $"Could not dispose {GetType().Name}: '{Name}'");

            exceptionAggregator.Execute(() => Stop("Dispose"));

            exceptionAggregator.Execute(() => _cts.Dispose());
            exceptionAggregator.Execute(() => _waitForChanges.Dispose());

            exceptionAggregator.ThrowIfNeeded();
        }

        private class TestMode
        {
            public readonly List<string> DebugOutput = new List<string>();
        }

        public void LowMemory()
        {
            _currentMaximumAllowedMemory = DefaultMaximumMemoryAllocation;
            _lowMemoryFlag.Raise();
        }

        public void LowMemoryOver()
        {
            _lowMemoryFlag.Lower();
        }
    }
}
