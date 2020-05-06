﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Exceptions.Documents;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;

namespace Raven.Server.Documents.TimeSeries
{
    public unsafe class TimeSeriesStorage
    {
        public const int MaxSegmentSize = 2048;

        public static readonly Slice AllTimeSeriesEtagSlice;

        private static readonly Slice CollectionTimeSeriesEtagsSlice;

        private static readonly Slice TimeSeriesKeysSlice;
        private static readonly Slice PendingDeletionSegments;
        private static readonly Slice DeletedRangesKey;
        private static readonly Slice AllDeletedRangesEtagSlice;
        private static readonly Slice CollectionDeletedRangesEtagsSlice;

        private static readonly TableSchema TimeSeriesSchema = new TableSchema
        {
            TableType = (byte)TableType.TimeSeries
        };

        private static readonly TableSchema DeleteRangesSchema = new TableSchema();

        private readonly DocumentDatabase _documentDatabase;
        private readonly DocumentsStorage _documentsStorage;

        private HashSet<string> _tableCreated = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        public readonly TimeSeriesStats Stats;
        public readonly TimeSeriesRollups Rollups;

        static TimeSeriesStorage()
        {
            using (StorageEnvironment.GetStaticContext(out ByteStringContext ctx))
            {
                Slice.From(ctx, "AllTimeSeriesEtag", ByteStringType.Immutable, out AllTimeSeriesEtagSlice);
                Slice.From(ctx, "CollectionTimeSeriesEtags", ByteStringType.Immutable, out CollectionTimeSeriesEtagsSlice);
                Slice.From(ctx, "TimeSeriesKeys", ByteStringType.Immutable, out TimeSeriesKeysSlice);
                Slice.From(ctx, "PendingDeletionSegments", ByteStringType.Immutable, out PendingDeletionSegments);
                Slice.From(ctx, "DeletedRangesKey", ByteStringType.Immutable, out DeletedRangesKey);
                Slice.From(ctx, "AllDeletedRangesEtag", ByteStringType.Immutable, out AllDeletedRangesEtagSlice);
                Slice.From(ctx, "CollectionDeletedRangesEtags", ByteStringType.Immutable, out CollectionDeletedRangesEtagsSlice);
            }

            TimeSeriesSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)TimeSeriesTable.TimeSeriesKey,
                Count = 1,
                Name = TimeSeriesKeysSlice,
                IsGlobal = true
            });

            TimeSeriesSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)TimeSeriesTable.Etag,
                Name = AllTimeSeriesEtagSlice,
                IsGlobal = true
            });

            TimeSeriesSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)TimeSeriesTable.Etag,
                Name = CollectionTimeSeriesEtagsSlice
            });

            DeleteRangesSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)DeletedRangeTable.RangeKey,
                Count = 1,
                Name = DeletedRangesKey,
                IsGlobal = true
            });

            DeleteRangesSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)DeletedRangeTable.Etag,
                Name = AllDeletedRangesEtagSlice,
                IsGlobal = true
            });

            DeleteRangesSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)DeletedRangeTable.Etag,
                Name = CollectionDeletedRangesEtagsSlice
            });
        }

        public TimeSeriesStorage(DocumentDatabase documentDatabase, Transaction tx)
        {
            _documentDatabase = documentDatabase;
            _documentsStorage = documentDatabase.DocumentsStorage;

            tx.CreateTree(TimeSeriesKeysSlice);
            tx.CreateTree(DeletedRangesKey);

            Stats = new TimeSeriesStats(tx);
            Rollups = new TimeSeriesRollups(documentDatabase.Name);
        }

        public static DateTime ExtractDateTimeFromKey(Slice key)
        {
            var span = key.AsSpan();
            var timeSlice = span.Slice(span.Length - sizeof(long), sizeof(long));
            var baseline = Bits.SwapBytes(MemoryMarshal.Read<long>(timeSlice));
            return new DateTime(baseline * 10_000);
        }

        public long PurgeSegmentsAndDeletedRanges(DocumentsOperationContext context, string collection, long upto, long numberOfEntriesToDelete)
        {
            var collectionName = _documentsStorage.GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                return 0;

            var deletedSegments = PurgeSegments(upto, context, collectionName, numberOfEntriesToDelete);
            var deletedRanges = PurgeDeletedRanged(upto, context, collectionName, numberOfEntriesToDelete - deletedSegments);
            return deletedRanges + deletedSegments;
        }

        private long PurgeDeletedRanged(in long upto, DocumentsOperationContext context, CollectionName collectionName, long numberOfEntriesToDelete)
        {
            var tableName = collectionName.GetTableName(CollectionTableType.TimeSeriesDeletedRanges);
            var table = context.Transaction.InnerTransaction.OpenTable(DeleteRangesSchema, tableName);

            if (table == null || table.NumberOfEntries == 0 || numberOfEntriesToDelete <= 0)
                return 0;

            return table.DeleteBackwardFrom(DeleteRangesSchema.FixedSizeIndexes[CollectionDeletedRangesEtagsSlice], upto, numberOfEntriesToDelete);
        }

        private long PurgeSegments(long upto, DocumentsOperationContext context, CollectionName collectionName, long numberOfEntriesToDelete)
        {
            var tableName = collectionName.GetTableName(CollectionTableType.TimeSeries);
            var table = context.Transaction.InnerTransaction.OpenTable(TimeSeriesSchema, tableName);

            if (table == null || table.NumberOfEntries == 0 || numberOfEntriesToDelete <= 0)
                return 0;

            var pendingDeletion = context.Transaction.InnerTransaction.CreateTree(PendingDeletionSegments);
            var outdated = new List<Slice>();
            var hasMore = true;
            var deletedCount = 0;
            while (hasMore)
            {
                using (var it = pendingDeletion.MultiRead(collectionName.Name))
                {
                    if (it.Seek(Slices.BeforeAllKeys) == false)
                        return deletedCount;

                    do
                    {
                        var etag = it.CurrentKey.CreateReader().ReadBigEndianInt64();
                        if (etag > upto)
                        {
                            hasMore = false;
                            break;
                        }

                        if (table.DeleteByIndex(TimeSeriesSchema.FixedSizeIndexes[CollectionTimeSeriesEtagsSlice], etag)) 
                            deletedCount++;

                        outdated.Add(it.CurrentKey.Clone(context.Allocator));

                        hasMore = it.MoveNext();
                    } while (hasMore && outdated.Count < numberOfEntriesToDelete);
                }

                foreach (var etagSlice in outdated)
                {
                    pendingDeletion.MultiDelete(collectionName.Name, etagSlice);
                }

                outdated.Clear();
            }

            return deletedCount;
        }

        public class DeletionRangeRequest
        {
            public string DocumentId;
            public string Collection;
            public string Name;
            public DateTime From;
            public DateTime To;

            public override string ToString()
            {
                return $"Deletion request for time-series {Name} in document {DocumentId} from {From} to {To}";
            }
        }

        private string InsertDeletedRange(DocumentsOperationContext context, DeletionRangeRequest deletionRangeRequest, string remoteChangeVector = null)
        {
            var collection = deletionRangeRequest.Collection;
            var documentId = deletionRangeRequest.DocumentId;
            var from = deletionRangeRequest.From;
            var to = deletionRangeRequest.To;
            var name = deletionRangeRequest.Name;

            var collectionName = _documentsStorage.ExtractCollectionName(context, collection);
            var table = GetOrCreateDeleteRangesTable(context.Transaction.InnerTransaction, collectionName);

            from = EnsureMillisecondsPrecision(from);
            to = EnsureMillisecondsPrecision(to);

            string changeVector;
            long etag;
            if (remoteChangeVector != null)
            {
                changeVector = remoteChangeVector;
                etag = _documentsStorage.GenerateNextEtag();
            }
            else
            {
                (changeVector, etag) = GenerateChangeVector(context);
            }

            using (var sliceHolder = new TimeSeriesSliceHolder(context, documentId, name, collectionName.Name).WithEtag(etag))
            using (table.Allocate(out var tvb))
            using (Slice.From(context.Allocator, changeVector, out var cv))
            {
                tvb.Add(sliceHolder.TimeSeriesKeySlice);
                tvb.Add(Bits.SwapBytes(etag));
                tvb.Add(cv);
                tvb.Add(sliceHolder.CollectionSlice);
                tvb.Add(context.GetTransactionMarker());
                tvb.Add(from.Ticks);
                tvb.Add(to.Ticks);

                table.Set(tvb);
            }

            return changeVector;
        }

        public string RemoveTimestampRange(DocumentsOperationContext context, DeletionRangeRequest deletionRangeRequest, string remoteChangeVector = null)
        {
            deletionRangeRequest.From = EnsureMillisecondsPrecision(deletionRangeRequest.From);
            deletionRangeRequest.To = EnsureMillisecondsPrecision(deletionRangeRequest.To);

            InsertDeletedRange(context, deletionRangeRequest, remoteChangeVector);

            var collection = deletionRangeRequest.Collection;
            var documentId = deletionRangeRequest.DocumentId;
            var from = deletionRangeRequest.From;
            var to = deletionRangeRequest.To;
            var name = deletionRangeRequest.Name;

            var collectionName = _documentsStorage.ExtractCollectionName(context, collection);
            var table = GetOrCreateTimeSeriesTable(context.Transaction.InnerTransaction, collectionName);

            using (var slicer = new TimeSeriesSliceHolder(context, documentId, name))
            {
                var stats = Stats.GetStats(context, slicer);
                if (stats == default || stats.Count == 0)
                    return null; // nothing to delete here

                if (stats.End < from)
                    return null; // nothing to delete here

                slicer.SetBaselineToKey(stats.Start > from ? stats.Start : from);

                // first try to find the previous segment containing from value
                if (table.SeekOneBackwardByPrimaryKeyPrefix(slicer.TimeSeriesPrefixSlice, slicer.TimeSeriesKeySlice, out var segmentValueReader) == false)
                {
                    // or the first segment _after_ the from value
                    if (table.SeekOnePrimaryKeyWithPrefix(slicer.TimeSeriesPrefixSlice, slicer.TimeSeriesKeySlice, out segmentValueReader) == false)
                        return null;
                }

                var baseline = GetBaseline(segmentValueReader);
                string changeVector = null;
                bool removeOccured = false;

                while (true)
                {
                    if (TryRemoveRange(out var nextSegment) == false)
                        break;

                    if (nextSegment == null)
                        break;

                    baseline = nextSegment.Value;
                }

                if (removeOccured == false)
                    return null; // nothing happened, the deletion request was out of date

                context.Transaction.AddAfterCommitNotification(new TimeSeriesChange
                {
                    ChangeVector = changeVector, 
                    DocumentId = documentId,
                    Name = name,
                    Type = TimeSeriesChangeTypes.Delete,
                    From = from,
                    To = to,
                    CollectionName = collectionName.Name
                });

                return changeVector;

                bool TryRemoveRange(out DateTime? next)
                {
                    next = default;

                    if (baseline > to)
                        return false; // we got to the end

                    using (var holder = new TimeSeriesSegmentHolder(this, context, documentId, name, collectionName, baseline))
                    {
                        if (holder.LoadCurrentSegment() == false)
                            return false;

                        // we need to get the next segment before the actual remove, since it might lead to a split
                        next = TryGetNextBaseline();

                        var readOnlySegment = holder.ReadOnlySegment;
                        var end = readOnlySegment.GetLastTimestamp(baseline);

                        if (baseline > end)
                            return false;

                        if (remoteChangeVector != null)
                        {
                            if (ChangeVectorUtils.GetConflictStatus(remoteChangeVector, holder.ReadOnlyChangeVector) == ConflictStatus.AlreadyMerged)
                            {
                                // the deleted range is older than this segment, so we don't touch this segment
                                return false;
                            }
                        }

                        if (readOnlySegment.NumberOfLiveEntries == 0)
                            return true; // nothing to delete here

                        var newSegment = new TimeSeriesValuesSegment(holder.SliceHolder.SegmentBuffer.Ptr, MaxSegmentSize);
                        newSegment.Initialize(readOnlySegment.NumberOfValues);

                        // TODO: RavenDB-14851 manipulating segments as a whole must be done carefully in a distributed env 
                        // if (baseline >= from && end <= to) // entire segment can be deleted
                        // {
                        //     // update properly the start/end of the time-series
                        //     holder.AddNewValue(baseline, new double[readOnlySegment.NumberOfValues], Slices.Empty.AsSpan(), ref newSegment, TimeSeriesValuesSegment.Dead);
                        //     holder.AddNewValue(readOnlySegment.GetLastTimestamp(baseline), new double[readOnlySegment.NumberOfValues], Slices.Empty.AsSpan(), ref newSegment, TimeSeriesValuesSegment.Dead);
                        //     holder.AppendDeadSegment(newSegment);
                        //
                        //     removeOccured = true;
                        //     return true;
                        // }

                        var segmentChanged = false;
                        using (var enumerator = readOnlySegment.GetEnumerator(context.Allocator))
                        {
                            var state = new TimestampState[readOnlySegment.NumberOfValues];
                            var values = new double[readOnlySegment.NumberOfValues];
                            var tag = new TimeSeriesValuesSegment.TagPointer();

                            while (enumerator.MoveNext(out int ts, values, state, ref tag, out var status))
                            {
                                var current = baseline.AddMilliseconds(ts);

                                if (current > to && segmentChanged == false)
                                    return false; // we can exit here earlier

                                if (current >= from && current <= to && 
                                    status == TimeSeriesValuesSegment.Live)
                                {
                                    holder.AddNewValue(current, values, tag.AsSpan(), ref newSegment, TimeSeriesValuesSegment.Dead);
                                    removeOccured = true;
                                    segmentChanged = true;
                                    continue;
                                }

                                holder.AddExistingValue(current, values, tag.AsSpan(), ref newSegment, status);
                            }
                        }

                        if (segmentChanged)
                        {
                            holder.AppendExistingSegment(newSegment);
                            changeVector = holder.ChangeVector;
                        }

                        return end < to;
                    }
                }

                DateTime? TryGetNextBaseline()
                {
                    var offset = slicer.TimeSeriesKeySlice.Size - sizeof(long);
                    *(long*)(slicer.TimeSeriesKeySlice.Content.Ptr + offset) = Bits.SwapBytes(baseline.Ticks / 10_000);

                    foreach (var (_, tvh) in table.SeekByPrimaryKeyPrefix(slicer.TimeSeriesPrefixSlice, slicer.TimeSeriesKeySlice, 0))
                    {
                        return GetBaseline(tvh.Reader);
                    }

                    return null;
                }
            }
        }

        private static DateTime GetBaseline(TableValueReader reader)
        {
            var key = reader.Read((int)TimeSeriesTable.TimeSeriesKey, out int keySize);
            var baselineMilliseconds = Bits.SwapBytes(
                *(long*)(key + keySize - sizeof(long))
            );
            var ticks = baselineMilliseconds * 10_000;
            var baseline = new DateTime(ticks);
            return baseline;
        }

        public static void RemoveTimeSeriesNameFromMetadata(DocumentsOperationContext ctx, string docId, string tsName)
        {
            var storage = ctx.DocumentDatabase.DocumentsStorage;

            var doc = storage.Get(ctx, docId);
            if (doc == null)
                return;

            var tss = storage.TimeSeriesStorage;
            if (tss.Stats.GetStats(ctx, docId, tsName).Count > 0)
                return;

            var data = doc.Data;
            var flags = doc.Flags.Strip(DocumentFlags.FromClusterTransaction | DocumentFlags.Resolved);

            BlittableJsonReaderArray tsNames = null;
            if (doc.TryGetMetadata(out var metadata))
            {
                metadata.TryGet(Constants.Documents.Metadata.TimeSeries, out tsNames);
            }

            if (metadata == null || tsNames == null)
                return;

            var tsNamesList = new List<string>(tsNames.Length + 1);
            for (var i = 0; i < tsNames.Length; i++)
            {
                var val = tsNames.GetStringByIndex(i);
                if (val == null)
                    continue;
                tsNamesList.Add(val);
            }

            var location = tsNames.BinarySearch(tsName, StringComparison.OrdinalIgnoreCase);
            if (location < 0)
                return;

            tsNamesList.RemoveAt(location);

            data.Modifications = new DynamicJsonValue(data);
            metadata.Modifications = new DynamicJsonValue(metadata);

            if (tsNamesList.Count == 0)
            {
                metadata.Modifications.Remove(Constants.Documents.Metadata.TimeSeries);
                flags = flags.Strip(DocumentFlags.HasTimeSeries);
            }
            else
            {
                metadata.Modifications[Constants.Documents.Metadata.TimeSeries] = tsNamesList;
            }

            data.Modifications[Constants.Documents.Metadata.Key] = metadata;

            using (data)
            {
                var newDocumentData = ctx.ReadObject(doc.Data, docId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                storage.Put(ctx, docId, null, newDocumentData, flags: flags,
                    nonPersistentFlags: NonPersistentDocumentFlags.ByTimeSeriesUpdate);
            }
        }

        private static TimeSeriesValuesSegment TableValueToSegment(ref TableValueReader segmentValueReader, out DateTime baseline)
        {
            var segmentPtr = segmentValueReader.Read((int)TimeSeriesTable.Segment, out int segmentSize);
            var segment = new TimeSeriesValuesSegment(segmentPtr, segmentSize);
            Debug.Assert(segmentSize == segment.NumberOfBytes);

            var key = segmentValueReader.Read((int)TimeSeriesTable.TimeSeriesKey, out int keySize);
            var baselineMilliseconds = Bits.SwapBytes(
                *(long*)(key + keySize - sizeof(long))
            );
            var ticks = baselineMilliseconds * 10_000;
            baseline = new DateTime(ticks);
            return segment;
        }

        public static DateTime EnsureMillisecondsPrecision(DateTime dt)
        {
            var remainder = dt.Ticks % 10_000;
            if (remainder != 0)
                dt = dt.AddTicks(-remainder);

            return dt;
        }

        public void DeleteTimeSeriesForDocument(DocumentsOperationContext context, string documentId, CollectionName collection)
        {
            // this will be called as part of document's delete

            var table = GetOrCreateTimeSeriesTable(context.Transaction.InnerTransaction, collection);
            using (DocumentIdWorker.GetSliceFromId(context, documentId, out Slice documentKeyPrefix, SpecialChars.RecordSeparator))
            {
                table.DeleteByPrimaryKeyPrefix(documentKeyPrefix);
                Stats.DeleteByPrimaryKeyPrefix(context, collection, documentKeyPrefix);
            }
        }

        public Reader GetReader(DocumentsOperationContext context, string documentId, string name, DateTime from, DateTime to, TimeSpan? offset = null)
        {
            return new Reader(context, documentId, name, from, to, offset);
        }

        public class Reader
        {
            private readonly DocumentsOperationContext _context;
            private readonly string _documentId;
            private readonly string _name;
            private readonly DateTime _from, _to;
            private readonly Table _table;
            internal TableValueReader _tvr;
            private double[] _values = Array.Empty<double>();
            private TimestampState[] _states = Array.Empty<TimestampState>();
            private TimeSeriesValuesSegment.TagPointer _tagPointer;
            private LazyStringValue _tag;
            private TimeSeriesValuesSegment _currentSegment;
            private TimeSpan? _offset;

            public Reader(DocumentsOperationContext context, string documentId, string name, DateTime from, DateTime to, TimeSpan? offset)
            {
                _context = context;
                _documentId = documentId;
                _name = name;
                _table = new Table(TimeSeriesSchema, context.Transaction.InnerTransaction);
                _tag = new LazyStringValue(null, null, 0, context);
                _offset = offset;

                _from = from;
                _to = to;
            }

            internal bool Init()
            {
                using (var holder = new TimeSeriesSliceHolder(_context, _documentId, _name).WithBaseline(_from))
                {
                    if (_table.SeekOneBackwardByPrimaryKeyPrefix(holder.TimeSeriesPrefixSlice, holder.TimeSeriesKeySlice, out _tvr) == false)
                    {
                        return _table.SeekOnePrimaryKeyWithPrefix(holder.TimeSeriesPrefixSlice, holder.TimeSeriesKeySlice, out _tvr);
                    }

                    return true;
                }
            }

            public SingleResult First()
            {
                return AllValues().FirstOrDefault();
            }

            public SingleResult Last()
            {
                var date = _to;
                while (true)
                {
                    using (var holder = new TimeSeriesSliceHolder(_context, _documentId, _name).WithBaseline(date))
                    {
                        if (_table.SeekOneBackwardByPrimaryKeyPrefix(holder.TimeSeriesPrefixSlice, holder.TimeSeriesKeySlice, out _tvr) == false)
                            return null;
                    }

                    InitializeSegment(out var baseline, out var segment);
                    date = new DateTime(baseline * 10_000);
                    if (segment.NumberOfLiveEntries == 0)
                    {
                        // find the prev segment
                        date = date.AddMilliseconds(-1);
                        continue;
                    }

                    if (_to == DateTime.MaxValue)
                        return segment.YieldAllValues(_context, date, includeDead: false).Last();

                    SingleResult last = default;
                    foreach (var item in segment.YieldAllValues(_context, date, includeDead: false))
                    {
                        if (item.Timestamp > _to)
                            return last;

                        last = item;
                    }

                    return last;
                }
            }

            public class SingleResult
            {
                public DateTime Timestamp;
                public Memory<double> Values;
                public LazyStringValue Tag;
                public ulong Status;
            }

            public class SegmentResult
            {
                public DateTime Start, End;
                public StatefulTimestampValueSpan Summary;
                public string ChangeVector;
                private Reader _reader;

                public SegmentResult(Reader reader)
                {
                    _reader = reader;
                }

                public IEnumerable<SingleResult> Values => _reader.YieldSegment(Start);
            }

            internal class SeriesSummary
            {
                public SeriesSummary(int numberOfValues)
                {
                    Min = new double[numberOfValues];
                    Max = new double[numberOfValues];
                }

                public int Count { get; set; }

                public double[] Min { get; set; }

                public double[] Max { get; set; }
            }

            internal SeriesSummary GetSummary()
            {
                if (Init() == false)
                    return null;

                InitializeSegment(out _, out _currentSegment);

                var result = new SeriesSummary(_currentSegment.NumberOfValues);

                do
                {
                    if (_currentSegment.NumberOfEntries == 0)
                        continue;

                    for (int i = 0; i < _currentSegment.NumberOfValues; i++)
                    {
                        if (result.Count == 0)
                        {
                            result.Min[i] = _currentSegment.SegmentValues.Span[i].Min;
                            result.Max[i] = _currentSegment.SegmentValues.Span[i].Max;
                            continue;
                        }

                        if (double.IsNaN(_currentSegment.SegmentValues.Span[i].Min) == false)
                        {
                            result.Min[i] = Math.Min(result.Min[i], _currentSegment.SegmentValues.Span[i].Min);
                        }

                        if (double.IsNaN(_currentSegment.SegmentValues.Span[i].Max) == false)
                        {
                            result.Max[i] = Math.Max(result.Max[i], _currentSegment.SegmentValues.Span[i].Max);
                        }
                    }

                    result.Count += _currentSegment.SegmentValues.Span[0].Count;
                } while (NextSegment(out _));

                return result;
            }

            public IEnumerable<(IEnumerable<SingleResult> IndividualValues, SegmentResult Segment)> SegmentsOrValues()
            {
                if (Init() == false)
                    yield break;

                var segmentResult = new SegmentResult(this);
                InitializeSegment(out var baselineMilliseconds, out _currentSegment);

                while (true)
                {
                    var baseline = new DateTime(baselineMilliseconds * 10_000, DateTimeKind.Utc);

                    if (baseline > _to)
                        yield break;

                    if (_currentSegment.NumberOfValues != _values.Length)
                    {
                        _values = new double[_currentSegment.NumberOfValues];
                        _states = new TimestampState[_currentSegment.NumberOfValues];
                    }

                    if (_offset.HasValue)
                    {
                        baseline = DateTime.SpecifyKind(baseline, DateTimeKind.Unspecified).Add(_offset.Value);
                    }

                    segmentResult.End = _currentSegment.GetLastTimestamp(baseline);
                    segmentResult.Start = baseline;
                    segmentResult.ChangeVector = GetCurrentSegmentChangeVector();

                    if (segmentResult.Start >= _from &&
                        segmentResult.End <= _to &&
                        _currentSegment.NumberOfLiveEntries > 0)
                    {
                        // we can yield the whole segment in one go
                        segmentResult.Summary = _currentSegment.SegmentValues;
                        yield return (null, segmentResult);
                    }
                    else
                    {
                        yield return (YieldSegment(baseline), segmentResult);
                    }

                    if (NextSegment(out baselineMilliseconds) == false)
                        yield break;
                }
            }

            public IEnumerable<SingleResult> AllValues(bool includeDead = false)
            {
                if (Init() == false)
                    yield break;

                InitializeSegment(out var baselineMilliseconds, out _currentSegment);
                while (true)
                {
                    var baseline = new DateTime(baselineMilliseconds * 10_000, DateTimeKind.Utc);

                    if (baseline > _to)
                        yield break;

                    var openSegment = includeDead ? 
                        _currentSegment.NumberOfEntries > 0 : 
                        _currentSegment.NumberOfLiveEntries > 0;

                    if (openSegment)
                    {
                        if (_currentSegment.NumberOfValues > _values.Length)
                        {
                            _values = new double[_currentSegment.NumberOfValues];
                            _states = new TimestampState[_currentSegment.NumberOfValues];
                        }

                        if (_offset.HasValue)
                        {
                            baseline = DateTime.SpecifyKind(baseline, DateTimeKind.Unspecified).Add(_offset.Value);
                        }

                        foreach (var val in YieldSegment(baseline, includeDead))
                        {
                            yield return val;
                        }
                    }

                    if (NextSegment(out baselineMilliseconds) == false)
                        yield break;
                }
            }

            private IEnumerable<SingleResult> YieldSegment(DateTime baseline, bool includeDead = false)
            {
                var shouldBreak = includeDead ? _currentSegment.NumberOfEntries == 0 : _currentSegment.NumberOfLiveEntries == 0;
                if (shouldBreak)
                    yield break;

                using (var enumerator = _currentSegment.GetEnumerator(_context.Allocator))
                {
                    while (enumerator.MoveNext(out int ts, _values, _states, ref _tagPointer, out var status))
                    {
                        if (includeDead == false && 
                            status == TimeSeriesValuesSegment.Dead)
                            continue;

                        var cur = baseline.AddMilliseconds(ts);

                        if (cur > _to)
                            yield break;

                        if (cur < _from)
                            continue;

                        var tag = SetTimestampTag();

                        var end = _values.Length;
                        while (end >= 0 && double.IsNaN(_values[end - 1]))
                        {
                            end--;
                        }

                        yield return new SingleResult
                        {
                            Timestamp = cur,
                            Tag = tag,
                            Status = status,
                            Values = new Memory<double>(_values, 0, end)
                        };
                    }
                }
            }

            private LazyStringValue SetTimestampTag()
            {
                if (_tagPointer.Pointer == null)
                {
                    return null;
                }
                var lazyStringLen = BlittableJsonReaderBase.ReadVariableSizeInt(_tagPointer.Pointer, 0, out var offset);
                _tag.Renew(null, _tagPointer.Pointer + offset, lazyStringLen);
                return _tag;
            }

            public DateTime NextSegmentBaseline()
            {
                if (Init() == false)
                    return default;

                if (NextSegment(out var baselineMilliseconds) == false)
                    return default;

                return new DateTime(baselineMilliseconds * 10_000);
            }

            internal bool NextSegment(out long baselineMilliseconds)
            {
                byte* key = _tvr.Read((int)TimeSeriesTable.TimeSeriesKey, out int keySize);
                using (Slice.From(_context.Allocator, key, keySize - sizeof(long), out var prefix))
                using (Slice.From(_context.Allocator, key, keySize, out var current))
                {
                    foreach (var (nextKey, tvh) in _table.SeekByPrimaryKeyPrefix(prefix, current, 0))
                    {
                        _tvr = tvh.Reader;

                        InitializeSegment(out baselineMilliseconds, out _currentSegment);

                        return true;
                    }
                }

                baselineMilliseconds = default;
                _currentSegment = default;

                return false;
            }

            private void InitializeSegment(out long baselineMilliseconds, out TimeSeriesValuesSegment readOnlySegment)
            {
                baselineMilliseconds = ReadBaseline();
                var segmentReadOnlyBuffer = _tvr.Read((int)TimeSeriesTable.Segment, out int size);
                readOnlySegment = new TimeSeriesValuesSegment(segmentReadOnlyBuffer, size);
            }

            private long ReadBaseline()
            {
                var key = _tvr.Read((int)TimeSeriesTable.TimeSeriesKey, out int keySize);
                return Bits.SwapBytes(*(long*)(key + keySize - sizeof(long)));
            }

            public DateTime ReadBaselineAsDateTime()
            {
                return new DateTime(ReadBaseline() * 10_000);
            }

            internal string GetCurrentSegmentChangeVector()
            {
                return DocumentsStorage.TableValueToChangeVector(_context, (int)TimeSeriesTable.ChangeVector, ref _tvr);
            }

            internal (long Etag, string ChangeVector, DateTime Baseline) GetSegmentInfo()
            {
                var changeVector = GetCurrentSegmentChangeVector();
                var etag = DocumentsStorage.TableValueToEtag((int)TimeSeriesTable.Etag, ref _tvr);
                var baseline = new DateTime(ReadBaseline() * 10_000);

                return (etag, changeVector, baseline);
            }
        }

        public bool TryAppendEntireSegment(DocumentsOperationContext context, TimeSeriesReplicationItem item, string docId, LazyStringValue name, DateTime baseline)
        {
            var collectionName = _documentsStorage.ExtractCollectionName(context, item.Collection);
            return TryAppendEntireSegment(context, item.Key, docId, name, collectionName, item.ChangeVector, item.Segment, baseline);
        }

        public bool TryAppendEntireSegment(DocumentsOperationContext context, Slice key, CollectionName collectionName, TimeSeriesItem item)
        {
            return TryAppendEntireSegment(context, key, item.DocId, context.GetLazyStringForFieldWithCaching(item.Name), collectionName, item.ChangeVector, item.Segment, item.Baseline);
        }

        private bool TryAppendEntireSegment(
            DocumentsOperationContext context,
            Slice key,
            string documentId,
            string name,
            CollectionName collectionName,
            string changeVector,
            TimeSeriesValuesSegment segment,
            DateTime baseline
            )
        {
            var table = GetOrCreateTimeSeriesTable(context.Transaction.InnerTransaction, collectionName);

            if (table.ReadByKey(key, out var tvr))
            {
                var existingChangeVector = DocumentsStorage.TableValueToChangeVector(context, (int)TimeSeriesTable.ChangeVector, ref tvr);

                var status = ChangeVectorUtils.GetConflictStatus(changeVector, existingChangeVector);

                if (status == ConflictStatus.AlreadyMerged)
                    return true; // nothing to do, we already have this

                if (status == ConflictStatus.Update)
                {
                    // TODO: RavenDB-14851 currently this is not working as expected, and cause values to disappear
                    // we can put the segment directly only if the incoming segment doesn't overlap with any existing one
                    // using (Slice.From(context.Allocator, key.Content.Ptr, key.Size - sizeof(long), ByteStringType.Immutable, out var prefix))
                    // {
                    //     if (IsOverlapWithHigherSegment(prefix) == false)
                    //     {
                    //         var segmentReadOnlyBuffer = tvr.Read((int)TimeSeriesTable.Segment, out int size);
                    //         var readOnlySegment = new TimeSeriesValuesSegment(segmentReadOnlyBuffer, size);
                    //         Stats.UpdateCount(context, documentId, name, collectionName, -readOnlySegment.NumberOfLiveEntries);
                    //         
                    //         AppendEntireSegment();
                    //
                    //         return true;
                    //     }
                    // }
                }

                return false;
            }

            // if this segment isn't overlap with any other we can put it directly
            using (Slice.From(context.Allocator, key.Content.Ptr, key.Size - sizeof(long), ByteStringType.Immutable, out var prefix))
            {
                if (IsOverlapWithHigherSegment(prefix) || IsOverlapWithLowerSegment(prefix))
                    return false;

                AppendEntireSegment();
                return true;
            }

            void AppendEntireSegment()
            {
                ValidateSegment(segment);

                using (var slicer = new TimeSeriesSliceHolder(context, documentId, name, collectionName.Name))
                {
                    Stats.UpdateStats(context, slicer, collectionName, segment, baseline);

                    _documentDatabase.TimeSeriesPolicyRunner?.MarkSegmentForPolicy(context, slicer, baseline, changeVector, segment.NumberOfLiveEntries);
                    
                    var newEtag = _documentsStorage.GenerateNextEtag();
                    if (segment.NumberOfLiveEntries == 0)
                    {
                        MarkSegmentAsPendingDeletion(context, collectionName.Name, newEtag);
                    }

                    using (Slice.From(context.Allocator, changeVector, out Slice cv))
                    using (table.Allocate(out TableValueBuilder tvb))
                    {
                        tvb.Add(key);
                        tvb.Add(Bits.SwapBytes(newEtag));
                        tvb.Add(cv);
                        tvb.Add(segment.Ptr, segment.NumberOfBytes);
                        tvb.Add(slicer.CollectionSlice);
                        tvb.Add(context.GetTransactionMarker());

                        table.Set(tvb);
                    }

                    context.Transaction.AddAfterCommitNotification(new TimeSeriesChange
                    {
                        CollectionName = collectionName.Name,
                        ChangeVector = changeVector,
                        DocumentId = documentId,
                        Name = name,
                        Type = TimeSeriesChangeTypes.Put,
                        From = DateTime.MinValue,
                        To = DateTime.MaxValue
                    });
                }

                EnsureStatsAndDataIntegrity(context, documentId, name);
            }

            bool IsOverlapWithHigherSegment(Slice prefix)
            {
                var lastTimestamp = segment.GetLastTimestamp(baseline);
                var nextSegmentBaseline = BaselineOfNextSegment(table, prefix, key, baseline);
                return lastTimestamp >= nextSegmentBaseline;
            }

            bool IsOverlapWithLowerSegment(Slice prefix)
            {
                var myLastTimestamp = segment.GetLastTimestamp(baseline);
                using (Slice.From(context.Allocator, key.Content.Ptr, key.Size, ByteStringType.Immutable, out var lastKey))
                {
                    *(long*)(lastKey.Content.Ptr + lastKey.Size - sizeof(long)) = Bits.SwapBytes(myLastTimestamp.Ticks / 10_000);
                    if (table.SeekOneBackwardByPrimaryKeyPrefix(prefix, lastKey, out tvr) == false)
                    {
                        return false;
                    }
                }

                var prevSegment = TableValueToSegment(ref tvr, out var prevBaseline);
                var last = prevSegment.GetLastTimestamp(prevBaseline);
                return last >= baseline;
            }
        }

        private static DateTime? BaselineOfNextSegment(TimeSeriesSegmentHolder segmentHolder, DateTime myDate)
        {
            var table = segmentHolder.Table;
            var prefix = segmentHolder.SliceHolder.TimeSeriesPrefixSlice;
            var key = segmentHolder.SliceHolder.TimeSeriesKeySlice;

            return BaselineOfNextSegment(table, prefix, key, myDate);
        }

        private static DateTime? BaselineOfNextSegment(Table table, Slice prefix, Slice key, DateTime myDate)
        {
            if (table.SeekOnePrimaryKeyWithPrefix(prefix, key, out var reader))
            {
                var currentKey = reader.Read((int)TimeSeriesTable.TimeSeriesKey, out var keySize);
                var baseline = Bits.SwapBytes(
                    *(long*)(currentKey + keySize - sizeof(long))
                );
                var date = new DateTime(baseline * 10_000);
                if (date > myDate)
                    return date;

                foreach (var (_, holder) in table.SeekByPrimaryKeyPrefix(prefix, key, 0))
                {
                    currentKey = holder.Reader.Read((int)TimeSeriesTable.TimeSeriesKey, out keySize);
                    baseline = Bits.SwapBytes(
                        *(long*)(currentKey + keySize - sizeof(long))
                    );
                    return new DateTime(baseline * 10_000);
                }
            }

            return null;
        }

        public class TimeSeriesSegmentHolder : IDisposable
        {
            private readonly TimeSeriesStorage _tss;
            private readonly DocumentsOperationContext _context;
            public readonly TimeSeriesSliceHolder SliceHolder;
            public readonly bool FromReplication;
            private readonly string _docId;
            private readonly CollectionName _collection;
            private readonly string _name;

            private TableValueReader _tvr;

            public long BaselineMilliseconds => BaselineDate.Ticks / 10_000;
            public DateTime BaselineDate;
            public TimeSeriesValuesSegment ReadOnlySegment;
            public string ReadOnlyChangeVector;

            private long _currentEtag;

            private string _currentChangeVector;
            public string ChangeVector => _currentChangeVector;

            private AllocatedMemoryData _clonedReadonlySegment;

            public TimeSeriesSegmentHolder(
                TimeSeriesStorage tss,
                DocumentsOperationContext context,
                string docId,
                string name,
                CollectionName collection,
                DateTime timeStamp,
                string fromReplicationChangeVector = null
                )
            {
                _tss = tss;
                _context = context;
                _collection = collection;
                _docId = docId;
                _name = name;

                SliceHolder = new TimeSeriesSliceHolder(_context, docId, name, _collection.Name).WithBaseline(timeStamp);
                SliceHolder.CreateSegmentBuffer();

                FromReplication = fromReplicationChangeVector != null;
                _tss.GenerateChangeVector(_context, fromReplicationChangeVector); // update the database change vector
            }

            public TimeSeriesSegmentHolder(
                TimeSeriesStorage tss,
                DocumentsOperationContext context,
                TimeSeriesSliceHolder allocator,
                string docId,
                string name,
                CollectionName collection,
                string fromReplicationChangeVector)
            {
                _tss = tss;
                _context = context;
                SliceHolder = allocator;
                _collection = collection;
                _docId = docId;
                _name = name;
                
                FromReplication = fromReplicationChangeVector != null;

                BaselineDate = allocator.CurrentBaseline;
                allocator.CreateSegmentBuffer();
                _tss.GenerateChangeVector(_context, fromReplicationChangeVector); // update the database change vector
            }

            private void Initialize()
            {
                Debug.Assert(_tvr.Equals(default) == false);
                var readOnlySegment = TableValueToSegment(ref _tvr, out BaselineDate);

                // while appending or deleting, we might change the same segment.
                // So we clone it.
                Debug.Assert(_clonedReadonlySegment == null);
                _clonedReadonlySegment = readOnlySegment.Clone(_context, out ReadOnlySegment);
                ReadOnlyChangeVector = DocumentsStorage.TableValueToChangeVector(_context, (int)TimeSeriesTable.ChangeVector, ref _tvr);

                SliceHolder.SetBaselineToKey(BaselineDate);

                _countWasReduced = false;
            }

            public Table Table => _tss.GetOrCreateTimeSeriesTable(_context.Transaction.InnerTransaction, _collection);

            private bool _countWasReduced;

            private void ReduceCountBeforeAppend()
            {
                if (_countWasReduced)
                    return;

                // we modified this segment so we need to reduce the original count
                _countWasReduced = true;
                _tss.Stats.UpdateCount(_context, SliceHolder, _collection, -ReadOnlySegment.NumberOfLiveEntries);
            }

            public void AppendExistingSegment(TimeSeriesValuesSegment newValueSegment)
            {
                (_currentChangeVector, _currentEtag) = _tss.GenerateChangeVector(_context);

                ValidateSegment(newValueSegment);
                if (newValueSegment.NumberOfLiveEntries == 0)
                {
                    MarkSegmentAsPendingDeletion(_context, _collection.Name, _currentEtag);
                }

                ReduceCountBeforeAppend();
                _tss.Stats.UpdateStats(_context, SliceHolder, _collection, newValueSegment, BaselineDate);

                using (Table.Allocate(out var tvb))
                using (Slice.From(_context.Allocator, _currentChangeVector, out var cv))
                {
                    tvb.Add(SliceHolder.TimeSeriesKeySlice);
                    tvb.Add(Bits.SwapBytes(_currentEtag));
                    tvb.Add(cv);
                    tvb.Add(newValueSegment.Ptr, newValueSegment.NumberOfBytes);
                    tvb.Add(SliceHolder.CollectionSlice);
                    tvb.Add(_context.GetTransactionMarker());

                    Table.Set(tvb);
                }

                EnsureStatsAndDataIntegrity(_context, _docId, _name);
            }

            public void AppendDeadSegment(TimeSeriesValuesSegment newValueSegment)
            {
                (_currentChangeVector, _currentEtag) = _tss.GenerateChangeVector(_context);

                ValidateSegment(newValueSegment);
                MarkSegmentAsPendingDeletion(_context, _collection.Name, _currentEtag);

                ReduceCountBeforeAppend();
                _tss.Stats.UpdateStats(_context, SliceHolder, _collection, newValueSegment, BaselineDate);

                using (Table.Allocate(out var tvb))
                {
                    tvb.Add(SliceHolder.TimeSeriesKeySlice);
                    tvb.Add(Bits.SwapBytes(_currentEtag));
                    tvb.Add(Slices.Empty); // we put empty slice in the change-vector, so it wouldn't replicate
                    tvb.Add(newValueSegment.Ptr, newValueSegment.NumberOfBytes);
                    tvb.Add(SliceHolder.CollectionSlice);
                    tvb.Add(_context.GetTransactionMarker());

                    Table.Set(tvb);
                }

                EnsureStatsAndDataIntegrity(_context, _docId, _name);
            }
            
            public void AppendToNewSegment(Reader.SingleResult item)
            {
                BaselineDate = EnsureMillisecondsPrecision(item.Timestamp);
                SliceHolder.SetBaselineToKey(BaselineDate);

                var newSegment = new TimeSeriesValuesSegment(SliceHolder.SegmentBuffer.Ptr, MaxSegmentSize);
                newSegment.Initialize(item.Values.Length);
                newSegment.Append(_context.Allocator, 0, item.Values.Span, SliceHolder.TagAsSpan(item.Tag), item.Status);

                ValidateSegment(newSegment);

                _tss.Stats.UpdateStats(_context, SliceHolder, _collection, newSegment, BaselineDate);
                _tss._documentDatabase.TimeSeriesPolicyRunner?.MarkForPolicy(_context, SliceHolder, BaselineDate, item.Status);

                (_currentChangeVector, _currentEtag) = _tss.GenerateChangeVector(_context);

                using (Slice.From(_context.Allocator, _currentChangeVector, out Slice cv))
                using (Table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(SliceHolder.TimeSeriesKeySlice);
                    tvb.Add(Bits.SwapBytes(_currentEtag));
                    tvb.Add(cv);
                    tvb.Add(newSegment.Ptr, newSegment.NumberOfBytes);
                    tvb.Add(SliceHolder.CollectionSlice);
                    tvb.Add(_context.GetTransactionMarker());

                    Table.Insert(tvb);
                }

                EnsureStatsAndDataIntegrity(_context, _docId, _name);
            }

            public void AddNewValue(Reader.SingleResult result, ref TimeSeriesValuesSegment segment)
            {
                AddNewValue(result.Timestamp, result.Values.Span, SliceHolder.TagAsSpan(result.Tag), ref segment, result.Status);
            }

            public void AddNewValue(DateTime time, Span<double> values, Span<byte> tagSlice, ref TimeSeriesValuesSegment segment, ulong status)
            {
                AddValueInternal(time, values, tagSlice, ref segment, status);
                _context.DocumentDatabase.TimeSeriesPolicyRunner?.MarkForPolicy(_context, SliceHolder, time, status);
            }

            public void AddExistingValue(DateTime time, Span<double> values, Span<byte> tagSlice, ref TimeSeriesValuesSegment segment, ulong status)
            {
                AddValueInternal(time, values, tagSlice, ref segment, status);
            }

            private void AddValueInternal(DateTime time, Span<double> values, Span<byte> tagSlice, ref TimeSeriesValuesSegment segment, ulong status)
            {
                var timestampDiff = (int)((time - BaselineDate).Ticks / 10_000);
                if (segment.Append(_context.Allocator, timestampDiff, values, tagSlice, status) == false)
                {
                    FlushCurrentSegment(ref segment, values, tagSlice, status);
                    UpdateBaseline(timestampDiff);
                }
            }

            public bool LoadCurrentSegment()
            {
                if (Table.SeekOneBackwardByPrimaryKeyPrefix(SliceHolder.TimeSeriesPrefixSlice, SliceHolder.TimeSeriesKeySlice, out _tvr))
                {
                    Initialize();
                    return true;
                }

                return false;
            }

            private void FlushCurrentSegment(
                ref TimeSeriesValuesSegment splitSegment,
                Span<double> currentValues,
                Span<byte> currentTag,
                ulong status)
            {
                AppendExistingSegment(splitSegment);

                splitSegment.Initialize(currentValues.Length);

                var result = splitSegment.Append(_context.Allocator, 0, currentValues, currentTag, status);
                if (result == false)
                    throw new InvalidOperationException($"After renewal of segment, was unable to append a new value. Shouldn't happen. Doc: {_docId}, name: {_name}");
            }

            public void UpdateBaseline(long timestampDiff)
            {
                Debug.Assert(timestampDiff > 0);
                BaselineDate = BaselineDate.AddMilliseconds(timestampDiff);
                SliceHolder.SetBaselineToKey(BaselineDate);
            }

            public void Dispose()
            {
                SliceHolder.Dispose();

                if (_clonedReadonlySegment != null)
                {
                    _context.ReturnMemory(_clonedReadonlySegment);
                }
            }
        }

        public string AppendTimestamp(
            DocumentsOperationContext context,
            string documentId,
            string collection,
            string name,
            IEnumerable<TimeSeriesOperation.AppendOperation> toAppend,
            string changeVectorFromReplication = null)
        {
            var holder = new Reader.SingleResult();

            return AppendTimestamp(context, documentId, collection, name, toAppend.Select(ToResult), changeVectorFromReplication);

            Reader.SingleResult ToResult(TimeSeriesOperation.AppendOperation element)
            {
                holder.Values = element.Values;
                holder.Tag = context.GetLazyString(element.Tag);
                holder.Timestamp = element.Timestamp;
                holder.Status = TimeSeriesValuesSegment.Live;
                return holder;
            }
        }

        private class AppendEnumerator : IEnumerator<Reader.SingleResult>
        {
            private readonly bool _fromReplication;
            private readonly IEnumerator<Reader.SingleResult> _toAppend;
            private Reader.SingleResult _current;

            public AppendEnumerator(IEnumerable<Reader.SingleResult> toAppend, bool fromReplication)
            {
                _fromReplication = fromReplication;
                _toAppend = toAppend.GetEnumerator();
            }

            public bool MoveNext()
            {
                var currentTimestamp = _current?.Timestamp;
                if (_toAppend.MoveNext() == false)
                {
                    _current = null;
                    return false;
                }

                var next = _toAppend.Current;
                if (currentTimestamp >= next.Timestamp)
                    throw new InvalidDataException("TimeSeries entries must be sorted by their timestamps, and cannot contain duplicate timestamps. " +
                                                   $"Got: current '{currentTimestamp}', next '{next.Timestamp}'.");

                if (_fromReplication == false)
                {
                    AssertNoNanValue(next);
                }

                _current = next;
                return true;
            }

            public void Reset()
            {
                throw new NotImplementedException();
            }

            object IEnumerator.Current => _current;

            public Reader.SingleResult Current => _current;

            public void Dispose()
            {
                _toAppend.Dispose();
            }
        }

        public string AppendTimestamp(
            DocumentsOperationContext context,
            string documentId,
            string collection,
            string name,
            IEnumerable<Reader.SingleResult> toAppend,
            string changeVectorFromReplication = null,
            bool verifyName = true
            )
        {
            if (context.Transaction == null)
            {
                DocumentPutAction.ThrowRequiresTransaction();
                Debug.Assert(false); // never hit
            }

            var collectionName = _documentsStorage.ExtractCollectionName(context, collection);
            var newSeries = Stats.GetStats(context, documentId, name).Count == 0;
            if (newSeries && verifyName)
            {
                VerifyLegalName(name);
            }

            using (var appendEnumerator = new AppendEnumerator(toAppend, changeVectorFromReplication != null))
            {
                while (appendEnumerator.MoveNext())
                {
                    var retry = true;
                    while (retry)
                    {
                        retry = false;
                        var current = appendEnumerator.Current;
                        Debug.Assert(current != null);

                        if (changeVectorFromReplication == null)
                        {
                            // not from replication
                            AssertNoNanValue(current);
                        }

                        current.Timestamp = EnsureMillisecondsPrecision(current.Timestamp);

                        using (var slicer = new TimeSeriesSliceHolder(context, documentId, name, collection).WithBaseline(current.Timestamp))
                        {
                            var segmentHolder = new TimeSeriesSegmentHolder(this, context, slicer, documentId, name, collectionName, changeVectorFromReplication);
                            if (segmentHolder.LoadCurrentSegment() == false)
                            {
                                // no matches for this series at all, need to create new segment
                                segmentHolder.AppendToNewSegment(current);
                                break;
                            }

                            EnsureNumberOfValues(segmentHolder.ReadOnlySegment.NumberOfValues, ref current);

                            if (TryAppendToCurrentSegment(context, segmentHolder, appendEnumerator, out var newValueFetched))
                                break;

                            if (newValueFetched)
                            {
                                retry = true;
                                continue;
                            }

                            if (ValueTooFar(segmentHolder, current))
                            {
                                segmentHolder.AppendToNewSegment(appendEnumerator.Current);
                                break;
                            }

                            retry = SplitSegment(context, segmentHolder, appendEnumerator, current);
                        }
                    }
                }
            }

            if (newSeries)
            {
                var stats = Stats.GetStats(context, documentId, name);
                if (stats.Count > 0)
                    AddTimeSeriesNameToMetadata(context, documentId, name);
            }

            context.Transaction.AddAfterCommitNotification(new TimeSeriesChange
            {
                CollectionName = collectionName.Name,
                ChangeVector = context.LastDatabaseChangeVector,
                DocumentId = documentId,
                Name = name,
                Type = TimeSeriesChangeTypes.Put,
                From = DateTime.MinValue,
                To = DateTime.MaxValue
            });

            return context.LastDatabaseChangeVector;
        }

        private static void VerifyLegalName(string name)
        {
            for (int i = 0; i < name.Length; i++)
            {
                if (name[i] == TimeSeriesConfiguration.TimeSeriesRollupSeparator)
                    throw new InvalidOperationException($"Illegal time series name : '{name}'. " +
                                                        $"Time series names cannot contain '{TimeSeriesConfiguration.TimeSeriesRollupSeparator}' character, " +
                                                        "since this character is reserved for time series rollups.");
            }
        }

        private bool ValueTooFar(TimeSeriesSegmentHolder segmentHolder, Reader.SingleResult current)
        {
            var deltaInMs = (current.Timestamp.Ticks / 10_000) - segmentHolder.BaselineMilliseconds;
            return deltaInMs >= int.MaxValue;
        }

        private static bool EnsureNumberOfValues(int segmentNumberOfValues, ref Reader.SingleResult current)
        {
            if (segmentNumberOfValues > current.Values.Length)
            {
                var updatedValues = new Memory<double>(new double[segmentNumberOfValues]);
                current.Values.CopyTo(updatedValues);

                for (int i = current.Values.Length; i < updatedValues.Length; i++)
                {
                    updatedValues.Span[i] = double.NaN;
                }

                // we create a new instance to avoid having NaN in the case of re-appending
                current = new Reader.SingleResult
                {
                    Timestamp = current.Timestamp,
                    Values = updatedValues,
                    Status = current.Status,
                    Tag = current.Tag
                };
            }

            return segmentNumberOfValues == current.Values.Length;
        }

        private bool TryAppendToCurrentSegment(
            DocumentsOperationContext context,
            TimeSeriesSegmentHolder segmentHolder,
            IEnumerator<Reader.SingleResult> appendEnumerator,
            out bool newValueFetched)
        {
            var segment = segmentHolder.ReadOnlySegment;
            var slicer = segmentHolder.SliceHolder;

            var current = appendEnumerator.Current;
            var lastTimestamp = segment.GetLastTimestamp(segmentHolder.BaselineDate);
            var nextSegmentBaseline = BaselineOfNextSegment(segmentHolder, current.Timestamp) ?? DateTime.MaxValue;

            TimeSeriesValuesSegment newSegment = default;
            newValueFetched = false;
            while (true)
            {
                var canAppend = current.Timestamp > lastTimestamp && segment.NumberOfValues == current.Values.Length;
                var deltaInMs = (current.Timestamp.Ticks / 10_000) - segmentHolder.BaselineMilliseconds;
                var inRange = deltaInMs < int.MaxValue;

                if (canAppend && inRange) // if the range is too big (over 24.85 days, using ms precision), we need a new segment
                {
                    // this is the simplest scenario, we can just add it.
                    if (newValueFetched == false)
                    {
                        segment.CopyTo(slicer.SegmentBuffer.Ptr);
                        newSegment = new TimeSeriesValuesSegment(slicer.SegmentBuffer.Ptr, MaxSegmentSize);
                    }

                    // checking if we run out of space here, in which can we'll create new segment
                    if (newSegment.Append(context.Allocator, (int)deltaInMs, current.Values.Span, slicer.TagAsSpan(current.Tag), current.Status))
                    {
                        _documentDatabase.TimeSeriesPolicyRunner?.MarkForPolicy(context, segmentHolder.SliceHolder, current.Timestamp, current.Status);

                        newValueFetched = true;
                        if (appendEnumerator.MoveNext() == false)
                        {
                            segmentHolder.AppendExistingSegment(newSegment);
                            return true;
                        }
                        
                        current = appendEnumerator.Current;
                        if (current.Timestamp < nextSegmentBaseline)
                        {
                            if (EnsureNumberOfValues(newSegment.NumberOfValues, ref current))
                                continue;
                        }

                        canAppend = false;
                    }
                }

                if (newValueFetched)
                {
                    segmentHolder.AppendExistingSegment(newSegment);
                    return false;
                }

                if (canAppend)
                {
                    // either the range is too high to fit in a single segment (~25 days) or the
                    // previous segment is full, we can just create a completely new segment with the
                    // new value
                    segmentHolder.AppendToNewSegment(appendEnumerator.Current);
                    return true;
                }

                return false;
            }
        }

        private bool SplitSegment(
            DocumentsOperationContext context,
            TimeSeriesSegmentHolder timeSeriesSegment,
            IEnumerator<Reader.SingleResult> reader,
            Reader.SingleResult current)
        {
            // here we have a complex scenario, we need to add it in the middle of the current segment
            // to do that, we have to re-create it from scratch.

            // the first thing to do here it to copy the segment out, because we may be writing it in multiple
            // steps, and move the actual values as we do so

            var originalBaseline = timeSeriesSegment.BaselineDate;
            var nextSegmentBaseline = BaselineOfNextSegment(timeSeriesSegment, current.Timestamp);
            var segmentToSplit = timeSeriesSegment.ReadOnlySegment;
            var segmentChanged = false;
            var additionalValueSize = Math.Max(0, current.Values.Length - timeSeriesSegment.ReadOnlySegment.NumberOfValues);
            var newNumberOfValues = additionalValueSize + timeSeriesSegment.ReadOnlySegment.NumberOfValues;

            using (context.Allocator.Allocate(segmentToSplit.NumberOfBytes, out var currentSegmentBuffer))
            {
                Memory.Copy(currentSegmentBuffer.Ptr, segmentToSplit.Ptr, segmentToSplit.NumberOfBytes);
                var readOnlySegment = new TimeSeriesValuesSegment(currentSegmentBuffer.Ptr, segmentToSplit.NumberOfBytes);

                var splitSegment = new TimeSeriesValuesSegment(timeSeriesSegment.SliceHolder.SegmentBuffer.Ptr, MaxSegmentSize);
                splitSegment.Initialize(current.Values.Span.Length);
                using (context.Allocator.Allocate(newNumberOfValues * sizeof(double), out var valuesBuffer))
                using (context.Allocator.Allocate(readOnlySegment.NumberOfValues * sizeof(TimestampState), out var stateBuffer))
                {
                    Memory.Set(valuesBuffer.Ptr, 0, valuesBuffer.Length);
                    Memory.Set(stateBuffer.Ptr, 0, stateBuffer.Length);

                    var currentValues = new Span<double>(valuesBuffer.Ptr, readOnlySegment.NumberOfValues);
                    var updatedValues = new Span<double>(valuesBuffer.Ptr, newNumberOfValues);
                    var state = new Span<TimestampState>(stateBuffer.Ptr, readOnlySegment.NumberOfValues);
                    var currentTag = new TimeSeriesValuesSegment.TagPointer();

                    for (int i = readOnlySegment.NumberOfValues; i < newNumberOfValues; i++)
                    {
                        updatedValues[i] = double.NaN;
                    }

                    using (var enumerator = readOnlySegment.GetEnumerator(context.Allocator))
                    {
                        while (enumerator.MoveNext(out var currentTimestamp, currentValues, state, ref currentTag, out var localStatus))
                        {
                            var currentTime = originalBaseline.AddMilliseconds(currentTimestamp);
                            while (true)
                            {
                                var compare = Compare(currentTime, currentValues, localStatus, current, nextSegmentBaseline, timeSeriesSegment);
                                if (compare != CompareResult.Remote)
                                {
                                    timeSeriesSegment.AddExistingValue(currentTime, updatedValues, currentTag.AsSpan(), ref splitSegment, localStatus);
                                    if (currentTime == current?.Timestamp)
                                    {
                                        reader.MoveNext();
                                        current = reader.Current;
                                    }
                                    break;
                                }

                                segmentChanged = true;
                                Debug.Assert(current != null);

                                if (EnsureNumberOfValues(newNumberOfValues, ref current) == false)
                                {
                                    // the next value to append has a larger number of values.
                                    // we need to append the rest of the open segment and only then we can re-append this value.
                                    timeSeriesSegment.AddNewValue(currentTime, updatedValues, currentTag.AsSpan(), ref splitSegment, localStatus);
                                    while (enumerator.MoveNext(out currentTimestamp, currentValues, state, ref currentTag, out localStatus))
                                        timeSeriesSegment.AddNewValue(currentTime, updatedValues, currentTag.AsSpan(), ref splitSegment, localStatus);

                                    timeSeriesSegment.AppendExistingSegment(splitSegment);
                                    return true;
                                }

                                timeSeriesSegment.AddNewValue(current, ref splitSegment);

                                if (currentTime == current.Timestamp)
                                {
                                    reader.MoveNext();
                                    current = reader.Current;
                                    break; // the local value was overwritten
                                }
                                reader.MoveNext();
                                current = reader.Current;
                            }
                        }
                    }

                    var retryAppend = current != null;
                    if (retryAppend && (current.Timestamp >= nextSegmentBaseline == false))
                    {
                        timeSeriesSegment.AddNewValue(current, ref splitSegment);
                        segmentChanged = true;
                        retryAppend = false;
                    }

                    if (segmentChanged == false)
                        return false;

                    timeSeriesSegment.AppendExistingSegment(splitSegment);
                    return retryAppend;
                }
            }
        }

        private enum CompareResult
        {
            Local,
            Equal,
            Remote
        }

        private static CompareResult Compare(DateTime localTime, Span<double> localValues, ulong localStatus, Reader.SingleResult remote, DateTime? nextSegmentBaseline, TimeSeriesSegmentHolder holder)
        {
            if (remote == null)
                return CompareResult.Local;

            if (localTime < remote.Timestamp)
                return CompareResult.Local;

            if (remote.Timestamp >= nextSegmentBaseline)
                return CompareResult.Local;

            if (localTime == remote.Timestamp)
            {
                if (holder.FromReplication == false)
                    return CompareResult.Remote; // if not from replication, remote value is an update

                if (localValues.Length != remote.Values.Length)
                {
                    // larger number of values wins
                    if (localValues.Length > remote.Values.Length)
                        return CompareResult.Local;

                    return CompareResult.Remote;
                }

                // deletion wins
                if (localStatus == TimeSeriesValuesSegment.Dead)
                {
                    if (remote.Status == TimeSeriesValuesSegment.Dead)
                        return CompareResult.Equal;

                    return CompareResult.Local;
                } 

                if (remote.Status == TimeSeriesValuesSegment.Dead) // deletion wins
                    return CompareResult.Remote;

                var compare = localValues.SequenceCompareTo(remote.Values.Span);
                if (compare == 0)
                    return CompareResult.Equal;
                if (compare < 0)
                    return CompareResult.Remote;
                return CompareResult.Local;
            }

            return CompareResult.Remote;
        }

        public void AddTimeSeriesNameToMetadata(DocumentsOperationContext ctx, string docId, string tsName)
        {
            var tss = _documentDatabase.DocumentsStorage.TimeSeriesStorage;
            if (tss.Stats.GetStats(ctx, docId, tsName).Count == 0)
                return;

            var doc = _documentDatabase.DocumentsStorage.Get(ctx, docId);
            if (doc == null)
                return;

            tsName = EnsureOriginalName(ctx, docId, tsName);

            var data = doc.Data;
            BlittableJsonReaderArray tsNames = null;
            if (doc.TryGetMetadata(out var metadata))
            {
                metadata.TryGet(Constants.Documents.Metadata.TimeSeries, out tsNames);
            }

            if (tsNames == null)
            {
                data.Modifications = new DynamicJsonValue(data);
                if (metadata == null)
                {
                    data.Modifications[Constants.Documents.Metadata.Key] =
                        new DynamicJsonValue { [Constants.Documents.Metadata.TimeSeries] = new[] { tsName } };
                }
                else
                {
                    metadata.Modifications = new DynamicJsonValue(metadata) { [Constants.Documents.Metadata.TimeSeries] = new[] { tsName } };
                    data.Modifications[Constants.Documents.Metadata.Key] = metadata;
                }
            }
            else
            {
                var tsNamesList = new List<string>(tsNames.Length + 1);
                for (var i = 0; i < tsNames.Length; i++)
                {
                    var val = tsNames.GetStringByIndex(i);
                    if (val == null)
                        continue;
                    tsNamesList.Add(val);
                }

                var location = tsNames.BinarySearch(tsName, StringComparison.OrdinalIgnoreCase);
                if (location >= 0)
                    return;

                tsNamesList.Insert(~location, tsName);

                data.Modifications = new DynamicJsonValue(data);

                metadata.Modifications = new DynamicJsonValue(metadata) { [Constants.Documents.Metadata.TimeSeries] = tsNamesList };

                data.Modifications[Constants.Documents.Metadata.Key] = metadata;
            }

            var flags = doc.Flags.Strip(DocumentFlags.FromClusterTransaction | DocumentFlags.Resolved);
            flags |= DocumentFlags.HasTimeSeries;

            using (data)
            {
                var newDocumentData = ctx.ReadObject(doc.Data, docId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                _documentDatabase.DocumentsStorage.Put(ctx, docId, null, newDocumentData, flags: flags, nonPersistentFlags: NonPersistentDocumentFlags.ByTimeSeriesUpdate);
            }
        }

        private static string EnsureOriginalName(DocumentsOperationContext ctx, string docId, string tsName)
        {
            var parts = tsName.Split(TimeSeriesConfiguration.TimeSeriesRollupSeparator);
            if (parts.Length == 1)
                return tsName;

            if (parts.Length == 2)
            {
                var raw = GetOriginalName(ctx, docId, parts[0]);
                return $"{raw}{TimeSeriesConfiguration.TimeSeriesRollupSeparator}{parts[1]}";
            }

            Debug.Assert(false, $"The time-series '{tsName}' in document '{docId}', has invalid number of parts ({parts.Length}).");
            return tsName;
        }

        public static string GetOriginalName(DocumentsOperationContext context, string docId, string lowerName)
        {
            try
            {
                using (DocumentIdWorker.GetLower(context.Allocator, docId, out var docIdSlice))
                {
                    if (context.DocumentDatabase.DocumentsStorage.GetTableValueReaderForDocument(context, docIdSlice, throwOnConflict: true, out var tvr) == false)
                        throw new InvalidOperationException($"Can't find document '{docId}'");

                    var doc = new BlittableJsonReaderObject(tvr.Read((int)DocumentsStorage.DocumentsTable.Data, out int size), size, context);
                    using (doc)
                    {
                        var name = GetOriginalName(lowerName, doc);
                        if (name == null)
                            throw new InvalidOperationException($"Can't find the time-series '{lowerName}' in document '{docId}'");
                        return name;
                    }
                }
            }
            catch (DocumentConflictException)
            {
                // will try to get it from the conflict storage
                foreach (var conflict in context.DocumentDatabase.DocumentsStorage.ConflictsStorage.GetConflictsFor(context, lowerName))
                {
                    if (conflict.Doc == null)
                        continue;

                    using (conflict.Doc)
                    {
                        var name = GetOriginalName(lowerName, conflict.Doc);
                        if (name == null)
                            continue;
                        return name;
                    }
                }
                throw new InvalidOperationException($"Can't find the time-series '{lowerName}' in conflicted documents '{docId}'");
            }
        }

        public static string GetOriginalName(string lowerName, BlittableJsonReaderObject doc)
        {
            if (doc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false)
                return null;

            if (metadata.TryGet(Constants.Documents.Metadata.TimeSeries, out BlittableJsonReaderArray ts) == false)
                return null;

            foreach (LazyStringValue item in ts)
            {
                if (string.Equals(item, lowerName, StringComparison.InvariantCultureIgnoreCase))
                    return item;
            }

            return null;
        }

        public IEnumerable<TimeSeriesReplicationItem> GetSegmentsFrom(DocumentsOperationContext context, long etag)
        {
            var table = new Table(TimeSeriesSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(TimeSeriesSchema.FixedSizeIndexes[AllTimeSeriesEtagSlice], etag, 0))
            {
                yield return CreateTimeSeriesSegmentItem(context, ref result.Reader);
            }
        }

        private static TimeSeriesReplicationItem CreateTimeSeriesSegmentItem(DocumentsOperationContext context, ref TableValueReader reader)
        {
            var etag = *(long*)reader.Read((int)TimeSeriesTable.Etag, out _);
            var changeVectorPtr = reader.Read((int)TimeSeriesTable.ChangeVector, out int changeVectorSize);
            var segmentPtr = reader.Read((int)TimeSeriesTable.Segment, out int segmentSize);

            var item = new TimeSeriesReplicationItem
            {
                Type = ReplicationBatchItem.ReplicationItemType.TimeSeriesSegment,
                ChangeVector = Encoding.UTF8.GetString(changeVectorPtr, changeVectorSize),
                Segment = new TimeSeriesValuesSegment(segmentPtr, segmentSize),
                Collection = DocumentsStorage.TableValueToId(context, (int)TimeSeriesTable.Collection, ref reader),
                Etag = Bits.SwapBytes(etag),
                TransactionMarker = DocumentsStorage.TableValueToShort((int)TimeSeriesTable.TransactionMarker, nameof(TimeSeriesTable.TransactionMarker), ref reader)
            };

            var keyPtr = reader.Read((int)TimeSeriesTable.TimeSeriesKey, out int keySize);
            item.ToDispose(Slice.From(context.Allocator, keyPtr, keySize, ByteStringType.Immutable, out item.Key));

            return item;
        }

        public IEnumerable<TimeSeriesDeletedRangeItem> GetDeletedRangesFrom(DocumentsOperationContext context, long etag)
        {
            var table = new Table(DeleteRangesSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(DeleteRangesSchema.FixedSizeIndexes[AllDeletedRangesEtagSlice], etag, 0))
            {
                yield return CreateDeletedRangeItem(context, ref result.Reader);
            }
        }

        private static TimeSeriesDeletedRangeItem CreateDeletedRangeItem(DocumentsOperationContext context, ref TableValueReader reader)
        {
            var etag = *(long*)reader.Read((int)DeletedRangeTable.Etag, out _);
            var changeVectorPtr = reader.Read((int)DeletedRangeTable.ChangeVector, out int changeVectorSize);

            var item = new TimeSeriesDeletedRangeItem
            {
                Type = ReplicationBatchItem.ReplicationItemType.DeletedTimeSeriesRange,
                ChangeVector = Encoding.UTF8.GetString(changeVectorPtr, changeVectorSize),
                Collection = DocumentsStorage.TableValueToId(context, (int)DeletedRangeTable.Collection, ref reader),
                Etag = Bits.SwapBytes(etag),
                TransactionMarker = DocumentsStorage.TableValueToShort((int)DeletedRangeTable.TransactionMarker, nameof(DeletedRangeTable.TransactionMarker), ref reader),
                From = DocumentsStorage.TableValueToDateTime((int)DeletedRangeTable.From, ref reader),
                To = DocumentsStorage.TableValueToDateTime((int)DeletedRangeTable.To, ref reader),
            };

            var keyPtr = reader.Read((int)DeletedRangeTable.RangeKey, out int keySize);
            item.ToDispose(Slice.From(context.Allocator, keyPtr, keySize - sizeof(long), ByteStringType.Immutable, out item.Key));

            return item;
        }

        public TimeSeriesSegmentEntry GetTimeSeries(DocumentsOperationContext context, Slice key)
        {
            var table = new Table(TimeSeriesSchema, context.Transaction.InnerTransaction);

            if (table.ReadByKey(key, out var reader) == false)
                return null;

            return CreateTimeSeriesItem(context, ref reader);
        }

        public TimeSeriesSegmentEntry GetTimeSeries(DocumentsOperationContext context, long etag)
        {
            var table = new Table(TimeSeriesSchema, context.Transaction.InnerTransaction);
            var index = TimeSeriesSchema.FixedSizeIndexes[AllTimeSeriesEtagSlice];

            if (table.Read(context.Allocator, index, etag, out var tvr) == false)
                return null;

            return CreateTimeSeriesItem(context, ref tvr);
        }

        public IEnumerable<TimeSeriesSegmentEntry> GetTimeSeriesFrom(DocumentsOperationContext context, long etag, long take)
        {
            var table = new Table(TimeSeriesSchema, context.Transaction.InnerTransaction);

            foreach (var result in table.SeekForwardFrom(TimeSeriesSchema.FixedSizeIndexes[AllTimeSeriesEtagSlice], etag, 0))
            {
                if (take-- <= 0)
                    yield break;

                yield return CreateTimeSeriesItem(context, ref result.Reader);
            }
        }

        public IEnumerable<TimeSeriesSegmentEntry> GetTimeSeriesFrom(DocumentsOperationContext context, string collection, long etag, long take)
        {
            var collectionName = _documentsStorage.GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                yield break;

            var table = GetOrCreateTimeSeriesTable(context.Transaction.InnerTransaction, collectionName);

            if (table == null)
                yield break;

            foreach (var result in table.SeekForwardFrom(TimeSeriesSchema.FixedSizeIndexes[CollectionTimeSeriesEtagsSlice], etag, skip: 0))
            {
                if (take-- <= 0)
                    yield break;

                yield return CreateTimeSeriesItem(context, ref result.Reader);
            }
        }

        private static TimeSeriesSegmentEntry CreateTimeSeriesItem(DocumentsOperationContext context, ref TableValueReader reader)
        {
            var etag = *(long*)reader.Read((int)TimeSeriesTable.Etag, out _);
            var changeVectorPtr = reader.Read((int)TimeSeriesTable.ChangeVector, out int changeVectorSize);
            var segmentPtr = reader.Read((int)TimeSeriesTable.Segment, out int segmentSize);
            var keyPtr = reader.Read((int)TimeSeriesTable.TimeSeriesKey, out int keySize);

            var key = new LazyStringValue(null, keyPtr, keySize, context);

            TimeSeriesValuesSegment.ParseTimeSeriesKey(keyPtr, keySize, context, out var docId, out var lowerName, out var baseline);

            return new TimeSeriesSegmentEntry
            {
                Key = key,
                DocId = docId,
                Name = lowerName,
                ChangeVector = Encoding.UTF8.GetString(changeVectorPtr, changeVectorSize),
                Segment = new TimeSeriesValuesSegment(segmentPtr, segmentSize),
                SegmentSize = segmentSize,
                Collection = DocumentsStorage.TableValueToId(context, (int)TimeSeriesTable.Collection, ref reader),
                Start = baseline,
                Etag = Bits.SwapBytes(etag),
            };
        }

        internal Reader.SeriesSummary GetSeriesSummary(DocumentsOperationContext context, string documentId, string name)
        {
            var reader = GetReader(context, documentId, name, DateTime.MinValue, DateTime.MaxValue);
            return reader.GetSummary();
        }

        private (string ChangeVector, long NewEtag) GenerateChangeVector(DocumentsOperationContext context)
        {
            return GenerateChangeVector(context, null);
        }

        private (string ChangeVector, long NewEtag) GenerateChangeVector(DocumentsOperationContext context, string fromReplicationChangeVector)
        {
            var newEtag = _documentsStorage.GenerateNextEtag();
            string databaseChangeVector = context.LastDatabaseChangeVector ?? DocumentsStorage.GetDatabaseChangeVector(context);
            string changeVector = ChangeVectorUtils.TryUpdateChangeVector(_documentDatabase, databaseChangeVector, newEtag).ChangeVector;

            if (fromReplicationChangeVector != null)
            {
                changeVector = ChangeVectorUtils.MergeVectors(fromReplicationChangeVector, changeVector);
            }

            context.LastDatabaseChangeVector = changeVector;
            return (changeVector, newEtag);
        }

        private static void ValidateSegment(TimeSeriesValuesSegment segment)
        {
            if (segment.NumberOfBytes > MaxSegmentSize)
                throw new ArgumentOutOfRangeException("Attempted to write a time series segment that is larger (" + segment.NumberOfBytes + ") than the maximum size allowed.");
        }

        public long GetNumberOfTimeSeriesSegments(DocumentsOperationContext context)
        {
            var fstIndex = TimeSeriesSchema.FixedSizeIndexes[AllTimeSeriesEtagSlice];
            var fst = context.Transaction.InnerTransaction.FixedTreeFor(fstIndex.Name, sizeof(long));
            return fst.NumberOfEntries;
        }

        private static void AssertNoNanValue(Reader.SingleResult toAppend)
        {
            foreach (var val in toAppend.Values.Span)
            {
                if (double.IsNaN(val))
                    throw new InvalidOperationException("Failed to append TimeSeries entry. TimeSeries entries cannot have 'double.NaN' as one of their values. " +
                                                        $"Failed on Timestamp : '{toAppend.Timestamp.GetDefaultRavenFormat()}', Values : [{string.Join(',', toAppend.Values.ToArray())}]. ");
            }
        }

        private Table GetOrCreateTimeSeriesTable(Transaction tx, CollectionName collection)
        {
            return GetOrCreateTable(tx, TimeSeriesSchema, collection, CollectionTableType.TimeSeries);
        }

        private Table GetOrCreateDeleteRangesTable(Transaction tx, CollectionName collection)
        {
            return GetOrCreateTable(tx, DeleteRangesSchema, collection, CollectionTableType.TimeSeriesDeletedRanges);
        }

        private Table GetOrCreateTable(Transaction tx, TableSchema tableSchema, CollectionName collection, CollectionTableType type)
        {
            string tableName = collection.GetTableName(type);

            if (tx.IsWriteTransaction && _tableCreated.Contains(tableName) == false)
            {
                // RavenDB-11705: It is possible that this will revert if the transaction
                // aborts, so we must record this only after the transaction has been committed
                // note that calling the Create() method multiple times is a noop
                tableSchema.Create(tx, tableName, 16);
                tx.LowLevelTransaction.OnDispose += _ =>
                {
                    if (tx.LowLevelTransaction.Committed == false)
                        return;

                    // not sure if we can _rely_ on the tx write lock here, so let's be safe and create
                    // a new instance, just in case
                    _tableCreated = new HashSet<string>(_tableCreated, StringComparer.OrdinalIgnoreCase)
                    {
                        tableName
                    };
                };
            }

            return tx.OpenTable(tableSchema, tableName);
        }

        public DynamicJsonArray GetTimeSeriesLowerNamesForDocument(DocumentsOperationContext context, string docId)
        {
            var table = new Table(TimeSeriesSchema, context.Transaction.InnerTransaction);
            var list = new DynamicJsonArray();

            // here we need to find all of the time series names for a given document.

            // for example we have:
            // doc/heartbeats/123
            // doc/heartbeats/234
            // doc/heartbeats/666
            // doc/pulse/123
            // doc/pulse/54656

            // so we seek backwards, starting from the end.
            // extracting the last name and use it as a prefix for the next iteration

            var dummyHolder = new ByteStringStorage
            {
                Flags = ByteStringType.Mutable,
                Length = 0,
                Ptr = (byte*)0,
                Size = 0
            };
            var slice = new Slice(new ByteString(&dummyHolder));

            using (DocumentIdWorker.GetSliceFromId(context, docId, out var documentKeyPrefix, SpecialChars.RecordSeparator))
            using (DocumentIdWorker.GetSliceFromId(context, docId, out var last, SpecialChars.RecordSeparator + 1))
            {
                if (table.SeekOneBackwardByPrimaryKeyPrefix(documentKeyPrefix, last, out var reader) == false)
                    return list;

                var size = documentKeyPrefix.Size;
                bool excludeValueFromSeek;
                do
                {
                    var lowerName = GetLowerCasedNameAndUpdateSlice(ref reader, size, ref slice);
                    if (lowerName == null)
                    {
                        excludeValueFromSeek = true;
                        continue;
                    }

                    excludeValueFromSeek = false;
                    list.Add(lowerName);
                } while (table.SeekOneBackwardByPrimaryKeyPrefix(documentKeyPrefix, slice, out reader, excludeValueFromSeek));

                list.Items.Reverse();
                return list;
            }
        }

        private static string GetLowerCasedNameAndUpdateSlice(ref TableValueReader reader, int prefixSize, ref Slice slice)
        {
            var segmentPtr = reader.Read((int)TimeSeriesTable.Segment, out var size);
            var segment = new TimeSeriesValuesSegment(segmentPtr, size);
            var keyPtr = reader.Read((int)TimeSeriesTable.TimeSeriesKey, out size);

            for (int i = 0; i < segment.NumberOfValues; i++)
            {
                if (segment.SegmentValues.Span[i].Count > 0)
                {
                    var name = Encoding.UTF8.GetString(keyPtr + prefixSize, size - prefixSize - sizeof(long) - 1);
                    slice.Content._pointer->Ptr = keyPtr;
                    slice.Content._pointer->Length = size - sizeof(long);
                    slice.Content._pointer->Size = size - sizeof(long);
                    return name;
                }
            }

            // this segment is empty or marked as deleted, look for the next segment in this time-series
            slice.Content._pointer->Ptr = keyPtr;
            slice.Content._pointer->Length = size;
            slice.Content._pointer->Size = size;
            return null;
        }

        public long GetLastTimeSeriesEtag(DocumentsOperationContext context)
        {
            var table = new Table(TimeSeriesSchema, context.Transaction.InnerTransaction);

            var result = table.ReadLast(TimeSeriesSchema.FixedSizeIndexes[AllTimeSeriesEtagSlice]);
            if (result == null)
                return 0;

            return DocumentsStorage.TableValueToEtag((int)TimeSeriesTable.Etag, ref result.Reader);
        }

        public long GetLastTimeSeriesEtag(DocumentsOperationContext context, string collection)
        {
            var collectionName = _documentsStorage.GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                return 0;

            var table = GetOrCreateTimeSeriesTable(context.Transaction.InnerTransaction, collectionName);

            // ReSharper disable once UseNullPropagation
            if (table == null)
                return 0;

            var result = table.ReadLast(TimeSeriesSchema.FixedSizeIndexes[CollectionTimeSeriesEtagsSlice]);
            if (result == null)
                return 0;

            return DocumentsStorage.TableValueToEtag((int)TimeSeriesTable.Etag, ref result.Reader);
        }

        private static void MarkSegmentAsPendingDeletion(DocumentsOperationContext context, string collection, long etag)
        {
            var pendingDeletion = context.Transaction.InnerTransaction.CreateTree(PendingDeletionSegments);
            using (context.Allocator.From(Bits.SwapBytes(etag), out var etagSlice))
            {
                pendingDeletion.MultiAdd(collection, new Slice(etagSlice));
            }
        }

        [Conditional("DEBUG")]
        private static void EnsureStatsAndDataIntegrity(DocumentsOperationContext context, string docId, string name)
        {
            if (context.Transaction.InnerTransaction.IsWriteTransaction == false)
                return;

            var tss = context.DocumentDatabase.DocumentsStorage.TimeSeriesStorage;
            var stats = tss.Stats.GetStats(context, docId, name);
            var reader = tss.GetReader(context, docId, name, DateTime.MinValue, DateTime.MaxValue);

            if (stats.Count == 0)
            {
                Debug.Assert(stats.Start == default);
                Debug.Assert(stats.End == default);
                Debug.Assert(reader.AllValues().Any() == false);
                return;
            }

            var first = reader.First().Timestamp;
            var last = reader.Last().Timestamp;

            Debug.Assert(first == stats.Start, $"Failed start check: {first} == {stats.Start}");
            Debug.Assert(last == stats.End, $"Failed end check: {last} == {stats.End}");
        }

        private enum TimeSeriesTable
        {
            // Format of this is:
            // lower document id, record separator, lower time series name, record separator, segment start
            TimeSeriesKey = 0,

            Etag = 1,
            ChangeVector = 2,
            Segment = 3,
            Collection = 4,
            TransactionMarker = 5
        }

        private enum DeletedRangeTable
        {
            // lower document id, record separator, lower time series name, record separator, local etag
            RangeKey = 0,

            Etag = 1,
            ChangeVector = 2,
            Collection = 3,
            TransactionMarker = 4,
            From = 5,
            To = 6,
        }
    }
}
