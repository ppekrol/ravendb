﻿using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Changes;

internal class ShardedDatabaseChanges : AbstractDatabaseChanges<ShardedDatabaseConnectionState>, IShardedDatabaseChanges
{
    public ShardedDatabaseChanges(RequestExecutor requestExecutor, string databaseName, Action onDispose, string nodeTag, bool throttleConnection) 
        : base(requestExecutor, databaseName, onDispose, nodeTag, throttleConnection)
    {
    }

    public IChangesObservable<BlittableJsonReaderObject> ForDocument(string docId)
    {
        if (string.IsNullOrWhiteSpace(docId))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(docId));

        var counter = GetOrAddConnectionState("docs/" + docId, "watch-doc", "unwatch-doc", docId);

        var taskedObservable = new ChangesObservable<BlittableJsonReaderObject, ShardedDatabaseConnectionState>(
            counter,
            filter: null);

        return taskedObservable;
    }

    public IChangesObservable<BlittableJsonReaderObject> ForAllDocuments()
    {
        var counter = GetOrAddConnectionState("all-docs", "watch-docs", "unwatch-docs", null);

        var taskedObservable = new ChangesObservable<BlittableJsonReaderObject, ShardedDatabaseConnectionState>(
            counter,
            filter: null);

        return taskedObservable;
    }

    public IChangesObservable<BlittableJsonReaderObject> ForDocumentsStartingWith(string docIdPrefix)
    {
        throw new NotImplementedException();
    }

    public IChangesObservable<BlittableJsonReaderObject> ForDocumentsInCollection(string collectionName)
    {
        throw new NotImplementedException();
    }

    public IChangesObservable<BlittableJsonReaderObject> ForDocumentsInCollection<TEntity>()
    {
        throw new NotImplementedException();
    }

    public IChangesObservable<BlittableJsonReaderObject> ForAllCounters()
    {
        var counter = GetOrAddConnectionState("all-counters", "watch-counters", "unwatch-counters", null);

        var taskedObservable = new ChangesObservable<BlittableJsonReaderObject, ShardedDatabaseConnectionState>(
            counter,
            filter: null);

        return taskedObservable;
    }

    public IChangesObservable<BlittableJsonReaderObject> ForCounter(string counterName)
    {
        if (string.IsNullOrWhiteSpace(counterName))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(counterName));

        var counter = GetOrAddConnectionState($"counter/{counterName}", "watch-counter", "unwatch-counter", counterName);

        var taskedObservable = new ChangesObservable<BlittableJsonReaderObject, ShardedDatabaseConnectionState>(
            counter,
            filter: null);

        return taskedObservable;
    }

    public IChangesObservable<BlittableJsonReaderObject> ForCounterOfDocument(string documentId, string counterName)
    {
        if (string.IsNullOrWhiteSpace(documentId))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(documentId));
        if (string.IsNullOrWhiteSpace(counterName))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(counterName));

        var counter = GetOrAddConnectionState($"document/{documentId}/counter/{counterName}", "watch-document-counter", "unwatch-document-counter", value: null, values: new[] { documentId, counterName });

        var taskedObservable = new ChangesObservable<BlittableJsonReaderObject, ShardedDatabaseConnectionState>(
            counter,
            filter: null);

        return taskedObservable;
    }

    public IChangesObservable<BlittableJsonReaderObject> ForCountersOfDocument(string documentId)
    {
        if (string.IsNullOrWhiteSpace(documentId))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(documentId));

        var counter = GetOrAddConnectionState($"document/{documentId}/counter", "watch-document-counters", "unwatch-document-counters", documentId);

        var taskedObservable = new ChangesObservable<BlittableJsonReaderObject, ShardedDatabaseConnectionState>(
            counter,
            filter: null);

        return taskedObservable;
    }

    public IChangesObservable<BlittableJsonReaderObject> ForIndex(string indexName)
    {
        throw new NotImplementedException();
    }

    public IChangesObservable<BlittableJsonReaderObject> ForAllIndexes()
    {
        throw new NotImplementedException();
    }

    public IChangesObservable<BlittableJsonReaderObject> ForOperationId(long operationId)
    {
        throw new NotImplementedException();
    }

    public IChangesObservable<BlittableJsonReaderObject> ForAllOperations()
    {
        throw new NotImplementedException();
    }

    public IChangesObservable<BlittableJsonReaderObject> ForAllTimeSeries()
    {
        var counter = GetOrAddConnectionState("all-timeseries", "watch-all-timeseries", "unwatch-all-timeseries", null);

        var taskedObservable = new ChangesObservable<BlittableJsonReaderObject, ShardedDatabaseConnectionState>(
            counter,
            filter: null);

        return taskedObservable;
    }

    public IChangesObservable<BlittableJsonReaderObject> ForTimeSeries(string timeSeriesName)
    {
        if (string.IsNullOrWhiteSpace(timeSeriesName))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(timeSeriesName));

        var counter = GetOrAddConnectionState($"timeseries/{timeSeriesName}", "watch-timeseries", "unwatch-timeseries", timeSeriesName);

        var taskedObservable = new ChangesObservable<BlittableJsonReaderObject, ShardedDatabaseConnectionState>(
            counter,
            filter: null);

        return taskedObservable;
    }

    public IChangesObservable<BlittableJsonReaderObject> ForTimeSeriesOfDocument(string documentId, string timeSeriesName)
    {
        if (string.IsNullOrWhiteSpace(documentId))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(documentId));
        if (string.IsNullOrWhiteSpace(timeSeriesName))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(timeSeriesName));

        var counter = GetOrAddConnectionState($"document/{documentId}/timeseries/{timeSeriesName}", "watch-document-timeseries", "unwatch-document-timeseries", value: null, values: new[] { documentId, timeSeriesName });

        var taskedObservable = new ChangesObservable<BlittableJsonReaderObject, ShardedDatabaseConnectionState>(
            counter,
            filter: null);

        return taskedObservable;
    }

    public IChangesObservable<BlittableJsonReaderObject> ForTimeSeriesOfDocument(string documentId)
    {
        if (string.IsNullOrWhiteSpace(documentId))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(documentId));

        var counter = GetOrAddConnectionState($"document/{documentId}/timeseries", "watch-all-document-timeseries", "unwatch-all-document-timeseries", documentId);

        var taskedObservable = new ChangesObservable<BlittableJsonReaderObject, ShardedDatabaseConnectionState>(
            counter,
            filter: null);

        return taskedObservable;
    }

    protected override ShardedDatabaseConnectionState CreateDatabaseConnectionState(Func<Task> onConnect, Func<Task> onDisconnect) => new(onConnect, onDisconnect);

    protected override void ProcessNotification(string type, BlittableJsonReaderObject change)
    {
        foreach (var state in States.ForceEnumerateInThreadSafeManner())
        {
            state.Value.Send(change);
        }
    }
}
