using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Routing;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Documents.Data
{
    internal interface ISmugglerDestination
    {
        ValueTask<IAsyncDisposable> InitializeAsync(DatabaseSmugglerOptionsServerSide options, SmugglerResult result, long buildVersion);

        IDatabaseRecordActions DatabaseRecord();

        IDocumentActions Documents(bool throwOnCollectionMismatchError = true);

        IDocumentActions RevisionDocuments();

        IDocumentActions Tombstones();

        IDocumentActions Conflicts();

        IIndexActions Indexes();

        IKeyValueActions<long> Identities();

        ICompareExchangeActions CompareExchange(string databaseName, JsonOperationContext context, BackupKind? backupKind, bool withDocuments);

        ICompareExchangeActions CompareExchangeTombstones(string databaseName, JsonOperationContext context);

        ICounterActions Counters(SmugglerResult result);

        ISubscriptionActions Subscriptions();

        IReplicationHubCertificateActions ReplicationHubCertificates();

        ITimeSeriesActions TimeSeries();

        ILegacyActions LegacyDocumentDeletions();

        ILegacyActions LegacyAttachmentDeletions();
    }

    internal interface IDocumentActions : INewDocumentActions
    {
        ValueTask WriteDocumentAsync(DocumentItem item, SmugglerProgressBase.CountsWithLastEtagAndAttachments progress, Func<ValueTask> beforeFlushing = null);

        ValueTask WriteTombstoneAsync(Tombstone tombstone, SmugglerProgressBase.CountsWithLastEtag progress);

        ValueTask WriteConflictAsync(DocumentConflict conflict, SmugglerProgressBase.CountsWithLastEtag progress);

        ValueTask DeleteDocumentAsync(string id);
        IEnumerable<DocumentItem> GetDocumentsWithDuplicateCollection();
    }

    internal interface INewCompareExchangeActions
    {
        JsonOperationContext GetContextForNewCompareExchangeValue();
    }

    internal interface INewItemActions
    {
        JsonOperationContext GetContextForNewDocument();
    }
    
    internal interface INewDocumentActions : INewItemActions, IAsyncDisposable
    {
        Stream GetTempStream();
    }

    internal interface IIndexActions : IAsyncDisposable
    {
        ValueTask WriteIndexAsync(IndexDefinitionBaseServerSide indexDefinition, IndexType indexType);

        ValueTask WriteIndexAsync(IndexDefinition indexDefinition);
    }

    internal interface ICounterActions : INewDocumentActions
    {
        ValueTask WriteCounterAsync(CounterGroupDetail counterDetail);

        ValueTask WriteLegacyCounterAsync(CounterDetail counterDetail);

        void RegisterForDisposal(IDisposable data);
    }

    internal interface ISubscriptionActions : IAsyncDisposable
    {
        ValueTask WriteSubscriptionAsync(SubscriptionState subscriptionState);
    }

    internal interface IReplicationHubCertificateActions : IAsyncDisposable
    {
        ValueTask WriteReplicationHubCertificateAsync(string hub, ReplicationHubAccess access);
    }

    internal interface IKeyValueActions<in T> : IAsyncDisposable
    {
        ValueTask WriteKeyValueAsync(string key, T value);
    }

    internal interface ICompareExchangeActions : INewCompareExchangeActions, IAsyncDisposable
    {
        ValueTask WriteKeyValueAsync(string key, BlittableJsonReaderObject value, Document existingDocument);

        ValueTask WriteTombstoneKeyAsync(string key);

        ValueTask FlushAsync();
    }

    internal interface IDatabaseRecordActions : IAsyncDisposable
    {
        ValueTask WriteDatabaseRecordAsync(DatabaseRecord databaseRecord, SmugglerResult result, AuthorizationStatus authorizationStatus, DatabaseRecordItemType databaseRecordItemType);
    }

    internal interface ITimeSeriesActions : IAsyncDisposable, INewItemActions
    {
        ValueTask WriteTimeSeriesAsync(TimeSeriesItem ts);
        
        void RegisterForDisposal(IDisposable data);

        void RegisterForReturnToTheContext(AllocatedMemoryData data);
    }

    internal interface ILegacyActions : IAsyncDisposable
    {
        ValueTask WriteLegacyDeletions(string id);
    }


}
