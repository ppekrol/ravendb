using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Web.System;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.BTrees;

namespace Raven.Server.Smuggler.Documents
{
    public class StreamDestination : ISmugglerDestination
    {
        private readonly Stream _stream;
        private GZipStream _gzipStream;
        private readonly DocumentsOperationContext _context;
        private readonly DatabaseSource _source;
        private AsyncBlittableJsonTextWriter _writer;
        private DatabaseSmugglerOptionsServerSide _options;
        private Func<LazyStringValue, bool> _filterMetadataProperty;

        public StreamDestination(Stream stream, DocumentsOperationContext context, DatabaseSource source)
        {
            _stream = stream;
            _context = context;
            _source = source;
        }

        public IDisposable Initialize(DatabaseSmugglerOptionsServerSide options, SmugglerResult result, long buildVersion)
        {
            _gzipStream = new GZipStream(_stream, CompressionMode.Compress, leaveOpen: true);
            _writer = new AsyncBlittableJsonTextWriter(_context, _gzipStream);
            _options = options;

            SetupMetadataFilterMethod(_context);

            _writer.WriteStartObjectAsync();

            _writer.WritePropertyNameAsync("BuildVersion");
            _writer.WriteIntegerAsync(buildVersion);

            return new DisposableAction(() =>
            {
                _writer.WriteEndObjectAsync();
                _writer.Dispose();
                _gzipStream.Dispose();
            });
        }

        private void SetupMetadataFilterMethod(JsonOperationContext context)
        {
            var skipCountersMetadata = _options.OperateOnTypes.HasFlag(DatabaseItemType.CounterGroups) == false;
            var skipAttachmentsMetadata = _options.OperateOnTypes.HasFlag(DatabaseItemType.Attachments) == false;
            var skipTimeSeriesMetadata = _options.OperateOnTypes.HasFlag(DatabaseItemType.TimeSeries) == false;

            var flags = 0;
            if (skipCountersMetadata)
                flags += 1;
            if (skipAttachmentsMetadata)
                flags += 2;
            if (skipTimeSeriesMetadata)
                flags += 4;

            if (flags == 0)
                return;

            var counters = context.GetLazyString(Constants.Documents.Metadata.Counters);
            var attachments = context.GetLazyString(Constants.Documents.Metadata.Attachments);
            var timeSeries = context.GetLazyString(Constants.Documents.Metadata.TimeSeries);

            switch (flags)
            {
                case 1: // counters
                    _filterMetadataProperty = metadataProperty => metadataProperty.Equals(counters);
                    break;
                case 2: // attachments
                    _filterMetadataProperty = metadataProperty => metadataProperty.Equals(attachments);
                    break;
                case 3: // counters, attachments
                    _filterMetadataProperty = metadataProperty => metadataProperty.Equals(counters) || metadataProperty.Equals(attachments);
                    break;
                case 4: // timeseries
                    _filterMetadataProperty = metadataProperty => metadataProperty.Equals(timeSeries);
                    break;
                case 5: // counters, timeseries
                    _filterMetadataProperty = metadataProperty => metadataProperty.Equals(counters) || metadataProperty.Equals(timeSeries);
                    break;
                case 6: // attachments, timeseries
                    _filterMetadataProperty = metadataProperty => metadataProperty.Equals(attachments) || metadataProperty.Equals(timeSeries);
                    break;
                case 7: // counters, attachments, timeseries
                    _filterMetadataProperty = metadataProperty => metadataProperty.Equals(counters) || metadataProperty.Equals(attachments) || metadataProperty.Equals(timeSeries);
                    break;
                default:
                    throw new NotSupportedException($"Not supported value: {flags}");
            }
        }

        public IDatabaseRecordActions DatabaseRecord()
        {
            return new DatabaseRecordActions(_writer, _context);
        }

        public IDocumentActions Documents()
        {
            return new StreamDocumentActions(_writer, _context, _source, _options, _filterMetadataProperty, "Docs");
        }

        public IDocumentActions RevisionDocuments()
        {
            return new StreamDocumentActions(_writer, _context, _source, _options, _filterMetadataProperty, nameof(DatabaseItemType.RevisionDocuments));
        }

        public IDocumentActions Tombstones()
        {
            return new StreamDocumentActions(_writer, _context, _source, _options, _filterMetadataProperty, nameof(DatabaseItemType.Tombstones));
        }

        public IDocumentActions Conflicts()
        {
            return new StreamDocumentActions(_writer, _context, _source, _options, _filterMetadataProperty, nameof(DatabaseItemType.Conflicts));
        }

        public IKeyValueActions<long> Identities()
        {
            return new StreamKeyValueActions<long>(_writer, nameof(DatabaseItemType.Identities));
        }

        public ICompareExchangeActions CompareExchange(JsonOperationContext context)
        {
            return new StreamCompareExchangeActions(_writer, nameof(DatabaseItemType.CompareExchange));
        }

        public ICompareExchangeActions CompareExchangeTombstones(JsonOperationContext context)
        {
            return new StreamCompareExchangeActions(_writer, nameof(DatabaseItemType.CompareExchangeTombstones));
        }

        public ICounterActions Counters(SmugglerResult result)
        {
            return new StreamCounterActions(_writer, _context, nameof(DatabaseItemType.CounterGroups));
        }

        public ISubscriptionActions Subscriptions()
        {
            return new StreamSubscriptionActions(_writer, _context, nameof(DatabaseItemType.Subscriptions));
        }

        public ITimeSeriesActions TimeSeries()
        {
            return new StreamTimeSeriesActions(_writer, _context, nameof(DatabaseItemType.TimeSeries));
        }

        public IIndexActions Indexes()
        {
            return new StreamIndexActions(_writer, _context);
        }

        private class DatabaseRecordActions : IDatabaseRecordActions
        {
            private readonly AsyncBlittableJsonTextWriter _writer;
            private readonly JsonOperationContext _context;

            public DatabaseRecordActions(AsyncBlittableJsonTextWriter writer, JsonOperationContext context)
            {
                _writer = writer;
                _context = context;

                _writer.WriteCommaAsync();
                _writer.WritePropertyNameAsync(nameof(DatabaseItemType.DatabaseRecord));
                _writer.WriteStartObjectAsync();
            }

            public void WriteDatabaseRecord(DatabaseRecord databaseRecord, SmugglerProgressBase.DatabaseRecordProgress progress, AuthorizationStatus authorizationStatus, DatabaseRecordItemType databaseRecordItemType)
            {
                _writer.WritePropertyNameAsync(nameof(databaseRecord.DatabaseName));
                _writer.WriteStringAsync(databaseRecord.DatabaseName);
                _writer.WriteCommaAsync();

                _writer.WritePropertyNameAsync(nameof(databaseRecord.Encrypted));
                _writer.WriteBool(databaseRecord.Encrypted);

                if (databaseRecordItemType.Contain(DatabaseRecordItemType.ConflictSolverConfig))
                {
                    _writer.WriteCommaAsync();
                    _writer.WritePropertyNameAsync(nameof(databaseRecord.ConflictSolverConfig));
                    WriteConflictSolver(databaseRecord.ConflictSolverConfig);
                }

                if (databaseRecordItemType.Contain(DatabaseRecordItemType.Settings))
                {
                    _writer.WriteCommaAsync();
                    _writer.WritePropertyNameAsync(nameof(databaseRecord.Settings));
                    WriteSettings(databaseRecord.Settings);
                }

                if (databaseRecordItemType.Contain(DatabaseRecordItemType.Revisions))
                {
                    _writer.WriteCommaAsync();
                    _writer.WritePropertyNameAsync(nameof(databaseRecord.Revisions));
                    WriteRevisions(databaseRecord.Revisions);
                }

                if (databaseRecordItemType.Contain(DatabaseRecordItemType.TimeSeries))
                {
                    _writer.WriteCommaAsync();
                    _writer.WritePropertyNameAsync(nameof(databaseRecord.TimeSeries));
                    WriteTimeSeries(databaseRecord.TimeSeries);
                }

                if (databaseRecordItemType.Contain(DatabaseRecordItemType.DocumentsCompression))
                {
                    _writer.WriteCommaAsync();
                    _writer.WritePropertyNameAsync(nameof(databaseRecord.DocumentsCompression));
                    WriteDocumentsCompression(databaseRecord.DocumentsCompression);
                }

                if (databaseRecordItemType.Contain(DatabaseRecordItemType.Expiration))
                {
                    _writer.WriteCommaAsync();
                    _writer.WritePropertyNameAsync(nameof(databaseRecord.Expiration));
                    WriteExpiration(databaseRecord.Expiration);
                }

                if (databaseRecordItemType.Contain(DatabaseRecordItemType.Client))
                {
                    _writer.WriteCommaAsync();
                    _writer.WritePropertyNameAsync(nameof(databaseRecord.Client));
                    WriteClientConfiguration(databaseRecord.Client);
                }

                if (databaseRecordItemType.Contain(DatabaseRecordItemType.Sorters))
                {
                    _writer.WriteCommaAsync();
                    _writer.WritePropertyNameAsync(nameof(databaseRecord.Sorters));
                    WriteSorters(databaseRecord.Sorters);
                }

                switch (authorizationStatus)
                {
                    case AuthorizationStatus.DatabaseAdmin:
                    case AuthorizationStatus.Operator:
                    case AuthorizationStatus.ClusterAdmin:
                        if (databaseRecordItemType.Contain(DatabaseRecordItemType.RavenConnectionStrings))
                        {
                            _writer.WriteCommaAsync();
                            _writer.WritePropertyNameAsync(nameof(databaseRecord.RavenConnectionStrings));
                            WriteRavenConnectionStrings(databaseRecord.RavenConnectionStrings);
                        }

                        if (databaseRecordItemType.Contain(DatabaseRecordItemType.SqlConnectionStrings))
                        {
                            _writer.WriteCommaAsync();
                            _writer.WritePropertyNameAsync(nameof(databaseRecord.SqlConnectionStrings));
                            WriteSqlConnectionStrings(databaseRecord.SqlConnectionStrings);
                        }

                        if (databaseRecordItemType.Contain(DatabaseRecordItemType.PeriodicBackups))
                        {
                            _writer.WriteCommaAsync();
                            _writer.WritePropertyNameAsync(nameof(databaseRecord.PeriodicBackups));
                            WritePeriodicBackups(databaseRecord.PeriodicBackups);
                        }

                        if (databaseRecordItemType.Contain(DatabaseRecordItemType.ExternalReplications))
                        {
                            _writer.WriteCommaAsync();
                            _writer.WritePropertyNameAsync(nameof(databaseRecord.ExternalReplications));
                            WriteExternalReplications(databaseRecord.ExternalReplications);
                        }

                        if (databaseRecordItemType.Contain(DatabaseRecordItemType.RavenEtls))
                        {
                            _writer.WriteCommaAsync();
                            _writer.WritePropertyNameAsync(nameof(databaseRecord.RavenEtls));
                            WriteRavenEtls(databaseRecord.RavenEtls);
                        }

                        if (databaseRecordItemType.Contain(DatabaseRecordItemType.SqlEtls))
                        {
                            _writer.WriteCommaAsync();
                            _writer.WritePropertyNameAsync(nameof(databaseRecord.SqlEtls));
                            WriteSqlEtls(databaseRecord.SqlEtls);
                        }

                        if (databaseRecordItemType.Contain(DatabaseRecordItemType.HubPullReplications))
                        {
                            _writer.WriteCommaAsync();
                            _writer.WritePropertyNameAsync(nameof(databaseRecord.HubPullReplications));
                            WriteHubPullReplications(databaseRecord.HubPullReplications);
                        }

                        if (databaseRecordItemType.Contain(DatabaseRecordItemType.SinkPullReplications))
                        {
                            _writer.WriteCommaAsync();
                            _writer.WritePropertyNameAsync(nameof(databaseRecord.SinkPullReplications));
                            WriteSinkPullReplications(databaseRecord.SinkPullReplications);
                        }

                        break;
                }
            }

            private void WriteHubPullReplications(List<PullReplicationDefinition> hubPullReplications)
            {
                if (hubPullReplications == null)
                {
                    _writer.WriteNullAsync();
                    return;
                }
                _writer.WriteStartArrayAsync();
                var first = true;
                foreach (var pullReplication in hubPullReplications)
                {
                    if (first == false)
                        _writer.WriteCommaAsync();
                    first = false;

                    _context.WriteAsync(_writer, pullReplication.ToJson());
                }
                _writer.WriteEndArrayAsync();
            }

            private void WriteSinkPullReplications(List<PullReplicationAsSink> sinkPullReplications)
            {
                if (sinkPullReplications == null)
                {
                    _writer.WriteNullAsync();
                    return;
                }
                _writer.WriteStartArrayAsync();
                var first = true;
                foreach (var pullReplication in sinkPullReplications)
                {
                    if (first == false)
                        _writer.WriteCommaAsync();
                    first = false;

                    _context.WriteAsync(_writer, pullReplication.ToJson());

                }
                _writer.WriteEndArrayAsync();
            }

            private void WriteSorters(Dictionary<string, SorterDefinition> sorters)
            {
                if (sorters == null)
                {
                    _writer.WriteNullAsync();
                    return;
                }

                _writer.WriteStartObjectAsync();
                var first = true;
                foreach (var sorter in sorters)
                {
                    if (first == false)
                        _writer.WriteCommaAsync();
                    first = false;

                    _writer.WritePropertyNameAsync(sorter.Key);
                    _context.WriteAsync(_writer, sorter.Value.ToJson());
                }

                _writer.WriteEndObjectAsync();
            }

            private static readonly HashSet<string> DoNotBackUp = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                RavenConfiguration.GetKey(x => x.Core.DataDirectory),
                RavenConfiguration.GetKey(x => x.Storage.TempPath),
                RavenConfiguration.GetKey(x => x.Indexing.TempPath),
                RavenConfiguration.GetKey(x => x.Licensing.License),
                RavenConfiguration.GetKey(x => x.Core.RunInMemory)
            };

            private static readonly HashSet<string> ServerWideKeys = DatabaseHelper.GetServerWideOnlyConfigurationKeys().ToHashSet(StringComparer.OrdinalIgnoreCase);

            private void WriteSettings(Dictionary<string, string> settings)
            {
                if (settings == null)
                {
                    _writer.WriteNullAsync();
                    return;
                }

                _writer.WriteStartArrayAsync();
                var first = true;
                foreach (var config in settings)
                {
                    if (!(DoNotBackUp.Contains(config.Key) ||
                          ServerWideKeys.Contains(config.Key)))
                    {
                        if (first == false)
                            _writer.WriteCommaAsync();
                        first = false;
                        _writer.WriteStartObjectAsync();
                        _writer.WritePropertyNameAsync(config.Key);
                        _writer.WriteStringAsync(config.Value);
                        _writer.WriteEndObjectAsync();
                    }
                }
                _writer.WriteEndArrayAsync();
            }

            private void WriteSqlEtls(List<SqlEtlConfiguration> sqlEtlConfiguration)
            {
                if (sqlEtlConfiguration == null)
                {
                    _writer.WriteNullAsync();
                    return;
                }
                _writer.WriteStartArrayAsync();

                var first = true;
                foreach (var etl in sqlEtlConfiguration)
                {
                    if (first == false)
                        _writer.WriteCommaAsync();
                    first = false;
                    _context.WriteAsync(_writer, etl.ToJson());
                }

                _writer.WriteEndArrayAsync();
            }

            private void WriteRavenEtls(List<RavenEtlConfiguration> ravenEtlConfiguration)
            {
                if (ravenEtlConfiguration == null)
                {
                    _writer.WriteNullAsync();
                    return;
                }
                _writer.WriteStartArrayAsync();

                var first = true;
                foreach (var etl in ravenEtlConfiguration)
                {
                    if (first == false)
                        _writer.WriteCommaAsync();
                    first = false;
                    _context.WriteAsync(_writer, etl.ToJson());
                }

                _writer.WriteEndArrayAsync();
            }

            private void WriteExternalReplications(List<ExternalReplication> externalReplication)
            {
                if (externalReplication == null)
                {
                    _writer.WriteNullAsync();
                    return;
                }
                _writer.WriteStartArrayAsync();

                var first = true;
                foreach (var replication in externalReplication)
                {
                    if (first == false)
                        _writer.WriteCommaAsync();
                    first = false;

                    _context.WriteAsync(_writer, replication.ToJson());
                }

                _writer.WriteEndArrayAsync();
            }

            private void WritePeriodicBackups(List<PeriodicBackupConfiguration> periodicBackup)
            {
                if (periodicBackup == null)
                {
                    _writer.WriteNullAsync();
                    return;
                }
                _writer.WriteStartArrayAsync();

                var first = true;

                foreach (var backup in periodicBackup)
                {
                    if (first == false)
                        _writer.WriteCommaAsync();
                    first = false;
                    _context.WriteAsync(_writer, backup.ToJson());
                }
                _writer.WriteEndArrayAsync();
            }

            private void WriteConflictSolver(ConflictSolver conflictSolver)
            {
                if (conflictSolver == null)
                {
                    _writer.WriteNullAsync();
                    return;
                }
                _context.WriteAsync(_writer, conflictSolver.ToJson());
            }

            private void WriteClientConfiguration(ClientConfiguration clientConfiguration)
            {
                if (clientConfiguration == null)
                {
                    _writer.WriteNullAsync();
                    return;
                }
                _context.WriteAsync(_writer, clientConfiguration.ToJson());
            }

            private void WriteExpiration(ExpirationConfiguration expiration)
            {
                if (expiration == null)
                {
                    _writer.WriteNullAsync();
                    return;
                }

                _context.WriteAsync(_writer, expiration.ToJson());
            }

            private void WriteRevisions(RevisionsConfiguration revisions)
            {
                if (revisions == null)
                {
                    _writer.WriteNullAsync();
                    return;
                }
                _context.WriteAsync(_writer, revisions.ToJson());
            }

            private void WriteTimeSeries(TimeSeriesConfiguration timeSeries)
            {
                if (timeSeries == null)
                {
                    _writer.WriteNullAsync();
                    return;
                }
                _context.WriteAsync(_writer, timeSeries.ToJson());
            }

            private void WriteDocumentsCompression(DocumentsCompressionConfiguration compressionConfiguration)
            {
                if (compressionConfiguration == null)
                {
                    _writer.WriteNullAsync();
                    return;
                }
                _context.WriteAsync(_writer, compressionConfiguration.ToJson());
            }

            private void WriteRavenConnectionStrings(Dictionary<string, RavenConnectionString> connections)
            {
                _writer.WriteStartObjectAsync();

                var first = true;
                foreach (var ravenConnectionString in connections)
                {
                    if (first == false)
                        _writer.WriteCommaAsync();
                    first = false;

                    _writer.WritePropertyNameAsync(ravenConnectionString.Key);

                    _context.WriteAsync(_writer, ravenConnectionString.Value.ToJson());
                }

                _writer.WriteEndObjectAsync();
            }

            private void WriteSqlConnectionStrings(Dictionary<string, SqlConnectionString> connections)
            {
                _writer.WriteStartObjectAsync();

                var first = true;
                foreach (var sqlConnectionString in connections)
                {
                    if (first == false)
                        _writer.WriteCommaAsync();
                    first = false;

                    _writer.WritePropertyNameAsync(sqlConnectionString.Key);

                    _context.WriteAsync(_writer, sqlConnectionString.Value.ToJson());
                }

                _writer.WriteEndObjectAsync();
            }

            public void Dispose()
            {
                _writer.WriteEndObjectAsync();
            }
        }

        private class StreamIndexActions : StreamActionsBase, IIndexActions
        {
            private readonly JsonOperationContext _context;

            public StreamIndexActions(AsyncBlittableJsonTextWriter writer, JsonOperationContext context)
                : base(writer, "Indexes")
            {
                _context = context;
            }

            public void WriteIndex(IndexDefinitionBase indexDefinition, IndexType indexType)
            {
                if (First == false)
                    Writer.WriteCommaAsync();
                First = false;

                Writer.WriteStartObjectAsync();

                Writer.WritePropertyNameAsync(nameof(IndexDefinition.Type));
                Writer.WriteStringAsync(indexType.ToString());
                Writer.WriteCommaAsync();

                Writer.WritePropertyNameAsync(nameof(IndexDefinition));
                indexDefinition.Persist(_context, Writer);

                Writer.WriteEndObjectAsync();
            }

            public void WriteIndex(IndexDefinition indexDefinition)
            {
                if (First == false)
                    Writer.WriteCommaAsync();
                First = false;

                Writer.WriteStartObjectAsync();

                Writer.WritePropertyNameAsync(nameof(IndexDefinition.Type));
                Writer.WriteStringAsync(indexDefinition.Type.ToString());
                Writer.WriteCommaAsync();

                Writer.WritePropertyNameAsync(nameof(IndexDefinition));
                Writer.WriteIndexDefinition(_context, indexDefinition);

                Writer.WriteEndObjectAsync();
            }
        }

        private class StreamCounterActions : StreamActionsBase, ICounterActions
        {
            private readonly DocumentsOperationContext _context;

            public void WriteCounter(CounterGroupDetail counterDetail)
            {
                CountersStorage.ConvertFromBlobToNumbers(_context, counterDetail);

                using (counterDetail)
                {
                    if (First == false)
                        Writer.WriteCommaAsync();
                    First = false;

                    Writer.WriteStartObjectAsync();

                    Writer.WritePropertyNameAsync(nameof(CounterItem.DocId));
                    Writer.WriteStringAsync(counterDetail.DocumentId, skipEscaping: true);
                    Writer.WriteCommaAsync();

                    Writer.WritePropertyNameAsync(nameof(CounterItem.ChangeVector));
                    Writer.WriteStringAsync(counterDetail.ChangeVector, skipEscaping: true);
                    Writer.WriteCommaAsync();

                    Writer.WritePropertyNameAsync(nameof(CounterItem.Batch.Values));
                    Writer.WriteObjectAsync(counterDetail.Values);

                    Writer.WriteEndObjectAsync();
                }
            }

            public void WriteLegacyCounter(CounterDetail counterDetail)
            {
                // Used only in Database Destination 
                throw new NotSupportedException("WriteLegacyCounter is not supported when writing to a Stream destination, " +
                                                "it is only supported when writing to Database destination. Shouldn't happen.");
            }

            public void RegisterForDisposal(IDisposable data)
            {
                throw new NotSupportedException("RegisterForDisposal is never used in StreamCounterActions. Shouldn't happen.");
            }

            public StreamCounterActions(AsyncBlittableJsonTextWriter writer, DocumentsOperationContext context, string propertyName) : base(writer, propertyName)
            {
                _context = context;
            }

            public DocumentsOperationContext GetContextForNewDocument()
            {
                _context.CachedProperties.NewDocument();
                return _context;
            }

            public Stream GetTempStream()
            {
                throw new NotSupportedException("GetTempStream is never used in StreamCounterActions. Shouldn't happen");
            }
        }

        private class StreamTimeSeriesActions : StreamActionsBase, ITimeSeriesActions
        {
            public StreamTimeSeriesActions(AsyncBlittableJsonTextWriter writer, DocumentsOperationContext context, string propertyName) : base(writer, propertyName)
            {
            }

            public unsafe void WriteTimeSeries(TimeSeriesItem item)
            {
                if (First == false)
                    Writer.WriteCommaAsync();
                First = false;

                Writer.WriteStartObjectAsync();
                {
                    Writer.WritePropertyNameAsync(Constants.Documents.Blob.Document);

                    Writer.WriteStartObjectAsync();
                    {
                        Writer.WritePropertyNameAsync(nameof(TimeSeriesItem.DocId));
                        Writer.WriteStringAsync(item.DocId);
                        Writer.WriteCommaAsync();

                        Writer.WritePropertyNameAsync(nameof(TimeSeriesItem.Name));
                        Writer.WriteStringAsync(item.Name);
                        Writer.WriteCommaAsync();

                        Writer.WritePropertyNameAsync(nameof(TimeSeriesItem.ChangeVector));
                        Writer.WriteStringAsync(item.ChangeVector);
                        Writer.WriteCommaAsync();

                        Writer.WritePropertyNameAsync(nameof(TimeSeriesItem.Collection));
                        Writer.WriteStringAsync(item.Collection);
                        Writer.WriteCommaAsync();

                        Writer.WritePropertyNameAsync(nameof(TimeSeriesItem.Baseline));
                        Writer.WriteDateTimeAsync(item.Baseline, true);    
                    }
                    Writer.WriteEndObjectAsync();
                    
                    Writer.WriteCommaAsync();
                    Writer.WritePropertyNameAsync(Constants.Documents.Blob.Size);
                    Writer.WriteIntegerAsync(item.SegmentSize);
                }
                Writer.WriteEndObjectAsync();

                Writer.WriteMemoryChunkAsync(item.Segment.Ptr, item.Segment.NumberOfBytes);
            }
        }

        private class StreamSubscriptionActions : StreamActionsBase, ISubscriptionActions
        {
            private readonly DocumentsOperationContext _context;
            private readonly AsyncBlittableJsonTextWriter _writer;

            public StreamSubscriptionActions(AsyncBlittableJsonTextWriter writer, DocumentsOperationContext context, string propertyName) : base(writer, propertyName)
            {
                _context = context;
                _writer = writer;
            }

            public void WriteSubscription(SubscriptionState subscriptionState)
            {
                if (First == false)
                    Writer.WriteCommaAsync();
                First = false;

                _context.WriteAsync(_writer, subscriptionState.ToJson());
            }
        }

        private class StreamDocumentActions : StreamActionsBase, IDocumentActions
        {
            private readonly DocumentsOperationContext _context;
            private readonly DatabaseSource _source;
            private readonly DatabaseSmugglerOptionsServerSide _options;
            private readonly Func<LazyStringValue, bool> _filterMetadataProperty;
            private HashSet<string> _attachmentStreamsAlreadyExported;

            public StreamDocumentActions(AsyncBlittableJsonTextWriter writer, DocumentsOperationContext context, DatabaseSource source, DatabaseSmugglerOptionsServerSide options, Func<LazyStringValue, bool> filterMetadataProperty, string propertyName)
                : base(writer, propertyName)
            {
                _context = context;
                _source = source;
                _options = options;
                _filterMetadataProperty = filterMetadataProperty;
            }

            public void WriteDocument(DocumentItem item, SmugglerProgressBase.CountsWithLastEtagAndAttachments progress)
            {
                if (item.Attachments != null)
                    throw new NotSupportedException();

                var document = item.Document;
                using (document)
                {
                    if (_options.OperateOnTypes.HasFlag(DatabaseItemType.Attachments))
                        WriteUniqueAttachmentStreams(document, progress);

                    if (First == false)
                        Writer.WriteCommaAsync();
                    First = false;

                    Writer.WriteDocument(_context, document, metadataOnly: false, _filterMetadataProperty);
                }
            }

            public void WriteTombstone(Tombstone tombstone, SmugglerProgressBase.CountsWithLastEtag progress)
            {
                if (First == false)
                    Writer.WriteCommaAsync();
                First = false;

                using (tombstone)
                {
                    _context.WriteAsync(Writer, new DynamicJsonValue
                    {
                        ["Key"] = tombstone.LowerId,
                        [nameof(Tombstone.Type)] = tombstone.Type.ToString(),
                        [nameof(Tombstone.Collection)] = tombstone.Collection,
                        [nameof(Tombstone.Flags)] = tombstone.Flags.ToString(),
                        [nameof(Tombstone.ChangeVector)] = tombstone.ChangeVector,
                        [nameof(Tombstone.DeletedEtag)] = tombstone.DeletedEtag,
                        [nameof(Tombstone.Etag)] = tombstone.Etag,
                        [nameof(Tombstone.LastModified)] = tombstone.LastModified,
                    });
                }
            }

            public void WriteConflict(DocumentConflict conflict, SmugglerProgressBase.CountsWithLastEtag progress)
            {
                if (First == false)
                    Writer.WriteCommaAsync();
                First = false;

                using (conflict)
                {
                    _context.WriteAsync(Writer, new DynamicJsonValue
                    {
                        [nameof(DocumentConflict.Id)] = conflict.Id,
                        [nameof(DocumentConflict.Collection)] = conflict.Collection,
                        [nameof(DocumentConflict.Flags)] = conflict.Flags.ToString(),
                        [nameof(DocumentConflict.ChangeVector)] = conflict.ChangeVector,
                        [nameof(DocumentConflict.Etag)] = conflict.Etag,
                        [nameof(DocumentConflict.LastModified)] = conflict.LastModified,
                        [nameof(DocumentConflict.Doc)] = conflict.Doc,
                    });
                }
            }

            public void DeleteDocument(string id)
            {
                // no-op
            }

            public Stream GetTempStream()
            {
                throw new NotSupportedException();
            }

            private void WriteUniqueAttachmentStreams(Document document, SmugglerProgressBase.CountsWithLastEtagAndAttachments progress)
            {
                if ((document.Flags & DocumentFlags.HasAttachments) != DocumentFlags.HasAttachments ||
                    document.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false ||
                    metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
                    return;

                if (_attachmentStreamsAlreadyExported == null)
                    _attachmentStreamsAlreadyExported = new HashSet<string>();

                foreach (BlittableJsonReaderObject attachment in attachments)
                {
                    if (attachment.TryGet(nameof(AttachmentName.Hash), out LazyStringValue hash) == false)
                    {
                        progress.Attachments.ErroredCount++;

                        throw new ArgumentException($"Hash field is mandatory in attachment's metadata: {attachment}");
                    }

                    progress.Attachments.ReadCount++;

                    if (_attachmentStreamsAlreadyExported.Add(hash))
                    {
                        using (var stream = _source.GetAttachmentStream(hash, out string tag))
                        {
                            if (stream == null)
                            {
                                progress.Attachments.ErroredCount++;
                                throw new ArgumentException($"Document {document.Id} seems to have a attachment hash: {hash}, but no correlating hash was found in the storage.");
                            }
                            WriteAttachmentStream(hash, stream, tag);
                        }
                    }
                }
            }

            public DocumentsOperationContext GetContextForNewDocument()
            {
                _context.CachedProperties.NewDocument();
                return _context;
            }

            private void WriteAttachmentStream(LazyStringValue hash, Stream stream, string tag)
            {
                if (First == false)
                    Writer.WriteCommaAsync();
                First = false;

                Writer.WriteStartObjectAsync();

                Writer.WritePropertyNameAsync(Constants.Documents.Metadata.Key);
                Writer.WriteStartObjectAsync();

                Writer.WritePropertyNameAsync(DocumentItem.ExportDocumentType.Key);
                Writer.WriteStringAsync(DocumentItem.ExportDocumentType.Attachment);

                Writer.WriteEndObjectAsync();
                Writer.WriteCommaAsync();

                Writer.WritePropertyNameAsync(nameof(AttachmentName.Hash));
                Writer.WriteStringAsync(hash);
                Writer.WriteCommaAsync();

                Writer.WritePropertyNameAsync(nameof(AttachmentName.Size));
                Writer.WriteIntegerAsync(stream.Length);
                Writer.WriteCommaAsync();

                Writer.WritePropertyNameAsync(nameof(DocumentItem.AttachmentStream.Tag));
                Writer.WriteStringAsync(tag);

                Writer.WriteEndObjectAsync();

                Writer.WriteStreamAsync(stream);
            }

        }

        private class StreamKeyValueActions<T> : StreamActionsBase, IKeyValueActions<T>
        {
            public StreamKeyValueActions(AsyncBlittableJsonTextWriter writer, string name)
                : base(writer, name)
            {
            }

            public void WriteKeyValue(string key, T value)
            {
                if (First == false)
                    Writer.WriteCommaAsync();
                First = false;

                Writer.WriteStartObjectAsync();
                Writer.WritePropertyNameAsync("Key");
                Writer.WriteStringAsync(key);
                Writer.WriteCommaAsync();
                Writer.WritePropertyNameAsync("Value");
                Writer.WriteStringAsync(value.ToString());
                Writer.WriteEndObjectAsync();
            }
        }

        private class StreamCompareExchangeActions : StreamActionsBase, ICompareExchangeActions
        {
            public StreamCompareExchangeActions(AsyncBlittableJsonTextWriter writer, string name)
                : base(writer, name)
            {
            }

            public void WriteKeyValue(string key, BlittableJsonReaderObject value)
            {
                using (value)
                {
                    if (First == false)
                        Writer.WriteCommaAsync();
                    First = false;

                    Writer.WriteStartObjectAsync();
                    Writer.WritePropertyNameAsync("Key");
                    Writer.WriteStringAsync(key);
                    Writer.WriteCommaAsync();
                    Writer.WritePropertyNameAsync("Value");
                    Writer.WriteStringAsync(value.ToString());
                    Writer.WriteEndObjectAsync();
                }
            }

            public void WriteTombstoneKey(string key)
            {
                if (First == false)
                    Writer.WriteCommaAsync();
                First = false;

                Writer.WriteStartObjectAsync();
                Writer.WritePropertyNameAsync("Key");
                Writer.WriteStringAsync(key);
                Writer.WriteEndObjectAsync();
            }
        }

        private abstract class StreamActionsBase : IDisposable
        {
            protected readonly AsyncBlittableJsonTextWriter Writer;

            protected bool First { get; set; }

            protected StreamActionsBase(AsyncBlittableJsonTextWriter writer, string propertyName)
            {
                Writer = writer;
                First = true;

                Writer.WriteCommaAsync();
                Writer.WritePropertyNameAsync(propertyName);
                Writer.WriteStartArrayAsync();
            }

            public void Dispose()
            {
                Writer.WriteEndArrayAsync();
            }
        }
    }
}
