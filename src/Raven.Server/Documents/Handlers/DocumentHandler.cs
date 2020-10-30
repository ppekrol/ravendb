﻿// -----------------------------------------------------------------------
//  <copyright file="DocumentHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session.Loaders;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Patch;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.TrafficWatch;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Sparrow.Utils;
using Voron;
using Constants = Raven.Client.Constants;
using DeleteDocumentCommand = Raven.Server.Documents.TransactionCommands.DeleteDocumentCommand;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace Raven.Server.Documents.Handlers
{
    public class DocumentHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/docs", "HEAD", AuthorizationStatus.ValidUser)]
        public Task Head()
        {
            var id = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
            var changeVector = GetStringFromHeaders("If-None-Match");

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var document = Database.DocumentsStorage.Get(context, id, DocumentFields.ChangeVector);
                if (document == null)
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                else
                {
                    if (changeVector == document.ChangeVector)
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                    else
                        HttpContext.Response.Headers[Constants.Headers.Etag] = "\"" + document.ChangeVector + "\"";
                }

                return Task.CompletedTask;
            }
        }

        [RavenAction("/databases/*/docs/size", "GET", AuthorizationStatus.ValidUser)]
        public async Task GetDocSize()
        {
            var id = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var document = Database.DocumentsStorage.GetDocumentMetrics(context, id);
                if (document == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return;
                }

                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

                var documentSizeDetails = new DocumentSizeDetails
                {
                    DocId = id,
                    ActualSize = document.Value.ActualSize,
                    HumaneActualSize = Sizes.Humane(document.Value.ActualSize),
                    AllocatedSize = document.Value.AllocatedSize,
                    HumaneAllocatedSize = Sizes.Humane(document.Value.AllocatedSize)
                };

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    await context.WriteAsync(writer, documentSizeDetails.ToJson());
                    await writer.FlushAsync();
                }
            }
        }

        [RavenAction("/databases/*/docs", "GET", AuthorizationStatus.ValidUser)]
        public async Task Get()
        {
            var ids = GetStringValuesQueryString("id", required: false);
            var metadataOnly = GetBoolValueQueryString("metadataOnly", required: false) ?? false;

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                if (ids.Count > 0)
                    await GetDocumentsByIdAsync(context, ids, metadataOnly);
                else
                    await GetDocumentsAsync(context, metadataOnly);

                if (TrafficWatchManager.HasRegisteredClients)
                    AddStringToHttpContext(ids.ToString(), TrafficWatchChangeType.Documents);
            }
        }

        [RavenAction("/databases/*/docs", "POST", AuthorizationStatus.ValidUser, DisableOnCpuCreditsExhaustion = true)]
        public async Task PostGet()
        {
            var metadataOnly = GetBoolValueQueryString("metadataOnly", required: false) ?? false;

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var docs = await context.ReadForMemoryAsync(RequestBodyStream(), "docs");
                if (docs.TryGet("Ids", out BlittableJsonReaderArray array) == false)
                    ThrowRequiredPropertyNameInRequest("Ids");

                var ids = new string[array.Length];
                for (int i = 0; i < array.Length; i++)
                {
                    ids[i] = array.GetStringByIndex(i);
                }

                context.OpenReadTransaction();

                // init here so it can be passed to TW
                var idsStringValues = new Microsoft.Extensions.Primitives.StringValues(ids);

                if (TrafficWatchManager.HasRegisteredClients)
                    AddStringToHttpContext(idsStringValues.ToString(), TrafficWatchChangeType.Documents);

                await GetDocumentsByIdAsync(context, idsStringValues, metadataOnly);
            }
        }

        private async Task GetDocumentsAsync(DocumentsOperationContext context, bool metadataOnly)
        {
            var sw = Stopwatch.StartNew();

            // everything here operates on all docs
            var databaseChangeVector = DocumentsStorage.GetDatabaseChangeVector(context);

            if (GetStringFromHeaders("If-None-Match") == databaseChangeVector)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                return;
            }
            HttpContext.Response.Headers["ETag"] = "\"" + databaseChangeVector + "\"";

            var etag = GetLongQueryString("etag", false);
            var start = GetStart();
            var pageSize = GetPageSize();
            var isStartsWith = HttpContext.Request.Query.ContainsKey("startsWith");

            IEnumerable<Document> documents;
            if (etag != null)
            {
                documents = Database.DocumentsStorage.GetDocumentsFrom(context, etag.Value, start, pageSize);
            }
            else if (isStartsWith)
            {
                documents = Database.DocumentsStorage.GetDocumentsStartingWith(context,
                     HttpContext.Request.Query["startsWith"],
                     HttpContext.Request.Query["matches"],
                     HttpContext.Request.Query["exclude"],
                     HttpContext.Request.Query["startAfter"],
                     start,
                     pageSize);
            }
            else // recent docs
            {
                documents = Database.DocumentsStorage.GetDocumentsInReverseEtagOrder(context, start, pageSize);
            }

            long numberOfResults;

            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                await writer.WriteStartObjectAsync();
                await writer.WritePropertyNameAsync("Results");

                numberOfResults = await writer.WriteDocumentsAsync(context, documents, metadataOnly);

                await writer.WriteEndObjectAsync();
                await writer.OuterFlushAsync();
            }

            AddPagingPerformanceHint(PagingOperationType.Documents, isStartsWith ? nameof(DocumentsStorage.GetDocumentsStartingWith) : nameof(GetDocumentsAsync), HttpContext.Request.QueryString.Value, numberOfResults, pageSize, sw.ElapsedMilliseconds);
        }

        private async Task GetDocumentsByIdAsync(DocumentsOperationContext context, Microsoft.Extensions.Primitives.StringValues ids, bool metadataOnly)
        {
            var sw = Stopwatch.StartNew();

            var includePaths = GetStringValuesQueryString("include", required: false);
            var documents = new List<Document>(ids.Count);
            var includes = new List<Document>(includePaths.Count * ids.Count);
            var includeDocs = new IncludeDocumentsCommand(Database.DocumentsStorage, context, includePaths, isProjection: false);

            GetCountersQueryString(Database, context, out var includeCounters);

            GetTimeSeriesQueryString(Database, context, out var includeTimeSeries);

            GetCompareExchangeValueQueryString(Database, out var includeCompareExchangeValues);

            using (includeCompareExchangeValues)
            {
                foreach (var id in ids)
                {
                    Document document = null;
                    if (string.IsNullOrEmpty(id) == false)
                    {
                        document = Database.DocumentsStorage.Get(context, id);
                    }
                    if (document == null && ids.Count == 1)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        return;
                    }

                    documents.Add(document);
                    includeDocs.Gather(document);
                    includeCounters?.Fill(document);
                    includeTimeSeries?.Fill(document);
                    includeCompareExchangeValues?.Gather(document);
                }

                includeDocs.Fill(includes);
                includeCompareExchangeValues?.Materialize();

                var actualEtag = ComputeHttpEtags.ComputeEtagForDocuments(documents, includes, includeCounters, includeTimeSeries, includeCompareExchangeValues);

                var etag = GetStringFromHeaders("If-None-Match");
                if (etag == actualEtag)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                    return;
                }

                HttpContext.Response.Headers[Constants.Headers.Etag] = "\"" + actualEtag + "\"";

                var numberOfResults = await WriteDocumentsJsonAsync(context, metadataOnly, documents, includes, includeCounters?.Results, includeTimeSeries?.Results,
                    includeCompareExchangeValues?.Results);

                AddPagingPerformanceHint(PagingOperationType.Documents, nameof(GetDocumentsByIdAsync), HttpContext.Request.QueryString.Value, numberOfResults,
                    documents.Count, sw.ElapsedMilliseconds);
            }
        }

        private void GetCompareExchangeValueQueryString(DocumentDatabase database, out IncludeCompareExchangeValuesCommand includeCompareExchangeValues)
        {
            includeCompareExchangeValues = null;

            var compareExchangeValues = GetStringValuesQueryString("cmpxchg", required: false);
            if (compareExchangeValues.Count == 0)
                return;

            includeCompareExchangeValues = IncludeCompareExchangeValuesCommand.InternalScope(database, compareExchangeValues);
        }

        private void GetCountersQueryString(DocumentDatabase database, DocumentsOperationContext context, out IncludeCountersCommand includeCounters)
        {
            includeCounters = null;

            var counters = GetStringValuesQueryString("counter", required: false);
            if (counters.Count == 0)
                return;

            if (counters.Count == 1 &&
                counters[0] == Constants.Counters.All)
            {
                counters = new string[0];
            }

            includeCounters = new IncludeCountersCommand(database, context, counters);
        }

        private void GetTimeSeriesQueryString(DocumentDatabase database, DocumentsOperationContext context, out IncludeTimeSeriesCommand includeTimeSeries)
        {
            includeTimeSeries = null;

            var timeSeriesNames = GetStringValuesQueryString("timeseries", required: false);
            if (timeSeriesNames.Count == 0)
                return;

            var fromList = GetStringValuesQueryString("from", required: false);
            var toList = GetStringValuesQueryString("to", required: false);

            if (timeSeriesNames.Count != fromList.Count || fromList.Count != toList.Count)
                throw new InvalidOperationException("Parameters 'timeseriesNames', 'fromList' and 'toList' must be of equal length. " +
                                                    $"Got : timeseriesNames.Count = {timeSeriesNames.Count}, fromList.Count = {fromList.Count}, toList.Count = {toList.Count}.");

            var hs = new HashSet<TimeSeriesRange>(TimeSeriesRangeComparer.Instance);

            for (int i = 0; i < timeSeriesNames.Count; i++)
            {
                hs.Add(new TimeSeriesRange
                {
                    Name = timeSeriesNames[i],
                    From = string.IsNullOrEmpty(fromList[i])
                        ? DateTime.MinValue
                        : TimeSeriesHandler.ParseDate(fromList[i], "from"),
                    To = string.IsNullOrEmpty(toList[i])
                        ? DateTime.MaxValue
                        : TimeSeriesHandler.ParseDate(toList[i], "to")
                });
            }

            includeTimeSeries = new IncludeTimeSeriesCommand(context,
                new Dictionary<string, HashSet<TimeSeriesRange>> { { string.Empty, hs } });
        }

        private async Task<long> WriteDocumentsJsonAsync(JsonOperationContext context, bool metadataOnly, IEnumerable<Document> documentsToWrite, List<Document> includes,
            Dictionary<string, List<CounterDetail>> counters, Dictionary<string, Dictionary<string, List<TimeSeriesRangeResult>>> timeseries, Dictionary<string, CompareExchangeValue<BlittableJsonReaderObject>> compareExchangeValues)
        {
            long numberOfResults;
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                await writer.WriteStartObjectAsync();
                await writer.WritePropertyNameAsync(nameof(GetDocumentsResult.Results));
                numberOfResults = await writer.WriteDocumentsAsync(context, documentsToWrite, metadataOnly);

                await writer.WriteCommaAsync();
                await writer.WritePropertyNameAsync(nameof(GetDocumentsResult.Includes));
                if (includes.Count > 0)
                {
                    await writer.WriteIncludesAsync(context, includes);
                }
                else
                {
                    await writer.WriteStartObjectAsync();
                    await writer.WriteEndObjectAsync();
                }

                if (counters?.Count > 0)
                {
                    await writer.WriteCommaAsync();
                    await writer.WritePropertyNameAsync(nameof(GetDocumentsResult.CounterIncludes));
                    await writer.WriteCountersAsync(counters);
                }

                if (timeseries?.Count > 0)
                {
                    await writer.WriteCommaAsync();
                    await writer.WritePropertyNameAsync(nameof(GetDocumentsResult.TimeSeriesIncludes));
                    await writer.WriteTimeSeriesAsync(timeseries);
                }

                if (compareExchangeValues?.Count > 0)
                {
                    await writer.WriteCommaAsync();
                    await writer.WritePropertyNameAsync(nameof(GetDocumentsResult.CompareExchangeValueIncludes));
                    await writer.WriteCompareExchangeValues(compareExchangeValues);
                }

                await writer.WriteEndObjectAsync();
                await writer.OuterFlushAsync();
            }
            return numberOfResults;
        }

        [RavenAction("/databases/*/docs", "DELETE", AuthorizationStatus.ValidUser, DisableOnCpuCreditsExhaustion = true)]
        public async Task Delete()
        {
            var id = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var changeVector = context.GetLazyString(GetStringFromHeaders("If-Match"));

                var cmd = new DeleteDocumentCommand(id, changeVector, Database, catchConcurrencyErrors: true);
                await Database.TxMerger.Enqueue(cmd);
                cmd.ExceptionDispatchInfo?.Throw();
            }

            NoContentStatus();
        }

        [RavenAction("/databases/*/docs", "PUT", AuthorizationStatus.ValidUser, DisableOnCpuCreditsExhaustion = true)]
        public async Task Put()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var id = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
                // We HAVE to read the document in full, trying to parallelize the doc read
                // and the identity generation needs to take into account that the identity
                // generation can fail and will leave the reading task hanging if we abort
                // easier to just do in synchronously
                var doc = await context.ReadForDiskAsync(RequestBodyStream(), id).ConfigureAwait(false);

                if (id[id.Length - 1] == '|')
                {
                    var (_, clusterId, _) = await ServerStore.GenerateClusterIdentityAsync(id, Database.IdentityPartsSeparator, Database.Name, GetRaftRequestIdFromQuery());
                    id = clusterId;
                }

                var changeVector = context.GetLazyString(GetStringFromHeaders("If-Match"));

                using (var cmd = new MergedPutCommand(doc, id, changeVector, Database, shouldValidateAttachments: true))
                {
                    await Database.TxMerger.Enqueue(cmd);

                    cmd.ExceptionDispatchInfo?.Throw();

                    HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        writer.WriteStartObjectAsync();

                        writer.WritePropertyNameAsync(nameof(PutResult.Id));
                        writer.WriteStringAsync(cmd.PutResult.Id);
                        writer.WriteCommaAsync();

                        writer.WritePropertyNameAsync(nameof(PutResult.ChangeVector));
                        writer.WriteStringAsync(cmd.PutResult.ChangeVector);

                        writer.WriteEndObjectAsync();
                    }
                }
            }
        }

        [RavenAction("/databases/*/docs", "PATCH", AuthorizationStatus.ValidUser, DisableOnCpuCreditsExhaustion = true)]
        public async Task Patch()
        {
            var id = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");

            var isTest = GetBoolValueQueryString("test", required: false) ?? false;
            var debugMode = GetBoolValueQueryString("debug", required: false) ?? isTest;
            var skipPatchIfChangeVectorMismatch = GetBoolValueQueryString("skipPatchIfChangeVectorMismatch", required: false) ?? false;

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var request = context.Read(RequestBodyStream(), "ScriptedPatchRequest");
                if (request.TryGet("Patch", out BlittableJsonReaderObject patchCmd) == false || patchCmd == null)
                    throw new ArgumentException("The 'Patch' field in the body request is mandatory");

                var patch = PatchRequest.Parse(patchCmd, out var patchArgs);

                PatchRequest patchIfMissing = null;
                BlittableJsonReaderObject patchIfMissingArgs = null;
                if (request.TryGet("PatchIfMissing", out BlittableJsonReaderObject patchIfMissingCmd) && patchIfMissingCmd != null)
                    patchIfMissing = PatchRequest.Parse(patchIfMissingCmd, out patchIfMissingArgs);

                var changeVector = context.GetLazyString(GetStringFromHeaders("If-Match"));

                var command = new PatchDocumentCommand(context,
                    id,
                    changeVector,
                    skipPatchIfChangeVectorMismatch,
                    (patch, patchArgs),
                    (patchIfMissing, patchIfMissingArgs),
                    Database,
                    isTest,
                    debugMode,
                    true,
                    returnDocument: false
                );

                if (isTest == false)
                {
                    await Database.TxMerger.Enqueue(command);
                }
                else
                {
                    // PutDocument requires the write access to the docs storage
                    // testing patching is rare enough not to optimize it
                    using (context.OpenWriteTransaction())
                    {
                        command.Execute(context, null);
                    }
                }

                switch (command.PatchResult.Status)
                {
                    case PatchStatus.DocumentDoesNotExist:
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        return;

                    case PatchStatus.Created:
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
                        break;

                    case PatchStatus.Skipped:
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                        return;

                    case PatchStatus.Patched:
                    case PatchStatus.NotModified:
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
                        break;

                    default:
                        throw new ArgumentOutOfRangeException();
                }

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObjectAsync();

                    writer.WritePropertyNameAsync(nameof(command.PatchResult.Status));
                    writer.WriteStringAsync(command.PatchResult.Status.ToString());
                    writer.WriteCommaAsync();

                    writer.WritePropertyNameAsync(nameof(command.PatchResult.ModifiedDocument));
                    writer.WriteObjectAsync(command.PatchResult.ModifiedDocument);

                    if (debugMode)
                    {
                        writer.WriteCommaAsync();
                        writer.WritePropertyNameAsync(nameof(command.PatchResult.OriginalDocument));
                        if (isTest)
                            writer.WriteObjectAsync(command.PatchResult.OriginalDocument);
                        else
                            writer.WriteNullAsync();

                        writer.WriteCommaAsync();

                        writer.WritePropertyNameAsync(nameof(command.PatchResult.Debug));

                        context.WriteAsync(writer, new DynamicJsonValue
                        {
                            ["Info"] = new DynamicJsonArray(command.DebugOutput),
                            ["Actions"] = command.DebugActions
                        });
                    }

                    switch (command.PatchResult.Status)
                    {
                        case PatchStatus.Created:
                        case PatchStatus.Patched:

                            writer.WriteCommaAsync();

                            writer.WritePropertyNameAsync(nameof(command.PatchResult.LastModified));
                            writer.WriteStringAsync(command.PatchResult.LastModified.GetDefaultRavenFormat());
                            writer.WriteCommaAsync();

                            writer.WritePropertyNameAsync(nameof(command.PatchResult.ChangeVector));
                            writer.WriteStringAsync(command.PatchResult.ChangeVector);
                            writer.WriteCommaAsync();

                            writer.WritePropertyNameAsync(nameof(command.PatchResult.Collection));
                            writer.WriteStringAsync(command.PatchResult.Collection);
                            break;
                    }

                    writer.WriteEndObjectAsync();
                }
            }
        }

        [RavenAction("/databases/*/docs/class", "GET", AuthorizationStatus.ValidUser, DisableOnCpuCreditsExhaustion = true)]
        public Task GenerateClassFromDocument()
        {
            var id = GetStringQueryString("id");
            var lang = (GetStringQueryString("lang", required: false) ?? "csharp")
                .Trim().ToLowerInvariant();

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var document = Database.DocumentsStorage.Get(context, id);
                if (document == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return Task.CompletedTask;
                }

                switch (lang)
                {
                    case "csharp":
                        break;

                    default:
                        throw new NotImplementedException($"Document code generator isn't implemented for {lang}");
                }

                using (var writer = new StreamWriter(ResponseBodyStream()))
                {
                    var codeGenerator = new JsonClassGenerator(lang);
                    var code = codeGenerator.Execute(document);
                    writer.Write(code);
                }

                return Task.CompletedTask;
            }
        }
    }

    public class DocumentSizeDetails : IDynamicJson
    {
        public string DocId { get; set; }
        public int ActualSize { get; set; }
        public string HumaneActualSize { get; set; }
        public int AllocatedSize { get; set; }
        public string HumaneAllocatedSize { get; set; }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(DocId)] = DocId,
                [nameof(ActualSize)] = ActualSize,
                [nameof(HumaneActualSize)] = HumaneActualSize,
                [nameof(AllocatedSize)] = AllocatedSize,
                [nameof(HumaneAllocatedSize)] = HumaneAllocatedSize
            };
        }
    }

    public class MergedPutCommand : TransactionOperationsMerger.MergedTransactionCommand, IDisposable
    {
        private string _id;
        private readonly LazyStringValue _expectedChangeVector;
        private readonly BlittableJsonReaderObject _document;
        private readonly DocumentDatabase _database;
        private readonly bool _shouldValidateAttachments;
        public ExceptionDispatchInfo ExceptionDispatchInfo;
        public DocumentsStorage.PutOperationResults PutResult;

        public static string GenerateNonConflictingId(DocumentDatabase database, string prefix)
        {
            return prefix + database.DocumentsStorage.GenerateNextEtag().ToString("D19") + "-" + Guid.NewGuid().ToBase64Unpadded();
        }

        public MergedPutCommand(BlittableJsonReaderObject doc, string id, LazyStringValue changeVector, DocumentDatabase database, bool shouldValidateAttachments = false)
        {
            _document = doc;
            _id = id;
            _expectedChangeVector = changeVector;
            _database = database;
            _shouldValidateAttachments = shouldValidateAttachments;
        }

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            if (_shouldValidateAttachments)
            {
                if (_document.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata)
                    && metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments))
                {
                    ValidateAttachments(attachments, context, _id);
                }
            }
            try
            {
                PutResult = _database.DocumentsStorage.Put(context, _id, _expectedChangeVector, _document);
            }
            catch (Voron.Exceptions.VoronConcurrencyErrorException)
            {
                // RavenDB-10581 - If we have a concurrency error on "doc-id/"
                // this means that we have existing values under the current etag
                // we'll generate a new (random) id for them.

                // The TransactionMerger will re-run us when we ask it to as a
                // separate transaction
                if (_id?.EndsWith(_database.IdentityPartsSeparator) == true)
                {
                    _id = GenerateNonConflictingId(_database, _id);
                    RetryOnError = true;
                }
                throw;
            }
            catch (ConcurrencyException e)
            {
                ExceptionDispatchInfo = ExceptionDispatchInfo.Capture(e);
            }
            return 1;
        }

        private void ValidateAttachments(BlittableJsonReaderArray attachments, DocumentsOperationContext context, string id)
        {
            if (attachments == null)
            {
                throw new InvalidOperationException($"Can not put document (id={id}) with '{Constants.Documents.Metadata.Attachments}': null");
            }

            foreach (BlittableJsonReaderObject attachment in attachments)
            {
                if (attachment.TryGet(nameof(AttachmentName.Hash), out string hash) == false || hash == null)
                {
                    throw new InvalidOperationException($"Can not put document (id={id}) because it contains an attachment without an hash property.");
                }
                using (Slice.From(context.Allocator, hash, out var hashSlice))
                {
                    if (AttachmentsStorage.GetCountOfAttachmentsForHash(context, hashSlice) < 1)
                    {
                        throw new InvalidOperationException($"Can not put document (id={id}) because it contains an attachment with hash={hash} but no such attachment is stored.");
                    }
                }
            }
        }

        public void Dispose()
        {
            _document?.Dispose();
        }

        public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
        {
            return new MergedPutCommandDto()
            {
                Id = _id,
                ExpectedChangeVector = _expectedChangeVector,
                Document = _document
            };
        }

        public class MergedPutCommandDto : TransactionOperationsMerger.IReplayableCommandDto<MergedPutCommand>
        {
            public string Id { get; set; }
            public LazyStringValue ExpectedChangeVector { get; set; }
            public BlittableJsonReaderObject Document { get; set; }

            public MergedPutCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
            {
                return new MergedPutCommand(Document, Id, ExpectedChangeVector, database);
            }
        }
    }
}
