﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries.Dynamic;
using Raven.Server.Documents.Queries.Facets;
using Raven.Server.Documents.Queries.Suggestions;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Index = Raven.Server.Documents.Indexes.Index;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace Raven.Server.Documents.Queries
{
    internal sealed class QueryRunner : AbstractDatabaseQueryRunner
    {
        private const int NumberOfRetries = 3;

        private readonly StaticIndexQueryRunner _static;
        private readonly AbstractDatabaseQueryRunner _dynamic;
        private readonly CollectionQueryRunner _collection;

        public QueryRunner(DocumentDatabase database) : base(database)
        {
            _static = new StaticIndexQueryRunner(database);
            _dynamic = database.Configuration.Indexing.DisableQueryOptimizerGeneratedIndexes
                ? new InvalidQueryRunner(database)
                : new DynamicQueryRunner(database);
            _collection = new CollectionQueryRunner(database);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public AbstractDatabaseQueryRunner GetRunner(IndexQueryServerSide query)
        {
            if (query.Metadata.IsDynamic)
            {
                if (query.Metadata.IsCollectionQuery == false)
                    return _dynamic;

                return _collection;
            }

            return _static;
        }

        public override async Task<DocumentQueryResult> ExecuteQuery(IndexQueryServerSide query, QueryOperationContext queryContext, long? existingResultEtag, OperationCancelToken token)
        {
            Exception lastException = null;
            for (var i = 0; i < NumberOfRetries; i++)
            {
                try
                {
                    Stopwatch sw = null;
                    QueryTimingsScope scope;
                    DocumentQueryResult result;
                    using (scope = query.Timings?.Start())
                    {
                        if (scope == null)
                            sw = Stopwatch.StartNew();

                        result = await GetRunner(query).ExecuteQuery(query, queryContext, existingResultEtag, token);
                    }

                    result.DurationInMs = sw != null ? (long)sw.Elapsed.TotalMilliseconds : (long)scope.Duration.TotalMilliseconds;

                    return result;
                }
                catch (ObjectDisposedException e)
                {
                    if (Database.DatabaseShutdown.IsCancellationRequested)
                        throw;

                    lastException = e;

                    await WaitForIndexBeingLikelyReplacedDuringQuery();
                }
                catch (OperationCanceledException e)
                {
                    if (Database.DatabaseShutdown.IsCancellationRequested)
                        throw;

                    if (token.Token.IsCancellationRequested)
                        throw;

                    lastException = e;

                    await WaitForIndexBeingLikelyReplacedDuringQuery();
                }
            }

            throw CreateRetriesFailedException(lastException);
        }

        public override async Task ExecuteStreamQuery(IndexQueryServerSide query, QueryOperationContext queryContext, HttpResponse response, IStreamQueryResultWriter<Document> writer, OperationCancelToken token)
        {
            Exception lastException = null;
            for (var i = 0; i < NumberOfRetries; i++)
            {
                try
                {
                    await GetRunner(query).ExecuteStreamQuery(query, queryContext, response, writer, token);
                    return;
                }
                catch (ObjectDisposedException e)
                {
                    if (Database.DatabaseShutdown.IsCancellationRequested)
                        throw;

                    lastException = e;

                    await WaitForIndexBeingLikelyReplacedDuringQuery();
                }
                catch (OperationCanceledException e)
                {
                    if (Database.DatabaseShutdown.IsCancellationRequested)
                        throw;

                    if (token.Token.IsCancellationRequested)
                        throw;

                    lastException = e;

                    await WaitForIndexBeingLikelyReplacedDuringQuery();
                }
            }

            throw CreateRetriesFailedException(lastException);
        }

        public override async Task ExecuteStreamIndexEntriesQuery(IndexQueryServerSide query, QueryOperationContext queryContext, HttpResponse response,
            IStreamQueryResultWriter<BlittableJsonReaderObject> writer, bool ignoreLimit, OperationCancelToken token)
        {
            Exception lastException = null;
            for (var i = 0; i < NumberOfRetries; i++)
            {
                try
                {
                    queryContext.CloseTransaction();
                    await GetRunner(query).ExecuteStreamIndexEntriesQuery(query, queryContext, response, writer, ignoreLimit, token);
                    return;
                }
                catch (ObjectDisposedException e)
                {
                    if (Database.DatabaseShutdown.IsCancellationRequested)
                        throw;

                    lastException = e;

                    await WaitForIndexBeingLikelyReplacedDuringQuery();
                }
                catch (OperationCanceledException e)
                {
                    if (Database.DatabaseShutdown.IsCancellationRequested)
                        throw;

                    if (token.Token.IsCancellationRequested)
                        throw;

                    lastException = e;

                    await WaitForIndexBeingLikelyReplacedDuringQuery();
                }
            }

            throw CreateRetriesFailedException(lastException);
        }

        public async Task<FacetedQueryResult> ExecuteFacetedQuery(IndexQueryServerSide query, long? existingResultEtag, QueryOperationContext queryContext, OperationCancelToken token)
        {
            if (query.Metadata.IsDynamic)
                throw new InvalidQueryException("Facet query must be executed against static index.", query.Metadata.QueryText, query.QueryParameters);
            if (query.Metadata.FilterScript != null)
                throw new InvalidQueryException("Facet query does not support a filter clause.", query.Metadata.QueryText, query.QueryParameters);

            Exception lastException = null;
            for (var i = 0; i < NumberOfRetries; i++)
            {
                try
                {
                    Stopwatch sw = null;
                    QueryTimingsScope scope;
                    FacetedQueryResult result;
                    using (scope = query.Timings?.Start())
                    {
                        if (scope == null)
                            sw = Stopwatch.StartNew();

                        result = await _static.ExecuteFacetedQuery(query, existingResultEtag, queryContext, token);
                    }

                    result.DurationInMs = sw != null ? (long)sw.Elapsed.TotalMilliseconds : (long)scope.Duration.TotalMilliseconds;

                    return result;
                }
                catch (ObjectDisposedException e)
                {
                    if (Database.DatabaseShutdown.IsCancellationRequested)
                        throw;

                    lastException = e;

                    await WaitForIndexBeingLikelyReplacedDuringQuery();
                }
                catch (OperationCanceledException e)
                {
                    if (Database.DatabaseShutdown.IsCancellationRequested)
                        throw;

                    if (token.Token.IsCancellationRequested)
                        throw;

                    lastException = e;

                    await WaitForIndexBeingLikelyReplacedDuringQuery();
                }
            }

            throw CreateRetriesFailedException(lastException);
        }

        public TermsQueryResultServerSide ExecuteGetTermsQuery(string indexName, string field, string fromValue, long? existingResultEtag, int pageSize, QueryOperationContext queryContext, OperationCancelToken token, out Index index)
        {
            Exception lastException = null;
            for (var i = 0; i < NumberOfRetries; i++)
            {
                try
                {
                    index = GetIndex(indexName);

                    queryContext.WithIndex(index);

                    var etag = index.GetIndexEtag(queryContext, null);
                    if (etag == existingResultEtag)
                        return TermsQueryResultServerSide.NotModifiedResult;

                    return index.GetTerms(field, fromValue, pageSize, queryContext, token);
                }
                catch (ObjectDisposedException e)
                {
                    if (Database.DatabaseShutdown.IsCancellationRequested)
                        throw;

                    lastException = e;

                    WaitForIndexBeingLikelyReplacedDuringQuery().GetAwaiter().GetResult();
                }
                catch (OperationCanceledException e)
                {
                    if (Database.DatabaseShutdown.IsCancellationRequested)
                        throw;

                    if (token.Token.IsCancellationRequested)
                        throw;

                    lastException = e;

                    WaitForIndexBeingLikelyReplacedDuringQuery().GetAwaiter().GetResult();
                }
            }

            throw CreateRetriesFailedException(lastException);
        }

        public override async Task<SuggestionQueryResult> ExecuteSuggestionQuery(IndexQueryServerSide query, QueryOperationContext queryContext, long? existingResultEtag, OperationCancelToken token)
        {
            Exception lastException = null;
            for (var i = 0; i < NumberOfRetries; i++)
            {
                try
                {
                    Stopwatch sw = null;
                    QueryTimingsScope scope;
                    SuggestionQueryResult result;
                    using (scope = query.Timings?.Start())
                    {
                        if (scope == null)
                            sw = Stopwatch.StartNew();

                        result = await GetRunner(query).ExecuteSuggestionQuery(query, queryContext, existingResultEtag, token);
                    }

                    result.DurationInMs = sw != null ? (long)sw.Elapsed.TotalMilliseconds : (long)scope.Duration.TotalMilliseconds;

                    return result;
                }
                catch (ObjectDisposedException e)
                {
                    if (Database.DatabaseShutdown.IsCancellationRequested)
                        throw;

                    lastException = e;

                    await WaitForIndexBeingLikelyReplacedDuringQuery();
                }
                catch (OperationCanceledException e)
                {
                    if (Database.DatabaseShutdown.IsCancellationRequested)
                        throw;

                    if (token.Token.IsCancellationRequested)
                        throw;

                    lastException = e;

                    await WaitForIndexBeingLikelyReplacedDuringQuery();
                }
            }

            throw CreateRetriesFailedException(lastException);
        }

        public override async Task<IndexEntriesQueryResult> ExecuteIndexEntriesQuery(IndexQueryServerSide query, QueryOperationContext queryContext, bool ignoreLimit, long? existingResultEtag, OperationCancelToken token)
        {
            Exception lastException = null;
            for (var i = 0; i < NumberOfRetries; i++)
            {
                try
                {
                    return await GetRunner(query).ExecuteIndexEntriesQuery(query, queryContext, ignoreLimit, existingResultEtag, token);
                }
                catch (ObjectDisposedException e)
                {
                    if (Database.DatabaseShutdown.IsCancellationRequested)
                        throw;

                    lastException = e;

                    await WaitForIndexBeingLikelyReplacedDuringQuery();
                }
                catch (OperationCanceledException e)
                {
                    if (Database.DatabaseShutdown.IsCancellationRequested)
                        throw;

                    if (token.Token.IsCancellationRequested)
                        throw;

                    lastException = e;

                    await WaitForIndexBeingLikelyReplacedDuringQuery();
                }
            }

            throw CreateRetriesFailedException(lastException);
        }

        public override async Task<IOperationResult> ExecuteDeleteQuery(IndexQueryServerSide query, QueryOperationOptions options, QueryOperationContext queryContext, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            Exception lastException = null;
            for (var i = 0; i < NumberOfRetries; i++)
            {
                try
                {
                    return await GetRunner(query).ExecuteDeleteQuery(query, options, queryContext, onProgress, token);
                }
                catch (ObjectDisposedException e)
                {
                    if (Database.DatabaseShutdown.IsCancellationRequested)
                        throw;

                    lastException = e;

                    await WaitForIndexBeingLikelyReplacedDuringQuery();
                }
                catch (OperationCanceledException e)
                {
                    if (Database.DatabaseShutdown.IsCancellationRequested)
                        throw;

                    if (token.Token.IsCancellationRequested)
                        throw;

                    lastException = e;

                    await WaitForIndexBeingLikelyReplacedDuringQuery();
                }
            }

            throw CreateRetriesFailedException(lastException);
        }

        public override async Task<IOperationResult> ExecutePatchQuery(IndexQueryServerSide query, QueryOperationOptions options, PatchRequest patch, BlittableJsonReaderObject patchArgs, QueryOperationContext queryContext, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            Exception lastException = null;
            for (var i = 0; i < NumberOfRetries; i++)
            {
                try
                {
                    return await GetRunner(query).ExecutePatchQuery(query, options, patch, patchArgs, queryContext, onProgress, token);
                }
                catch (ObjectDisposedException e)
                {
                    if (Database.DatabaseShutdown.IsCancellationRequested)
                        throw;

                    lastException = e;

                    await WaitForIndexBeingLikelyReplacedDuringQuery();
                }
                catch (OperationCanceledException e)
                {
                    if (Database.DatabaseShutdown.IsCancellationRequested)
                        throw;

                    if (token.Token.IsCancellationRequested)
                        throw;

                    lastException = e;

                    await WaitForIndexBeingLikelyReplacedDuringQuery();
                }
            }

            throw CreateRetriesFailedException(lastException);
        }

        private Task WaitForIndexBeingLikelyReplacedDuringQuery()
        {
            // sometimes we might encounter OperationCanceledException or ObjectDisposedException thrown during the query because the index is being replaced
            // in that case we want to repeat the query but let's give it a moment to properly finish the replacement operation (e.g. removal of old index files)

            return Task.Delay(500);
        }

        public List<DynamicQueryToIndexMatcher.Explanation> ExplainDynamicIndexSelection(IndexQueryServerSide query, out string indexName)
        {
            if (query.Metadata.IsDynamic == false)
                throw new InvalidOperationException("Explain can only work on dynamic indexes");

            if (_dynamic is DynamicQueryRunner d)
                return d.ExplainIndexSelection(query, out indexName);

            throw new NotSupportedException(InvalidQueryRunner.ErrorMessage);
        }

        private static Exception CreateRetriesFailedException(Exception inner)
        {
            return new InvalidOperationException($"Could not execute query. Tried {NumberOfRetries} times.", inner);
        }
    }
}
