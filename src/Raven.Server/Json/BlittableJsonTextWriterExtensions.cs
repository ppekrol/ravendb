using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Queries.Suggestions;
using Raven.Client.Documents.Queries.Timings;
using Raven.Client.Extensions;
using Raven.Server.Documents;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Indexes.Debugging;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Dynamic;
using Raven.Server.Documents.Queries.Facets;
using Raven.Server.Documents.Queries.Suggestions;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron.Data.BTrees;

namespace Raven.Server.Json
{
    internal static class BlittableJsonTextWriterExtensions
    {
        public static void WritePerformanceStats(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, IEnumerable<IndexPerformanceStats> stats)
        {
            writer.WriteStartObjectAsync();
            writer.WriteArray(context, "Results", stats, (w, c, stat) =>
            {
                w.WriteStartObject();

                w.WritePropertyName(nameof(stat.Name));
                w.WriteString(stat.Name);
                w.WriteComma();

                w.WriteArray(c, nameof(stat.Performance), stat.Performance, (wp, cp, performance) => { wp.WriteIndexingPerformanceStats(context, performance); });

                w.WriteEndObject();
            });
            writer.WriteEndObjectAsync();
        }

        public static void WriteEtlTaskPerformanceStats(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, IEnumerable<EtlTaskPerformanceStats> stats)
        {
            writer.WriteStartObjectAsync();
            writer.WriteArray(context, "Results", stats, (w, c, taskStats) =>
            {
                w.WriteStartObject();

                w.WritePropertyName(nameof(taskStats.TaskId));
                w.WriteInteger(taskStats.TaskId);
                w.WriteComma();

                w.WritePropertyName(nameof(taskStats.TaskName));
                w.WriteString(taskStats.TaskName);
                w.WriteComma();

                w.WritePropertyName(nameof(taskStats.EtlType));
                w.WriteString(taskStats.EtlType.ToString());
                w.WriteComma();

                w.WriteArray(c, nameof(taskStats.Stats), taskStats.Stats, (wp, cp, scriptStats) =>
                {
                    wp.WriteStartObject();

                    wp.WritePropertyName(nameof(scriptStats.TransformationName));
                    wp.WriteString(scriptStats.TransformationName);
                    wp.WriteComma();

                    wp.WriteArray(cp, nameof(scriptStats.Performance), scriptStats.Performance, (wpp, cpp, perfStats) => wpp.WriteEtlPerformanceStats(cpp, perfStats));

                    wp.WriteEndObject();
                });

                w.WriteEndObject();
            });
            writer.WriteEndObjectAsync();
        }

        public static void WriteEtlTaskProgress(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, IEnumerable<EtlTaskProgress> progress)
        {
            writer.WriteStartObjectAsync();
            writer.WriteArray(context, "Results", progress, (w, c, taskStats) =>
            {
                w.WriteStartObject();

                w.WritePropertyName(nameof(taskStats.TaskName));
                w.WriteString(taskStats.TaskName);
                w.WriteComma();

                w.WritePropertyName(nameof(taskStats.EtlType));
                w.WriteString(taskStats.EtlType.ToString());
                w.WriteComma();

                w.WriteArray(c, nameof(taskStats.ProcessesProgress), taskStats.ProcessesProgress, (wp, cp, processProgress) =>
                {
                    wp.WriteStartObject();

                    wp.WritePropertyName(nameof(processProgress.TransformationName));
                    wp.WriteString(processProgress.TransformationName);
                    wp.WriteComma();

                    wp.WritePropertyName(nameof(processProgress.Completed));
                    wp.WriteBool(processProgress.Completed);
                    wp.WriteComma();

                    wp.WritePropertyName(nameof(processProgress.Disabled));
                    wp.WriteBool(processProgress.Disabled);
                    wp.WriteComma();

                    wp.WritePropertyName(nameof(processProgress.AverageProcessedPerSecond));
                    wp.WriteDouble(processProgress.AverageProcessedPerSecond);
                    wp.WriteComma();

                    wp.WritePropertyName(nameof(processProgress.NumberOfDocumentsToProcess));
                    wp.WriteInteger(processProgress.NumberOfDocumentsToProcess);
                    wp.WriteComma();

                    wp.WritePropertyName(nameof(processProgress.TotalNumberOfDocuments));
                    wp.WriteInteger(processProgress.TotalNumberOfDocuments);
                    wp.WriteComma();

                    wp.WritePropertyName(nameof(processProgress.NumberOfDocumentTombstonesToProcess));
                    wp.WriteInteger(processProgress.NumberOfDocumentTombstonesToProcess);
                    wp.WriteComma();

                    wp.WritePropertyName(nameof(processProgress.TotalNumberOfDocumentTombstones));
                    wp.WriteInteger(processProgress.TotalNumberOfDocumentTombstones);
                    wp.WriteComma();

                    wp.WritePropertyName(nameof(processProgress.NumberOfCounterGroupsToProcess));
                    wp.WriteInteger(processProgress.NumberOfCounterGroupsToProcess);
                    wp.WriteComma();

                    wp.WritePropertyName(nameof(processProgress.TotalNumberOfCounterGroups));
                    wp.WriteInteger(processProgress.TotalNumberOfCounterGroups);

                    wp.WriteEndObject();
                });

                w.WriteEndObject();
            });
            writer.WriteEndObjectAsync();
        }

        public static void WriteExplanation(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, DynamicQueryToIndexMatcher.Explanation explanation)
        {
            writer.WriteStartObjectAsync();

            writer.WritePropertyNameAsync(nameof(explanation.Index));
            writer.WriteStringAsync(explanation.Index);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(explanation.Reason));
            writer.WriteStringAsync(explanation.Reason);

            writer.WriteEndObjectAsync();
        }

        public static void WriteSuggestionQueryResult(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, SuggestionQueryResult result, out long numberOfResults)
        {
            writer.WriteStartObjectAsync();

            writer.WritePropertyNameAsync(nameof(result.TotalResults));
            writer.WriteIntegerAsync(result.TotalResults);
            writer.WriteCommaAsync();

            if (result.CappedMaxResults != null)
            {
                writer.WritePropertyNameAsync(nameof(result.CappedMaxResults));
                writer.WriteIntegerAsync(result.CappedMaxResults.Value);
                writer.WriteCommaAsync();
            }

            writer.WritePropertyNameAsync(nameof(result.DurationInMs));
            writer.WriteIntegerAsync(result.DurationInMs);
            writer.WriteCommaAsync();

            writer.WriteQueryResult(context, result, metadataOnly: false, numberOfResults: out numberOfResults, partial: true);

            writer.WriteEndObjectAsync();
        }

        public static void WriteFacetedQueryResult(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, FacetedQueryResult result, out long numberOfResults)
        {
            writer.WriteStartObjectAsync();

            writer.WritePropertyNameAsync(nameof(result.TotalResults));
            writer.WriteIntegerAsync(result.TotalResults);
            writer.WriteCommaAsync();

            if (result.CappedMaxResults != null)
            {
                writer.WritePropertyNameAsync(nameof(result.CappedMaxResults));
                writer.WriteIntegerAsync(result.CappedMaxResults.Value);
                writer.WriteCommaAsync();
            }

            writer.WritePropertyNameAsync(nameof(result.DurationInMs));
            writer.WriteIntegerAsync(result.DurationInMs);
            writer.WriteCommaAsync();

            writer.WriteQueryResult(context, result, metadataOnly: false, numberOfResults: out numberOfResults, partial: true);

            writer.WriteEndObjectAsync();
        }

        public static void WriteSuggestionResult(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, SuggestionResult result)
        {
            writer.WriteStartObjectAsync();

            writer.WritePropertyNameAsync(nameof(result.Name));
            writer.WriteStringAsync(result.Name);
            writer.WriteCommaAsync();

            writer.WriteArray(nameof(result.Suggestions), result.Suggestions);

            writer.WriteEndObjectAsync();
        }

        public static void WriteFacetResult(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, FacetResult result)
        {
            writer.WriteStartObjectAsync();

            writer.WritePropertyNameAsync(nameof(result.Name));
            writer.WriteStringAsync(result.Name);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(result.Values));
            writer.WriteStartArrayAsync();
            var isFirstInternal = true;
            foreach (var value in result.Values)
            {
                if (isFirstInternal == false)
                    writer.WriteCommaAsync();

                isFirstInternal = false;

                writer.WriteStartObjectAsync();

                writer.WritePropertyNameAsync(nameof(value.Name));
                writer.WriteStringAsync(value.Name);
                writer.WriteCommaAsync();

                if (value.Average.HasValue)
                {
                    writer.WritePropertyNameAsync(nameof(value.Average));

                    using (var lazyStringValue = context.GetLazyString(value.Average.ToInvariantString()))
                        writer.WriteDoubleAsync(new LazyNumberValue(lazyStringValue));

                    writer.WriteCommaAsync();
                }

                if (value.Max.HasValue)
                {
                    writer.WritePropertyNameAsync(nameof(value.Max));

                    using (var lazyStringValue = context.GetLazyString(value.Max.ToInvariantString()))
                        writer.WriteDoubleAsync(new LazyNumberValue(lazyStringValue));

                    writer.WriteCommaAsync();
                }

                if (value.Min.HasValue)
                {
                    writer.WritePropertyNameAsync(nameof(value.Min));

                    using (var lazyStringValue = context.GetLazyString(value.Min.ToInvariantString()))
                        writer.WriteDoubleAsync(new LazyNumberValue(lazyStringValue));

                    writer.WriteCommaAsync();
                }

                if (value.Sum.HasValue)
                {
                    writer.WritePropertyNameAsync(nameof(value.Sum));

                    using (var lazyStringValue = context.GetLazyString(value.Sum.ToInvariantString()))
                        writer.WriteDoubleAsync(new LazyNumberValue(lazyStringValue));

                    writer.WriteCommaAsync();
                }

                writer.WritePropertyNameAsync(nameof(value.Count));
                writer.WriteIntegerAsync(value.Count);
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(value.Range));
                writer.WriteStringAsync(value.Range);

                writer.WriteEndObjectAsync();
            }
            writer.WriteEndArrayAsync();
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(result.RemainingHits));
            writer.WriteIntegerAsync(result.RemainingHits);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(result.RemainingTermsCount));
            writer.WriteIntegerAsync(result.RemainingTermsCount);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(result.RemainingTerms));
            writer.WriteStartArrayAsync();
            isFirstInternal = true;
            foreach (var term in result.RemainingTerms)
            {
                if (isFirstInternal == false)
                    writer.WriteCommaAsync();

                isFirstInternal = false;

                writer.WriteStringAsync(term);
            }
            writer.WriteEndArrayAsync();

            writer.WriteEndObjectAsync();
        }

        public static void WriteIndexEntriesQueryResult(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, IndexEntriesQueryResult result)
        {
            writer.WriteStartObjectAsync();

            writer.WritePropertyNameAsync(nameof(result.TotalResults));
            writer.WriteIntegerAsync(result.TotalResults);
            writer.WriteCommaAsync();

            if (result.CappedMaxResults != null)
            {
                writer.WritePropertyNameAsync(nameof(result.CappedMaxResults));
                writer.WriteIntegerAsync(result.CappedMaxResults.Value);
                writer.WriteCommaAsync();
            }

            writer.WritePropertyNameAsync(nameof(result.SkippedResults));
            writer.WriteIntegerAsync(result.SkippedResults);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(result.DurationInMs));
            writer.WriteIntegerAsync(result.DurationInMs);
            writer.WriteCommaAsync();

            writer.WriteQueryResult(context, result, metadataOnly: false, numberOfResults: out long _, partial: true);

            writer.WriteEndObjectAsync();
        }

        public static async Task<int> WriteDocumentQueryResultAsync(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, DocumentQueryResult result, bool metadataOnly, Action<AsyncBlittableJsonTextWriter> writeAdditionalData = null)
        {
            writer.WriteStartObjectAsync();

            writer.WritePropertyNameAsync(nameof(result.TotalResults));
            writer.WriteIntegerAsync(result.TotalResults);
            writer.WriteCommaAsync();

            if (result.CappedMaxResults != null)
            {
                writer.WritePropertyNameAsync(nameof(result.CappedMaxResults));
                writer.WriteIntegerAsync(result.CappedMaxResults.Value);
                writer.WriteCommaAsync();
            }

            writer.WritePropertyNameAsync(nameof(result.SkippedResults));
            writer.WriteIntegerAsync(result.SkippedResults);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(result.DurationInMs));
            writer.WriteIntegerAsync(result.DurationInMs);
            writer.WriteCommaAsync();

            writer.WriteArray(nameof(result.IncludedPaths), result.IncludedPaths);
            writer.WriteCommaAsync();

            var numberOfResults = await writer.WriteQueryResultAsync(context, result, metadataOnly, partial: true);

            if (result.Highlightings != null)
            {
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(result.Highlightings));
                writer.WriteStartObjectAsync();
                var first = true;
                foreach (var kvp in result.Highlightings)
                {
                    if (first == false)
                        writer.WriteCommaAsync();
                    first = false;

                    writer.WritePropertyNameAsync(kvp.Key);
                    writer.WriteStartObjectAsync();
                    var firstInner = true;
                    foreach (var kvpInner in kvp.Value)
                    {
                        if (firstInner == false)
                            writer.WriteCommaAsync();
                        firstInner = false;

                        writer.WriteArray(kvpInner.Key, kvpInner.Value);
                    }

                    writer.WriteEndObjectAsync();
                }

                writer.WriteEndObjectAsync();
            }

            if (result.Explanations != null)
            {
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(result.Explanations));
                writer.WriteStartObjectAsync();
                var first = true;
                foreach (var kvp in result.Explanations)
                {
                    if (first == false)
                        writer.WriteCommaAsync();
                    first = false;

                    writer.WriteArray(kvp.Key, kvp.Value);
                }

                writer.WriteEndObjectAsync();
            }

            var counters = result.GetCounterIncludes();
            if (counters != null)
            {
                writer.WriteCommaAsync();
                writer.WritePropertyNameAsync(nameof(result.CounterIncludes));
                await writer.WriteCountersAsync(counters);

                writer.WriteCommaAsync();
                writer.WritePropertyNameAsync(nameof(result.IncludedCounterNames));
                writer.WriteIncludedCounterNames(result.IncludedCounterNames);
            }

            var timeSeries = result.GetTimeSeriesIncludes();
            if (timeSeries != null)
            {
                writer.WriteCommaAsync();
                writer.WritePropertyNameAsync(nameof(result.TimeSeriesIncludes));
                await writer.WriteTimeSeriesAsync(timeSeries);
            }

            var compareExchangeValues = result.GetCompareExchangeValueIncludes();
            if (compareExchangeValues != null)
            {
                writer.WriteCommaAsync();
                writer.WritePropertyNameAsync(nameof(result.CompareExchangeValueIncludes));
                await writer.WriteCompareExchangeValues(compareExchangeValues);
            }

            writeAdditionalData?.Invoke(writer);

            writer.WriteEndObjectAsync();
            return numberOfResults;
        }

        public static void WriteIncludedCounterNames(this AbstractBlittableJsonTextWriter writer, Dictionary<string, string[]> includedCounterNames)
        {
            writer.WriteStartObjectAsync();

            var first = true;
            foreach (var kvp in includedCounterNames)
            {
                if (first == false)
                    writer.WriteCommaAsync();

                first = false;

                writer.WriteArray(kvp.Key, kvp.Value);
            }

            writer.WriteEndObjectAsync();
        }

        public static void WriteQueryResult<TResult, TInclude>(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, QueryResultBase<TResult, TInclude> result, bool metadataOnly, out long numberOfResults, bool partial = false)
        {
            if (partial == false)
                writer.WriteStartObjectAsync();

            writer.WritePropertyNameAsync(nameof(result.IndexName));
            writer.WriteStringAsync(result.IndexName);
            writer.WriteCommaAsync();

            var results = (object)result.Results;
            if (results is List<Document> documents)
            {
                writer.WritePropertyNameAsync(nameof(result.Results));
                writer.WriteDocuments(context, documents, metadataOnly, out numberOfResults);
                writer.WriteCommaAsync();
            }
            else if (results is List<BlittableJsonReaderObject> objects)
            {
                writer.WritePropertyNameAsync(nameof(result.Results));
                writer.WriteObjects(context, objects, out numberOfResults);
                writer.WriteCommaAsync();
            }
            else if (results is List<FacetResult> facets)
            {
                numberOfResults = facets.Count;

                writer.WriteArray(context, nameof(result.Results), facets, (w, c, facet) => w.WriteFacetResult(c, facet));
                writer.WriteCommaAsync();
            }
            else if (results is List<SuggestionResult> suggestions)
            {
                numberOfResults = suggestions.Count;

                writer.WriteArray(context, nameof(result.Results), suggestions, (w, c, suggestion) => w.WriteSuggestionResult(c, suggestion));
                writer.WriteCommaAsync();
            }
            else
                throw new NotSupportedException($"Cannot write query result of '{typeof(TResult)}' type in '{result.GetType()}'.");

            var includes = (object)result.Includes;
            if (includes is List<Document> includeDocuments)
            {
                writer.WritePropertyNameAsync(nameof(result.Includes));
                writer.WriteIncludes(context, includeDocuments);
                writer.WriteCommaAsync();
            }
            else if (includes is List<BlittableJsonReaderObject> includeObjects)
            {
                if (includeObjects.Count != 0)
                    throw new NotSupportedException("Cannot write query includes of List<BlittableJsonReaderObject>, but got non zero response");

                writer.WritePropertyNameAsync(nameof(result.Includes));
                writer.WriteStartObjectAsync();
                writer.WriteEndObjectAsync();
                writer.WriteCommaAsync();
            }
            else
                throw new NotSupportedException($"Cannot write query includes of '{typeof(TInclude)}' type in '{result.GetType()}'.");

            writer.WritePropertyNameAsync(nameof(result.IndexTimestamp));
            writer.WriteStringAsync(result.IndexTimestamp.ToString(DefaultFormat.DateTimeFormatsToWrite));
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(result.LastQueryTime));
            writer.WriteStringAsync(result.LastQueryTime.ToString(DefaultFormat.DateTimeFormatsToWrite));
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(result.IsStale));
            writer.WriteBool(result.IsStale);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(result.ResultEtag));
            writer.WriteIntegerAsync(result.ResultEtag);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(result.NodeTag));
            writer.WriteStringAsync(result.NodeTag);

            if (partial == false)
                writer.WriteEndObjectAsync();
        }

        public static async Task<int> WriteQueryResultAsync<TResult>(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, QueryResultServerSide<TResult> result, bool metadataOnly, bool partial = false)
        {
            int numberOfResults;

            if (partial == false)
                writer.WriteStartObjectAsync();

            writer.WritePropertyNameAsync(nameof(result.IndexName));
            writer.WriteStringAsync(result.IndexName);
            writer.WriteCommaAsync();

            var results = (object)result.Results;
            if (results is List<Document> documents)
            {
                writer.WritePropertyNameAsync(nameof(result.Results));
                numberOfResults = await writer.WriteDocumentsAsync(context, documents, metadataOnly);
                writer.WriteCommaAsync();
            }
            else if (results is List<BlittableJsonReaderObject> objects)
            {
                writer.WritePropertyNameAsync(nameof(result.Results));
                numberOfResults = await writer.WriteObjectsAsync(context, objects);
                writer.WriteCommaAsync();
            }
            else if (results is List<FacetResult> facets)
            {
                numberOfResults = facets.Count;

                writer.WriteArray(context, nameof(result.Results), facets, (w, c, facet) => w.WriteFacetResult(c, facet));
                writer.WriteCommaAsync();
                await writer.MaybeOuterFlushAsync();
            }
            else if (results is List<SuggestionResult> suggestions)
            {
                numberOfResults = suggestions.Count;

                writer.WriteArray(context, nameof(result.Results), suggestions, (w, c, suggestion) => w.WriteSuggestionResult(c, suggestion));
                writer.WriteCommaAsync();
                await writer.MaybeOuterFlushAsync();
            }
            else
                throw new NotSupportedException($"Cannot write query result of '{typeof(TResult)}' type in '{result.GetType()}'.");

            var includes = (object)result.Includes;
            if (includes is List<Document> includeDocuments)
            {
                writer.WritePropertyNameAsync(nameof(result.Includes));
                await writer.WriteIncludesAsync(context, includeDocuments);
                writer.WriteCommaAsync();
            }
            else if (includes is List<BlittableJsonReaderObject> includeObjects)
            {
                if (includeObjects.Count != 0)
                    throw new NotSupportedException("Cannot write query includes of List<BlittableJsonReaderObject>, but got non zero response");

                writer.WritePropertyNameAsync(nameof(result.Includes));
                writer.WriteStartObjectAsync();
                writer.WriteEndObjectAsync();
                writer.WriteCommaAsync();
            }
            else
                throw new NotSupportedException($"Cannot write query includes of '{includes.GetType()}' type in '{result.GetType()}'.");

            writer.WritePropertyNameAsync(nameof(result.IndexTimestamp));
            writer.WriteStringAsync(result.IndexTimestamp.ToString(DefaultFormat.DateTimeFormatsToWrite));
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(result.LastQueryTime));
            writer.WriteStringAsync(result.LastQueryTime.ToString(DefaultFormat.DateTimeFormatsToWrite));
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(result.IsStale));
            writer.WriteBool(result.IsStale);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(result.ResultEtag));
            writer.WriteIntegerAsync(result.ResultEtag);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(result.NodeTag));
            writer.WriteStringAsync(result.NodeTag);

            if (result.Timings != null)
            {
                writer.WriteCommaAsync();
                writer.WritePropertyNameAsync(nameof(result.Timings));
                writer.WriteQueryTimings(context, result.Timings);
            }

            if (result.TimeSeriesFields != null)
            {
                writer.WriteCommaAsync();
                writer.WriteArray(nameof(result.TimeSeriesFields), result.TimeSeriesFields);
            }

            if (partial == false)
                writer.WriteEndObjectAsync();

            return numberOfResults;
        }

        public static void WriteQueryTimings(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, QueryTimings queryTimings)
        {
            writer.WriteStartObjectAsync();

            writer.WritePropertyNameAsync(nameof(QueryTimings.DurationInMs));
            writer.WriteIntegerAsync(queryTimings.DurationInMs);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(QueryTimings.Timings));
            if (queryTimings.Timings != null)
            {
                writer.WriteStartObjectAsync();
                var first = true;

                foreach (var kvp in queryTimings.Timings)
                {
                    if (first == false)
                        writer.WriteCommaAsync();

                    first = false;

                    writer.WritePropertyNameAsync(kvp.Key);
                    writer.WriteQueryTimings(context, kvp.Value);
                }

                writer.WriteEndObjectAsync();
            }
            else
                writer.WriteNullAsync();

            writer.WriteEndObjectAsync();
        }

        public static void WriteTermsQueryResult(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, TermsQueryResultServerSide queryResult)
        {
            writer.WriteStartObjectAsync();

            writer.WritePropertyNameAsync(nameof(queryResult.IndexName));
            writer.WriteStringAsync(queryResult.IndexName);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(queryResult.ResultEtag));
            writer.WriteIntegerAsync(queryResult.ResultEtag);
            writer.WriteCommaAsync();

            writer.WriteArray(nameof(queryResult.Terms), queryResult.Terms);

            writer.WriteEndObjectAsync();
        }

        public static void WriteIndexingPerformanceStats(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, IndexingPerformanceStats stats)
        {
            var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(stats);
            writer.WriteObjectAsync(context.ReadObject(djv, "index/performance"));
        }

        public static void WriteEtlPerformanceStats(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, EtlPerformanceStats stats)
        {
            var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(stats);
            writer.WriteObjectAsync(context.ReadObject(djv, "etl/performance"));
        }

        public static void WriteIndexQuery(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, IIndexQuery query)
        {
            var indexQuery = query as IndexQueryServerSide;
            if (indexQuery != null)
            {
                writer.WriteIndexQuery(context, indexQuery);
                return;
            }

            throw new NotSupportedException($"Not supported query type: {query.GetType()}");
        }

        private static void WriteIndexQuery(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, IndexQueryServerSide query)
        {
            writer.WriteStartObjectAsync();

            writer.WritePropertyNameAsync(nameof(query.PageSize));
            writer.WriteIntegerAsync(query.PageSize);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(query.Query));
            if (query.Query != null)
                writer.WriteStringAsync(query.Query);
            else
                writer.WriteNullAsync();
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(query.SkipDuplicateChecking));
            writer.WriteBool(query.SkipDuplicateChecking);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(query.Start));
            writer.WriteIntegerAsync(query.Start);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(query.WaitForNonStaleResults));
            writer.WriteBool(query.WaitForNonStaleResults);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(query.WaitForNonStaleResultsTimeout));
            if (query.WaitForNonStaleResultsTimeout.HasValue)
                writer.WriteStringAsync(query.WaitForNonStaleResultsTimeout.Value.ToString());
            else
                writer.WriteNullAsync();

            writer.WriteEndObjectAsync();
        }

        public static void WriteDetailedDatabaseStatistics(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, DetailedDatabaseStatistics statistics)
        {
            writer.WriteStartObjectAsync();

            writer.WritePropertyNameAsync(nameof(statistics.CountOfIdentities));
            writer.WriteIntegerAsync(statistics.CountOfIdentities);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(statistics.CountOfCompareExchange));
            writer.WriteIntegerAsync(statistics.CountOfCompareExchange);
            writer.WriteCommaAsync();

            WriteDatabaseStatisticsInternal(writer, statistics);

            writer.WriteEndObjectAsync();
        }

        public static void WriteDatabaseStatistics(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, DatabaseStatistics statistics)
        {
            writer.WriteStartObjectAsync();

            WriteDatabaseStatisticsInternal(writer, statistics);

            writer.WriteEndObjectAsync();
        }

        private static void WriteDatabaseStatisticsInternal(AsyncBlittableJsonTextWriter writer, DatabaseStatistics statistics)
        {
            writer.WritePropertyNameAsync(nameof(statistics.CountOfIndexes));
            writer.WriteIntegerAsync(statistics.CountOfIndexes);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(statistics.CountOfDocuments));
            writer.WriteIntegerAsync(statistics.CountOfDocuments);
            writer.WriteCommaAsync();

            if (statistics.CountOfRevisionDocuments > 0)
            {
                writer.WritePropertyNameAsync(nameof(statistics.CountOfRevisionDocuments));
                writer.WriteIntegerAsync(statistics.CountOfRevisionDocuments);
                writer.WriteCommaAsync();
            }

            writer.WritePropertyNameAsync(nameof(statistics.CountOfTombstones));
            writer.WriteIntegerAsync(statistics.CountOfTombstones);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(statistics.CountOfDocumentsConflicts));
            writer.WriteIntegerAsync(statistics.CountOfDocumentsConflicts);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(statistics.CountOfConflicts));
            writer.WriteIntegerAsync(statistics.CountOfConflicts);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(statistics.CountOfAttachments));
            writer.WriteIntegerAsync(statistics.CountOfAttachments);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(statistics.CountOfCounterEntries));
            writer.WriteIntegerAsync(statistics.CountOfCounterEntries);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(statistics.CountOfTimeSeriesSegments));
            writer.WriteIntegerAsync(statistics.CountOfTimeSeriesSegments);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(statistics.CountOfUniqueAttachments));
            writer.WriteIntegerAsync(statistics.CountOfUniqueAttachments);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(statistics.DatabaseChangeVector));
            writer.WriteStringAsync(statistics.DatabaseChangeVector);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(statistics.DatabaseId));
            writer.WriteStringAsync(statistics.DatabaseId);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(statistics.NumberOfTransactionMergerQueueOperations));
            writer.WriteIntegerAsync(statistics.NumberOfTransactionMergerQueueOperations);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(statistics.Is64Bit));
            writer.WriteBool(statistics.Is64Bit);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(statistics.Pager));
            writer.WriteStringAsync(statistics.Pager);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(statistics.LastDocEtag));
            if (statistics.LastDocEtag.HasValue)
                writer.WriteIntegerAsync(statistics.LastDocEtag.Value);
            else
                writer.WriteNullAsync();
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(statistics.LastDatabaseEtag));
            if (statistics.LastDatabaseEtag.HasValue)
                writer.WriteIntegerAsync(statistics.LastDatabaseEtag.Value);
            else
                writer.WriteNullAsync();
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync((nameof(statistics.DatabaseChangeVector)));
            writer.WriteStringAsync(statistics.DatabaseChangeVector);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(statistics.LastIndexingTime));
            if (statistics.LastIndexingTime.HasValue)
                writer.WriteDateTimeAsync(statistics.LastIndexingTime.Value, isUtc: true);
            else
                writer.WriteNullAsync();
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(statistics.SizeOnDisk));
            writer.WriteStartObjectAsync();

            writer.WritePropertyNameAsync(nameof(statistics.SizeOnDisk.HumaneSize));
            writer.WriteStringAsync(statistics.SizeOnDisk.HumaneSize);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(statistics.SizeOnDisk.SizeInBytes));
            writer.WriteIntegerAsync(statistics.SizeOnDisk.SizeInBytes);

            writer.WriteEndObjectAsync();
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(statistics.TempBuffersSizeOnDisk));
            writer.WriteStartObjectAsync();

            writer.WritePropertyNameAsync(nameof(statistics.TempBuffersSizeOnDisk.HumaneSize));
            writer.WriteStringAsync(statistics.TempBuffersSizeOnDisk.HumaneSize);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(statistics.TempBuffersSizeOnDisk.SizeInBytes));
            writer.WriteIntegerAsync(statistics.TempBuffersSizeOnDisk.SizeInBytes);

            writer.WriteEndObjectAsync();
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(statistics.Indexes));
            writer.WriteStartArrayAsync();
            var isFirstInternal = true;
            foreach (var index in statistics.Indexes)
            {
                if (isFirstInternal == false)
                    writer.WriteCommaAsync();

                isFirstInternal = false;

                writer.WriteStartObjectAsync();

                writer.WritePropertyNameAsync(nameof(index.IsStale));
                writer.WriteBool(index.IsStale);
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(index.Name));
                writer.WriteStringAsync(index.Name);
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(index.LockMode));
                writer.WriteStringAsync(index.LockMode.ToString());
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(index.Priority));
                writer.WriteStringAsync(index.Priority.ToString());
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(index.State));
                writer.WriteStringAsync(index.State.ToString());
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(index.Type));
                writer.WriteStringAsync(index.Type.ToString());
                writer.WriteCommaAsync();
                
                writer.WritePropertyNameAsync(nameof(index.SourceType));
                writer.WriteStringAsync(index.SourceType.ToString());
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(index.LastIndexingTime));
                if (index.LastIndexingTime.HasValue)
                    writer.WriteDateTimeAsync(index.LastIndexingTime.Value, isUtc: true);
                else
                    writer.WriteNullAsync();

                writer.WriteEndObjectAsync();
            }

            writer.WriteEndArrayAsync();
        }

        public static void WriteIndexDefinition(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, IndexDefinition indexDefinition, bool removeAnalyzers = false)
        {
            writer.WriteStartObjectAsync();

            writer.WritePropertyNameAsync(nameof(indexDefinition.Name));
            writer.WriteStringAsync(indexDefinition.Name);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(indexDefinition.SourceType));
            writer.WriteStringAsync(indexDefinition.SourceType.ToString());
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(indexDefinition.Type));
            writer.WriteStringAsync(indexDefinition.Type.ToString());
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(indexDefinition.LockMode));
            if (indexDefinition.LockMode.HasValue)
                writer.WriteStringAsync(indexDefinition.LockMode.ToString());
            else
                writer.WriteNullAsync();
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(indexDefinition.Priority));
            if (indexDefinition.Priority.HasValue)
                writer.WriteStringAsync(indexDefinition.Priority.ToString());
            else
                writer.WriteNullAsync();
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(indexDefinition.OutputReduceToCollection));
            writer.WriteStringAsync(indexDefinition.OutputReduceToCollection);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(indexDefinition.ReduceOutputIndex));

            if (indexDefinition.ReduceOutputIndex.HasValue)
                writer.WriteIntegerAsync(indexDefinition.ReduceOutputIndex.Value);
            else
                writer.WriteNullAsync();
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(indexDefinition.PatternForOutputReduceToCollectionReferences));
            writer.WriteStringAsync(indexDefinition.PatternForOutputReduceToCollectionReferences);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(indexDefinition.PatternReferencesCollectionName));
            writer.WriteStringAsync(indexDefinition.PatternReferencesCollectionName);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(indexDefinition.Configuration));
            writer.WriteStartObjectAsync();
            var isFirstInternal = true;
            foreach (var kvp in indexDefinition.Configuration)
            {
                if (isFirstInternal == false)
                    writer.WriteCommaAsync();

                isFirstInternal = false;

                writer.WritePropertyNameAsync(kvp.Key);
                writer.WriteStringAsync(kvp.Value);
            }
            writer.WriteEndObjectAsync();
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(indexDefinition.AdditionalSources));
            writer.WriteStartObjectAsync();
            isFirstInternal = true;
            foreach (var kvp in indexDefinition.AdditionalSources)
            {
                if (isFirstInternal == false)
                    writer.WriteCommaAsync();

                isFirstInternal = false;

                writer.WritePropertyNameAsync(kvp.Key);
                writer.WriteStringAsync(kvp.Value);
            }
            writer.WriteEndObjectAsync();
            writer.WriteCommaAsync();

#if FEATURE_TEST_INDEX
            writer.WritePropertyName(nameof(indexDefinition.IsTestIndex));
            writer.WriteBool(indexDefinition.IsTestIndex);
            writer.WriteComma();
#endif

            writer.WritePropertyNameAsync(nameof(indexDefinition.Reduce));
            if (string.IsNullOrWhiteSpace(indexDefinition.Reduce) == false)
                writer.WriteStringAsync(indexDefinition.Reduce);
            else
                writer.WriteNullAsync();
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(indexDefinition.Maps));
            writer.WriteStartArrayAsync();
            isFirstInternal = true;
            foreach (var map in indexDefinition.Maps)
            {
                if (isFirstInternal == false)
                    writer.WriteCommaAsync();

                isFirstInternal = false;
                writer.WriteStringAsync(map);
            }
            writer.WriteEndArrayAsync();
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(indexDefinition.Fields));
            writer.WriteStartObjectAsync();
            isFirstInternal = true;
            foreach (var kvp in indexDefinition.Fields)
            {
                if (isFirstInternal == false)
                    writer.WriteCommaAsync();

                isFirstInternal = false;
                writer.WritePropertyNameAsync(kvp.Key);
                if (kvp.Value != null)
                    writer.WriteIndexFieldOptions(context, kvp.Value, removeAnalyzers);
                else
                    writer.WriteNullAsync();
            }
            writer.WriteEndObjectAsync();

            writer.WriteEndObjectAsync();
        }

        public static void WriteIndexProgress(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, IndexProgress progress)
        {
            writer.WriteStartObjectAsync();

            writer.WritePropertyNameAsync(nameof(progress.IsStale));
            writer.WriteBool(progress.IsStale);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(progress.IndexRunningStatus));
            writer.WriteStringAsync(progress.IndexRunningStatus.ToString());
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(progress.ProcessedPerSecond));
            writer.WriteDoubleAsync(progress.ProcessedPerSecond);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(progress.Collections));
            if (progress.Collections == null)
            {
                writer.WriteNullAsync();
            }
            else
            {
                writer.WriteStartObjectAsync();
                var isFirst = true;
                foreach (var kvp in progress.Collections)
                {
                    if (isFirst == false)
                        writer.WriteCommaAsync();

                    isFirst = false;

                    writer.WritePropertyNameAsync(kvp.Key);

                    writer.WriteStartObjectAsync();

                    writer.WritePropertyNameAsync(nameof(kvp.Value.LastProcessedDocumentEtag));
                    writer.WriteIntegerAsync(kvp.Value.LastProcessedDocumentEtag);
                    writer.WriteCommaAsync();

                    writer.WritePropertyNameAsync(nameof(kvp.Value.LastProcessedTombstoneEtag));
                    writer.WriteIntegerAsync(kvp.Value.LastProcessedTombstoneEtag);
                    writer.WriteCommaAsync();

                    writer.WritePropertyNameAsync(nameof(kvp.Value.NumberOfDocumentsToProcess));
                    writer.WriteIntegerAsync(kvp.Value.NumberOfDocumentsToProcess);
                    writer.WriteCommaAsync();

                    writer.WritePropertyNameAsync(nameof(kvp.Value.NumberOfTombstonesToProcess));
                    writer.WriteIntegerAsync(kvp.Value.NumberOfTombstonesToProcess);
                    writer.WriteCommaAsync();

                    writer.WritePropertyNameAsync(nameof(kvp.Value.TotalNumberOfDocuments));
                    writer.WriteIntegerAsync(kvp.Value.TotalNumberOfDocuments);
                    writer.WriteCommaAsync();

                    writer.WritePropertyNameAsync(nameof(kvp.Value.TotalNumberOfTombstones));
                    writer.WriteIntegerAsync(kvp.Value.TotalNumberOfTombstones);

                    writer.WriteEndObjectAsync();
                }
                writer.WriteEndObjectAsync();
            }

            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(progress.Name));
            writer.WriteStringAsync(progress.Name);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(progress.Type));
            writer.WriteStringAsync(progress.Type.ToString());
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(progress.SourceType));
            writer.WriteStringAsync(progress.SourceType.ToString());
            
            writer.WriteEndObjectAsync();
        }

        public static void WriteIndexStats(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, IndexStats stats)
        {
            var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(stats);
            writer.WriteObjectAsync(context.ReadObject(djv, "index/stats"));
        }

        private static void WriteIndexFieldOptions(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, IndexFieldOptions options, bool removeAnalyzers)
        {
            writer.WriteStartObjectAsync();

            writer.WritePropertyNameAsync(nameof(options.Analyzer));
            if (string.IsNullOrWhiteSpace(options.Analyzer) == false && !removeAnalyzers)
                writer.WriteStringAsync(options.Analyzer);
            else
                writer.WriteNullAsync();
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(options.Indexing));
            if (options.Indexing.HasValue)
                writer.WriteStringAsync(options.Indexing.ToString());
            else
                writer.WriteNullAsync();
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(options.Storage));
            if (options.Storage.HasValue)
                writer.WriteStringAsync(options.Storage.ToString());
            else
                writer.WriteNullAsync();
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(options.Suggestions));
            if (options.Suggestions.HasValue)
                writer.WriteBool(options.Suggestions.Value);
            else
                writer.WriteNullAsync();
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(options.TermVector));
            if (options.TermVector.HasValue)
                writer.WriteStringAsync(options.TermVector.ToString());
            else
                writer.WriteNullAsync();
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(options.Spatial));
            if (options.Spatial != null)
            {
                writer.WriteStartObjectAsync();

                writer.WritePropertyNameAsync(nameof(options.Spatial.Type));
                writer.WriteStringAsync(options.Spatial.Type.ToString());
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(options.Spatial.MaxTreeLevel));
                writer.WriteIntegerAsync(options.Spatial.MaxTreeLevel);
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(options.Spatial.MaxX));
                LazyStringValue lazyStringValue;
                using (lazyStringValue = context.GetLazyString(CharExtensions.ToInvariantString(options.Spatial.MaxX)))
                    writer.WriteDoubleAsync(new LazyNumberValue(lazyStringValue));
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(options.Spatial.MaxY));
                using (lazyStringValue = context.GetLazyString(CharExtensions.ToInvariantString(options.Spatial.MaxY)))
                    writer.WriteDoubleAsync(new LazyNumberValue(lazyStringValue));
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(options.Spatial.MinX));
                using (lazyStringValue = context.GetLazyString(CharExtensions.ToInvariantString(options.Spatial.MinX)))
                    writer.WriteDoubleAsync(new LazyNumberValue(lazyStringValue));
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(options.Spatial.MinY));
                using (lazyStringValue = context.GetLazyString(CharExtensions.ToInvariantString(options.Spatial.MinY)))
                    writer.WriteDoubleAsync(new LazyNumberValue(lazyStringValue));
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(options.Spatial.Strategy));
                writer.WriteStringAsync(options.Spatial.Strategy.ToString());
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(options.Spatial.Units));
                writer.WriteStringAsync(options.Spatial.Units.ToString());

                writer.WriteEndObjectAsync();
            }
            else
                writer.WriteNullAsync();

            writer.WriteEndObjectAsync();
        }

        public static void WriteDocuments(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, IEnumerable<Document> documents, bool metadataOnly, out long numberOfResults)
        {
            WriteDocuments(writer, context, documents.GetEnumerator(), metadataOnly, out numberOfResults);
        }

        public static void WriteDocuments(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, IEnumerator<Document> documents, bool metadataOnly,
            out long numberOfResults)
        {
            numberOfResults = 0;

            writer.WriteStartArrayAsync();

            var first = true;

            while (documents.MoveNext())
            {
                numberOfResults++;

                if (first == false)
                    writer.WriteCommaAsync();
                first = false;

                WriteDocument(writer, context, documents.Current, metadataOnly);
            }

            writer.WriteEndArrayAsync();
        }

        public static async Task<int> WriteDocumentsAsync(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, IEnumerable<Document> documents, bool metadataOnly)
        {
            int numberOfResults = 0;

            writer.WriteStartArrayAsync();

            var first = true;
            foreach (var document in documents)
            {
                numberOfResults++;

                if (first == false)
                    writer.WriteCommaAsync();
                first = false;

                WriteDocument(writer, context, document, metadataOnly);
                await writer.MaybeOuterFlushAsync();
            }

            writer.WriteEndArrayAsync();
            return numberOfResults;
        }

        public static void WriteDocument(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, Document document, bool metadataOnly, Func<LazyStringValue, bool> filterMetadataProperty = null)
        {
            if (document == null)
            {
                writer.WriteNullAsync();
                return;
            }

            if (document == Document.ExplicitNull)
            {
                writer.WriteNullAsync();
                return;
            }

            // Explicitly not disposing it, a single document can be 
            // used multiple times in a single query, for example, due to projections
            // so we will let the context handle it, rather than handle it directly ourselves
            //using (document.Data)
            {
                if (metadataOnly == false)
                    writer.WriteDocumentInternal(context, document, filterMetadataProperty);
                else
                    writer.WriteDocumentMetadata(context, document, filterMetadataProperty);
            }
        }

        public static void WriteIncludes(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, List<Document> includes)
        {
            writer.WriteStartObjectAsync();

            var first = true;
            foreach (var document in includes)
            {
                if (first == false)
                    writer.WriteCommaAsync();
                first = false;

                if (document is IncludeDocumentsCommand.ConflictDocument conflict)
                {
                    writer.WritePropertyNameAsync(conflict.Id);
                    WriteConflict(writer, conflict);
                    continue;
                }

                writer.WritePropertyNameAsync(document.Id);
                WriteDocument(writer, context, metadataOnly: false, document: document);
            }

            writer.WriteEndObjectAsync();
        }

        public static async Task WriteIncludesAsync(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, List<Document> includes)
        {
            writer.WriteStartObjectAsync();

            var first = true;
            foreach (var document in includes)
            {
                if (first == false)
                    writer.WriteCommaAsync();
                first = false;

                if (document is IncludeDocumentsCommand.ConflictDocument conflict)
                {
                    writer.WritePropertyNameAsync(conflict.Id);
                    WriteConflict(writer, conflict);
                    await writer.MaybeOuterFlushAsync();
                    continue;
                }

                writer.WritePropertyNameAsync(document.Id);
                WriteDocument(writer, context, metadataOnly: false, document: document);
                await writer.MaybeOuterFlushAsync();
            }

            writer.WriteEndObjectAsync();
        }

        private static void WriteConflict(AbstractBlittableJsonTextWriter writer, IncludeDocumentsCommand.ConflictDocument conflict)
        {
            writer.WriteStartObjectAsync();

            writer.WritePropertyNameAsync(Constants.Documents.Metadata.Key);
            writer.WriteStartObjectAsync();

            writer.WritePropertyNameAsync(Constants.Documents.Metadata.Id);
            writer.WriteStringAsync(conflict.Id);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(Constants.Documents.Metadata.ChangeVector);
            writer.WriteStringAsync(string.Empty);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(Constants.Documents.Metadata.Conflict);
            writer.WriteBool(true);

            writer.WriteEndObjectAsync();

            writer.WriteEndObjectAsync();
        }

        public static void WriteObjects(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, IEnumerable<BlittableJsonReaderObject> objects, out long numberOfResults)
        {
            numberOfResults = 0;

            writer.WriteStartArrayAsync();

            var first = true;
            foreach (var o in objects)
            {
                numberOfResults++;

                if (first == false)
                    writer.WriteCommaAsync();
                first = false;

                if (o == null)
                {
                    writer.WriteNullAsync();
                    continue;
                }

                using (o)
                {
                    writer.WriteObjectAsync(o);
                }
            }

            writer.WriteEndArrayAsync();
        }

        public static async Task<int> WriteObjectsAsync(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, IEnumerable<BlittableJsonReaderObject> objects)
        {
            int numberOfResults = 0;

            writer.WriteStartArrayAsync();

            var first = true;
            foreach (var o in objects)
            {
                numberOfResults++;

                if (first == false)
                    writer.WriteCommaAsync();
                first = false;

                if (o == null)
                {
                    writer.WriteNullAsync();
                    continue;
                }

                using (o)
                {
                    writer.WriteObjectAsync(o);
                }

                await writer.MaybeOuterFlushAsync();
            }

            writer.WriteEndArrayAsync();
            return numberOfResults;
        }

        public static void WriteCounters(this AsyncBlittableJsonTextWriter writer, Dictionary<string, List<CounterDetail>> counters)
        {
            writer.WriteStartObjectAsync();

            var first = true;
            foreach (var kvp in counters)
            {
                if (first == false)
                    writer.WriteCommaAsync();

                first = false;

                writer.WritePropertyNameAsync(kvp.Key);

                writer.WriteCountersForDocument(kvp.Value);
            }

            writer.WriteEndObjectAsync();
        }

        public static async Task WriteCountersAsync(this AsyncBlittableJsonTextWriter writer, Dictionary<string, List<CounterDetail>> counters)
        {
            writer.WriteStartObjectAsync();

            var first = true;
            foreach (var kvp in counters)
            {
                if (first == false)
                    writer.WriteCommaAsync();

                first = false;

                writer.WritePropertyNameAsync(kvp.Key);

                await writer.WriteCountersForDocumentAsync(kvp.Value);
            }

            writer.WriteEndObjectAsync();
        }

        private static void WriteCountersForDocument(this AsyncBlittableJsonTextWriter writer, List<CounterDetail> counters)
        {
            writer.WriteStartArrayAsync();

            var first = true;
            foreach (var counter in counters)
            {
                if (first == false)
                    writer.WriteCommaAsync();
                first = false;

                writer.WriteStartObjectAsync();

                writer.WritePropertyNameAsync(nameof(CounterDetail.DocumentId));
                writer.WriteStringAsync(counter.DocumentId);
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(CounterDetail.CounterName));
                writer.WriteStringAsync(counter.CounterName);
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(CounterDetail.TotalValue));
                writer.WriteIntegerAsync(counter.TotalValue);

                writer.WriteEndObjectAsync();
            }

            writer.WriteEndArrayAsync();
        }

        private static async Task WriteCountersForDocumentAsync(this AsyncBlittableJsonTextWriter writer, List<CounterDetail> counters)
        {
            writer.WriteStartArrayAsync();

            var first = true;
            foreach (var counter in counters)
            {
                if (first == false)
                    writer.WriteCommaAsync();
                first = false;

                if (counter == null)
                {
                    writer.WriteNullAsync();
                    await writer.MaybeOuterFlushAsync();
                    continue;
                }

                writer.WriteStartObjectAsync();

                writer.WritePropertyNameAsync(nameof(CounterDetail.DocumentId));
                writer.WriteStringAsync(counter.DocumentId);
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(CounterDetail.CounterName));
                writer.WriteStringAsync(counter.CounterName);
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(CounterDetail.TotalValue));
                writer.WriteIntegerAsync(counter.TotalValue);

                writer.WriteEndObjectAsync();

                await writer.MaybeOuterFlushAsync();
            }

            writer.WriteEndArrayAsync();
        }

        public static async Task WriteCompareExchangeValues(this AsyncBlittableJsonTextWriter writer, Dictionary<string, CompareExchangeValue<BlittableJsonReaderObject>> compareExchangeValues)
        {
            writer.WriteStartObjectAsync();

            var first = true;
            foreach (var kvp in compareExchangeValues)
            {
                if (first == false)
                    writer.WriteCommaAsync();

                first = false;

                writer.WritePropertyNameAsync(kvp.Key);

                writer.WriteStartObjectAsync();

                writer.WritePropertyNameAsync(nameof(kvp.Value.Key));
                writer.WriteStringAsync(kvp.Key);
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(kvp.Value.Index));
                writer.WriteIntegerAsync(kvp.Value.Index);
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(kvp.Value));
                writer.WriteObjectAsync(kvp.Value.Value);

                writer.WriteEndObjectAsync();

                await writer.MaybeOuterFlushAsync();
            }

            writer.WriteEndObjectAsync();
        }

        public static async Task WriteTimeSeriesAsync(this AsyncBlittableJsonTextWriter writer,
            Dictionary<string, Dictionary<string, List<TimeSeriesRangeResult>>> timeSeries)
        {
            writer.WriteStartObjectAsync();
            
            var first = true;
            foreach (var kvp in timeSeries)
            {
                if (first == false)
                    writer.WriteCommaAsync();

                first = false;

                writer.WritePropertyNameAsync(kvp.Key);

                await TimeSeriesHandler.WriteTimeSeriesRangeResults(context: null, writer, documentId: null, kvp.Value);
            }

            writer.WriteEndObjectAsync();
        }

        public static void WriteDocumentMetadata(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context,
            Document document, Func<LazyStringValue, bool> filterMetadataProperty = null)
        {
            writer.WriteStartObjectAsync();
            document.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata);
            WriteMetadata(writer, document, metadata, filterMetadataProperty);

            writer.WriteEndObjectAsync();
        }

        public static void WriteMetadata(this AbstractBlittableJsonTextWriter writer, Document document, BlittableJsonReaderObject metadata, Func<LazyStringValue, bool> filterMetadataProperty = null)
        {
            writer.WritePropertyNameAsync(Constants.Documents.Metadata.Key);
            writer.WriteStartObjectAsync();
            bool first = true;
            if (metadata != null)
            {
                var size = metadata.Count;
                var prop = new BlittableJsonReaderObject.PropertyDetails();

                for (int i = 0; i < size; i++)
                {
                    metadata.GetPropertyByIndex(i, ref prop);

                    if (filterMetadataProperty != null && filterMetadataProperty(prop.Name))
                        continue;

                    if (first == false)
                    {
                        writer.WriteCommaAsync();
                    }
                    first = false;
                    writer.WritePropertyNameAsync(prop.Name);
                    writer.WriteValueAsync(prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value);
                }
            }

            if (first == false)
            {
                writer.WriteCommaAsync();
            }
            writer.WritePropertyNameAsync(Constants.Documents.Metadata.ChangeVector);
            writer.WriteStringAsync(document.ChangeVector);

            if (document.Flags != DocumentFlags.None)
            {
                writer.WriteCommaAsync();
                writer.WritePropertyNameAsync(Constants.Documents.Metadata.Flags);
                writer.WriteStringAsync(document.Flags.ToString());
            }
            if (document.Id != null)
            {
                writer.WriteCommaAsync();
                writer.WritePropertyNameAsync(Constants.Documents.Metadata.Id);
                writer.WriteStringAsync(document.Id);
            }
            if (document.IndexScore != null)
            {
                writer.WriteCommaAsync();
                writer.WritePropertyNameAsync(Constants.Documents.Metadata.IndexScore);
                writer.WriteDoubleAsync(document.IndexScore.Value);
            }
            if (document.Distance != null)
            {
                writer.WriteCommaAsync();
                var result = document.Distance.Value;
                writer.WritePropertyNameAsync(Constants.Documents.Metadata.SpatialResult);
                writer.WriteStartObjectAsync();
                writer.WritePropertyNameAsync(nameof(result.Distance));
                writer.WriteDoubleAsync(result.Distance);
                writer.WriteCommaAsync();
                writer.WritePropertyNameAsync(nameof(result.Latitude));
                writer.WriteDoubleAsync(result.Latitude);
                writer.WriteCommaAsync();
                writer.WritePropertyNameAsync(nameof(result.Longitude));
                writer.WriteDoubleAsync(result.Longitude);
                writer.WriteEndObjectAsync();
            }
            if (document.LastModified != DateTime.MinValue)
            {
                writer.WriteCommaAsync();
                writer.WritePropertyNameAsync(Constants.Documents.Metadata.LastModified);
                writer.WriteDateTimeAsync(document.LastModified, isUtc: true);
            }
            writer.WriteEndObjectAsync();
        }

        private static readonly StringSegment MetadataKeySegment = new StringSegment(Constants.Documents.Metadata.Key);

        private static void WriteDocumentInternal(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, Document document, Func<LazyStringValue, bool> filterMetadataProperty = null)
        {
            writer.WriteStartObjectAsync();
            WriteDocumentProperties(writer, context, document, filterMetadataProperty);
            writer.WriteEndObjectAsync();
        }

        private static void WriteDocumentProperties(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, Document document, Func<LazyStringValue, bool> filterMetadataProperty = null)
        {
            var first = true;
            BlittableJsonReaderObject metadata = null;
            var metadataField = context.GetLazyStringForFieldWithCaching(MetadataKeySegment);

            var prop = new BlittableJsonReaderObject.PropertyDetails();
            using (var buffers = document.Data.GetPropertiesByInsertionOrder())
            {
                for (var i = 0; i < buffers.Properties.Count; i++)
                {
                    document.Data.GetPropertyByIndex(buffers.Properties.Array[i + buffers.Properties.Offset], ref prop);
                    if (metadataField.Equals(prop.Name))
                    {
                        metadata = (BlittableJsonReaderObject)prop.Value;
                        continue;
                    }
                    if (first == false)
                    {
                        writer.WriteCommaAsync();
                    }
                    first = false;
                    writer.WritePropertyNameAsync(prop.Name);
                    writer.WriteValueAsync(prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value);
                }
            }

            if (first == false)
                writer.WriteCommaAsync();
            WriteMetadata(writer, document, metadata, filterMetadataProperty);
        }

        public static void WriteDocumentPropertiesWithoutMetadata(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, Document document)
        {
            var first = true;

            var prop = new BlittableJsonReaderObject.PropertyDetails();

            using (var buffers = document.Data.GetPropertiesByInsertionOrder())
            {
                for (var i = 0; i < buffers.Properties.Count; i++)
                {
                    document.Data.GetPropertyByIndex(buffers.Properties.Array[i + buffers.Properties.Offset], ref prop);
                    if (first == false)
                    {
                        writer.WriteCommaAsync();
                    }
                    first = false;
                    writer.WritePropertyNameAsync(prop.Name);
                    writer.WriteValueAsync(prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value);
                }
            }
        }

        public static void WriteOperationIdAndNodeTag(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, long operationId, string nodeTag)
        {
            writer.WriteStartObjectAsync();

            writer.WritePropertyNameAsync(nameof(OperationIdResult.OperationId));
            writer.WriteIntegerAsync(operationId);

            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(OperationIdResult.OperationNodeTag));
            writer.WriteStringAsync(nodeTag);

            writer.WriteEndObjectAsync();
        }

        public static void WriteArrayOfResultsAndCount(this AsyncBlittableJsonTextWriter writer, IEnumerable<string> results)
        {
            writer.WriteStartObjectAsync();
            writer.WritePropertyNameAsync("Results");
            writer.WriteStartArrayAsync();

            var first = true;
            var count = 0;

            foreach (var id in results)
            {
                if (first == false)
                    writer.WriteCommaAsync();

                writer.WriteStringAsync(id);
                count++;

                first = false;
            }

            writer.WriteEndArrayAsync();
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync("Count");
            writer.WriteIntegerAsync(count);

            writer.WriteEndObjectAsync();
        }

        public static void WriteReduceTrees(this AsyncBlittableJsonTextWriter writer, IEnumerable<ReduceTree> trees)
        {
            writer.WriteStartObjectAsync();
            writer.WritePropertyNameAsync("Results");

            writer.WriteStartArrayAsync();

            var first = true;

            foreach (var tree in trees)
            {
                if (first == false)
                    writer.WriteCommaAsync();

                writer.WriteStartObjectAsync();

                writer.WritePropertyNameAsync(nameof(ReduceTree.Name));
                writer.WriteStringAsync(tree.Name);
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(ReduceTree.DisplayName));
                writer.WriteStringAsync(tree.DisplayName);
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(ReduceTree.Depth));
                writer.WriteIntegerAsync(tree.Depth);
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(ReduceTree.PageCount));
                writer.WriteIntegerAsync(tree.PageCount);
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(ReduceTree.NumberOfEntries));
                writer.WriteIntegerAsync(tree.NumberOfEntries);
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(ReduceTree.Root));
                writer.WriteTreePagesRecursively(new[] { tree.Root });

                writer.WriteEndObjectAsync();

                first = false;
            }

            writer.WriteEndArrayAsync();

            writer.WriteEndObjectAsync();
        }

        public static void WriteTreePagesRecursively(this AsyncBlittableJsonTextWriter writer, IEnumerable<ReduceTreePage> pages)
        {
            var first = true;

            foreach (var page in pages)
            {
                if (first == false)
                    writer.WriteCommaAsync();

                writer.WriteStartObjectAsync();

                writer.WritePropertyNameAsync(nameof(TreePage.PageNumber));
                writer.WriteIntegerAsync(page.PageNumber);
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(ReduceTreePage.AggregationResult));
                if (page.AggregationResult != null)
                    writer.WriteObjectAsync(page.AggregationResult);
                else
                    writer.WriteNullAsync();
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(ReduceTreePage.Children));
                if (page.Children != null)
                {
                    writer.WriteStartArrayAsync();
                    WriteTreePagesRecursively(writer, page.Children);
                    writer.WriteEndArrayAsync();
                }
                else
                    writer.WriteNullAsync();
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(ReduceTreePage.Entries));
                if (page.Entries != null)
                {
                    writer.WriteStartArrayAsync();

                    var firstEntry = true;
                    foreach (var entry in page.Entries)
                    {
                        if (firstEntry == false)
                            writer.WriteCommaAsync();

                        writer.WriteStartObjectAsync();

                        writer.WritePropertyNameAsync(nameof(MapResultInLeaf.Data));
                        writer.WriteObjectAsync(entry.Data);
                        writer.WriteCommaAsync();

                        writer.WritePropertyNameAsync(nameof(MapResultInLeaf.Source));
                        writer.WriteStringAsync(entry.Source);

                        writer.WriteEndObjectAsync();

                        firstEntry = false;
                    }

                    writer.WriteEndArrayAsync();
                }
                else
                    writer.WriteNullAsync();

                writer.WriteEndObjectAsync();
                first = false;
            }
        }
    }
}
