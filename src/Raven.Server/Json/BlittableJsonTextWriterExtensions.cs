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
        public static async ValueTask WritePerformanceStatsAsync(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, IEnumerable<IndexPerformanceStats> stats)
        {
            await writer.WriteStartObjectAsync();
            await writer.WriteArrayAsync(context, "Results", stats, async (w, c, stat) =>
            {
                await w.WriteStartObjectAsync();

                await w.WritePropertyNameAsync(nameof(stat.Name));
                await w.WriteStringAsync(stat.Name);
                await w.WriteCommaAsync();

                await Sparrow.Json.BlittableJsonTextWriterExtensions.WriteArrayAsync(w, c, nameof(stat.Performance), stat.Performance, (wp, cp, performance) => wp.WriteIndexingPerformanceStats(context, performance));

                await w.WriteEndObjectAsync();
            });
            await writer.WriteEndObjectAsync();
        }

        public static async ValueTask WriteEtlTaskPerformanceStats(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, IEnumerable<EtlTaskPerformanceStats> stats)
        {
            await writer.WriteStartObjectAsync();
            await writer.WriteArrayAsync(context, "Results", stats, async (w, c, taskStats) =>
            {
                await w.WriteStartObjectAsync();

                await w.WritePropertyNameAsync(nameof(taskStats.TaskId));
                await w.WriteIntegerAsync(taskStats.TaskId);
                await w.WriteCommaAsync();

                await w.WritePropertyNameAsync(nameof(taskStats.TaskName));
                await w.WriteStringAsync(taskStats.TaskName);
                await w.WriteCommaAsync();

                await w.WritePropertyNameAsync(nameof(taskStats.EtlType));
                await w.WriteStringAsync(taskStats.EtlType.ToString());
                await w.WriteCommaAsync();

                await Sparrow.Json.BlittableJsonTextWriterExtensions.WriteArrayAsync(w, c, nameof(taskStats.Stats), taskStats.Stats, async (wp, cp, scriptStats) =>
                {
                    await wp.WriteStartObjectAsync();

                    await wp.WritePropertyNameAsync(nameof(scriptStats.TransformationName));
                    await wp.WriteStringAsync(scriptStats.TransformationName);
                    await wp.WriteCommaAsync();

                    await Sparrow.Json.BlittableJsonTextWriterExtensions.WriteArrayAsync(wp, cp, nameof(scriptStats.Performance), scriptStats.Performance, (wpp, cpp, perfStats) => wpp.WriteEtlPerformanceStats(cpp, perfStats));

                    await wp.WriteEndObjectAsync();
                });

                await w.WriteEndObjectAsync();
            });
            await writer.WriteEndObjectAsync();
        }

        public static async ValueTask WriteEtlTaskProgressAsync(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, IEnumerable<EtlTaskProgress> progress)
        {
            await writer.WriteStartObjectAsync();
            await writer.WriteArrayAsync(context, "Results", progress, async (w, c, taskStats) =>
            {
                await w.WriteStartObjectAsync();

                await w.WritePropertyNameAsync(nameof(taskStats.TaskName));
                await w.WriteStringAsync(taskStats.TaskName);
                await w.WriteCommaAsync();

                await w.WritePropertyNameAsync(nameof(taskStats.EtlType));
                await w.WriteStringAsync(taskStats.EtlType.ToString());
                await w.WriteCommaAsync();

                await Sparrow.Json.BlittableJsonTextWriterExtensions.WriteArrayAsync(w, c, nameof(taskStats.ProcessesProgress), taskStats.ProcessesProgress, async (wp, cp, processProgress) =>
                {
                    await wp.WriteStartObjectAsync();

                    await wp.WritePropertyNameAsync(nameof(processProgress.TransformationName));
                    await wp.WriteStringAsync(processProgress.TransformationName);
                    await wp.WriteCommaAsync();

                    await wp.WritePropertyNameAsync(nameof(processProgress.Completed));
                    await wp.WriteBoolAsync(processProgress.Completed);
                    await wp.WriteCommaAsync();

                    await wp.WritePropertyNameAsync(nameof(processProgress.Disabled));
                    await wp.WriteBoolAsync(processProgress.Disabled);
                    await wp.WriteCommaAsync();

                    await wp.WritePropertyNameAsync(nameof(processProgress.AverageProcessedPerSecond));
                    await wp.WriteDoubleAsync(processProgress.AverageProcessedPerSecond);
                    await wp.WriteCommaAsync();

                    await wp.WritePropertyNameAsync(nameof(processProgress.NumberOfDocumentsToProcess));
                    await wp.WriteIntegerAsync(processProgress.NumberOfDocumentsToProcess);
                    await wp.WriteCommaAsync();

                    await wp.WritePropertyNameAsync(nameof(processProgress.TotalNumberOfDocuments));
                    await wp.WriteIntegerAsync(processProgress.TotalNumberOfDocuments);
                    await wp.WriteCommaAsync();

                    await wp.WritePropertyNameAsync(nameof(processProgress.NumberOfDocumentTombstonesToProcess));
                    await wp.WriteIntegerAsync(processProgress.NumberOfDocumentTombstonesToProcess);
                    await wp.WriteCommaAsync();

                    await wp.WritePropertyNameAsync(nameof(processProgress.TotalNumberOfDocumentTombstones));
                    await wp.WriteIntegerAsync(processProgress.TotalNumberOfDocumentTombstones);
                    await wp.WriteCommaAsync();

                    await wp.WritePropertyNameAsync(nameof(processProgress.NumberOfCounterGroupsToProcess));
                    await wp.WriteIntegerAsync(processProgress.NumberOfCounterGroupsToProcess);
                    await wp.WriteCommaAsync();

                    await wp.WritePropertyNameAsync(nameof(processProgress.TotalNumberOfCounterGroups));
                    await wp.WriteIntegerAsync(processProgress.TotalNumberOfCounterGroups);

                    await wp.WriteEndObjectAsync();
                });

                await w.WriteEndObjectAsync();
            });
            await writer.WriteEndObjectAsync();
        }

        public static async ValueTask WriteExplanation(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, DynamicQueryToIndexMatcher.Explanation explanation)
        {
            await writer.WriteStartObjectAsync();

            await writer.WritePropertyNameAsync(nameof(explanation.Index));
            await writer.WriteStringAsync(explanation.Index);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(explanation.Reason));
            await writer.WriteStringAsync(explanation.Reason);

            await writer.WriteEndObjectAsync();
        }

        public static async ValueTask<long> WriteSuggestionQueryResult(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, SuggestionQueryResult result)
        {
            await writer.WriteStartObjectAsync();

            await writer.WritePropertyNameAsync(nameof(result.TotalResults));
            await writer.WriteIntegerAsync(result.TotalResults);
            await writer.WriteCommaAsync();

            if (result.CappedMaxResults != null)
            {
                await writer.WritePropertyNameAsync(nameof(result.CappedMaxResults));
                await writer.WriteIntegerAsync(result.CappedMaxResults.Value);
                await writer.WriteCommaAsync();
            }

            await writer.WritePropertyNameAsync(nameof(result.DurationInMs));
            await writer.WriteIntegerAsync(result.DurationInMs);
            await writer.WriteCommaAsync();

            var numberOfResults = await writer.WriteQueryResult(context, result, metadataOnly: false, partial: true);

            await writer.WriteEndObjectAsync();

            return numberOfResults;
        }

        public static async ValueTask<long> WriteFacetedQueryResult(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, FacetedQueryResult result)
        {
            await writer.WriteStartObjectAsync();

            await writer.WritePropertyNameAsync(nameof(result.TotalResults));
            await writer.WriteIntegerAsync(result.TotalResults);
            await writer.WriteCommaAsync();

            if (result.CappedMaxResults != null)
            {
                await writer.WritePropertyNameAsync(nameof(result.CappedMaxResults));
                await writer.WriteIntegerAsync(result.CappedMaxResults.Value);
                await writer.WriteCommaAsync();
            }

            await writer.WritePropertyNameAsync(nameof(result.DurationInMs));
            await writer.WriteIntegerAsync(result.DurationInMs);
            await writer.WriteCommaAsync();

            var numberOfResults = await writer.WriteQueryResult(context, result, metadataOnly: false, partial: true);

            await writer.WriteEndObjectAsync();

            return numberOfResults;
        }

        public static async ValueTask WriteSuggestionResult(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, SuggestionResult result)
        {
            await writer.WriteStartObjectAsync();

            await writer.WritePropertyNameAsync(nameof(result.Name));
            await writer.WriteStringAsync(result.Name);
            await writer.WriteCommaAsync();

            await writer.WriteArrayAsync(nameof(result.Suggestions), result.Suggestions);

            await writer.WriteEndObjectAsync();
        }

        public static async ValueTask WriteFacetResult(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, FacetResult result)
        {
            await writer.WriteStartObjectAsync();

            await writer.WritePropertyNameAsync(nameof(result.Name));
            await writer.WriteStringAsync(result.Name);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(result.Values));
            await writer.WriteStartArrayAsync();
            var isFirstInternal = true;
            foreach (var value in result.Values)
            {
                if (isFirstInternal == false)
                    await writer.WriteCommaAsync();

                isFirstInternal = false;

                await writer.WriteStartObjectAsync();

                await writer.WritePropertyNameAsync(nameof(value.Name));
                await writer.WriteStringAsync(value.Name);
                await writer.WriteCommaAsync();

                if (value.Average.HasValue)
                {
                    await writer.WritePropertyNameAsync(nameof(value.Average));

                    using (var lazyStringValue = context.GetLazyString(value.Average.ToInvariantString()))
                        await writer.WriteDoubleAsync(new LazyNumberValue(lazyStringValue));

                    await writer.WriteCommaAsync();
                }

                if (value.Max.HasValue)
                {
                    await writer.WritePropertyNameAsync(nameof(value.Max));

                    using (var lazyStringValue = context.GetLazyString(value.Max.ToInvariantString()))
                        await writer.WriteDoubleAsync(new LazyNumberValue(lazyStringValue));

                    await writer.WriteCommaAsync();
                }

                if (value.Min.HasValue)
                {
                    await writer.WritePropertyNameAsync(nameof(value.Min));

                    using (var lazyStringValue = context.GetLazyString(value.Min.ToInvariantString()))
                        await writer.WriteDoubleAsync(new LazyNumberValue(lazyStringValue));

                    await writer.WriteCommaAsync();
                }

                if (value.Sum.HasValue)
                {
                    await writer.WritePropertyNameAsync(nameof(value.Sum));

                    using (var lazyStringValue = context.GetLazyString(value.Sum.ToInvariantString()))
                        await writer.WriteDoubleAsync(new LazyNumberValue(lazyStringValue));

                    await writer.WriteCommaAsync();
                }

                await writer.WritePropertyNameAsync(nameof(value.Count));
                await writer.WriteIntegerAsync(value.Count);
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(value.Range));
                await writer.WriteStringAsync(value.Range);

                await writer.WriteEndObjectAsync();
            }
            await writer.WriteEndArrayAsync();
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(result.RemainingHits));
            await writer.WriteIntegerAsync(result.RemainingHits);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(result.RemainingTermsCount));
            await writer.WriteIntegerAsync(result.RemainingTermsCount);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(result.RemainingTerms));
            await writer.WriteStartArrayAsync();
            isFirstInternal = true;
            foreach (var term in result.RemainingTerms)
            {
                if (isFirstInternal == false)
                    await writer.WriteCommaAsync();

                isFirstInternal = false;

                await writer.WriteStringAsync(term);
            }
            await writer.WriteEndArrayAsync();

            await writer.WriteEndObjectAsync();
        }

        public static async ValueTask WriteIndexEntriesQueryResult(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, IndexEntriesQueryResult result)
        {
            await writer.WriteStartObjectAsync();

            await writer.WritePropertyNameAsync(nameof(result.TotalResults));
            await writer.WriteIntegerAsync(result.TotalResults);
            await writer.WriteCommaAsync();

            if (result.CappedMaxResults != null)
            {
                await writer.WritePropertyNameAsync(nameof(result.CappedMaxResults));
                await writer.WriteIntegerAsync(result.CappedMaxResults.Value);
                await writer.WriteCommaAsync();
            }

            await writer.WritePropertyNameAsync(nameof(result.SkippedResults));
            await writer.WriteIntegerAsync(result.SkippedResults);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(result.DurationInMs));
            await writer.WriteIntegerAsync(result.DurationInMs);
            await writer.WriteCommaAsync();

            await writer.WriteQueryResult(context, result, metadataOnly: false, partial: true);

            await writer.WriteEndObjectAsync();
        }

        public static async ValueTask<long> WriteDocumentQueryResultAsync(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, DocumentQueryResult result, bool metadataOnly, Func<AsyncBlittableJsonTextWriter, ValueTask> writeAdditionalData = null)
        {
            await writer.WriteStartObjectAsync();

            await writer.WritePropertyNameAsync(nameof(result.TotalResults));
            await writer.WriteIntegerAsync(result.TotalResults);
            await writer.WriteCommaAsync();

            if (result.CappedMaxResults != null)
            {
                await writer.WritePropertyNameAsync(nameof(result.CappedMaxResults));
                await writer.WriteIntegerAsync(result.CappedMaxResults.Value);
                await writer.WriteCommaAsync();
            }

            await writer.WritePropertyNameAsync(nameof(result.SkippedResults));
            await writer.WriteIntegerAsync(result.SkippedResults);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(result.DurationInMs));
            await writer.WriteIntegerAsync(result.DurationInMs);
            await writer.WriteCommaAsync();

            await writer.WriteArrayAsync(nameof(result.IncludedPaths), result.IncludedPaths);
            await writer.WriteCommaAsync();

            var numberOfResults = await writer.WriteQueryResultAsync(context, result, metadataOnly, partial: true);

            if (result.Highlightings != null)
            {
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(result.Highlightings));
                await writer.WriteStartObjectAsync();
                var first = true;
                foreach (var kvp in result.Highlightings)
                {
                    if (first == false)
                        await writer.WriteCommaAsync();
                    first = false;

                    await writer.WritePropertyNameAsync(kvp.Key);
                    await writer.WriteStartObjectAsync();
                    var firstInner = true;
                    foreach (var kvpInner in kvp.Value)
                    {
                        if (firstInner == false)
                            await writer.WriteCommaAsync();
                        firstInner = false;

                        await writer.WriteArrayAsync(kvpInner.Key, kvpInner.Value);
                    }

                    await writer.WriteEndObjectAsync();
                }

                await writer.WriteEndObjectAsync();
            }

            if (result.Explanations != null)
            {
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(result.Explanations));
                await writer.WriteStartObjectAsync();
                var first = true;
                foreach (var kvp in result.Explanations)
                {
                    if (first == false)
                        await writer.WriteCommaAsync();
                    first = false;

                    await writer.WriteArrayAsync(kvp.Key, kvp.Value);
                }

                await writer.WriteEndObjectAsync();
            }

            var counters = result.GetCounterIncludes();
            if (counters != null)
            {
                await writer.WriteCommaAsync();
                await writer.WritePropertyNameAsync(nameof(result.CounterIncludes));
                await writer.WriteCountersAsync(counters);

                await writer.WriteCommaAsync();
                await writer.WritePropertyNameAsync(nameof(result.IncludedCounterNames));
                await writer.WriteIncludedCounterNames(result.IncludedCounterNames);
            }

            var timeSeries = result.GetTimeSeriesIncludes();
            if (timeSeries != null)
            {
                await writer.WriteCommaAsync();
                await writer.WritePropertyNameAsync(nameof(result.TimeSeriesIncludes));
                await writer.WriteTimeSeriesAsync(timeSeries);
            }

            var compareExchangeValues = result.GetCompareExchangeValueIncludes();
            if (compareExchangeValues != null)
            {
                await writer.WriteCommaAsync();
                await writer.WritePropertyNameAsync(nameof(result.CompareExchangeValueIncludes));
                await writer.WriteCompareExchangeValues(compareExchangeValues);
            }

            writeAdditionalData?.Invoke(writer);

            await writer.WriteEndObjectAsync();
            return numberOfResults;
        }

        public static async ValueTask WriteIncludedCounterNames(this AbstractBlittableJsonTextWriter writer, Dictionary<string, string[]> includedCounterNames)
        {
            await writer.WriteStartObjectAsync();

            var first = true;
            foreach (var kvp in includedCounterNames)
            {
                if (first == false)
                    await writer.WriteCommaAsync();

                first = false;

                await writer.WriteArrayAsync(kvp.Key, kvp.Value);
            }

            await writer.WriteEndObjectAsync();
        }

        public static async ValueTask<long> WriteQueryResult<TResult, TInclude>(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, QueryResultBase<TResult, TInclude> result, bool metadataOnly, bool partial = false)
        {
            var numberOfResults = 0L;

            if (partial == false)
                await writer.WriteStartObjectAsync();

            await writer.WritePropertyNameAsync(nameof(result.IndexName));
            await writer.WriteStringAsync(result.IndexName);
            await writer.WriteCommaAsync();

            var results = (object)result.Results;
            if (results is List<Document> documents)
            {
                await writer.WritePropertyNameAsync(nameof(result.Results));
                numberOfResults = await writer.WriteDocuments(context, documents, metadataOnly);
                await writer.WriteCommaAsync();
            }
            else if (results is List<BlittableJsonReaderObject> objects)
            {
                await writer.WritePropertyNameAsync(nameof(result.Results));
                numberOfResults = await writer.WriteObjects(context, objects);
                await writer.WriteCommaAsync();
            }
            else if (results is List<FacetResult> facets)
            {
                numberOfResults = facets.Count;

                await writer.WriteArrayAsync(context, nameof(result.Results), facets, (w, c, facet) => w.WriteFacetResult(c, facet));
                await writer.WriteCommaAsync();
            }
            else if (results is List<SuggestionResult> suggestions)
            {
                numberOfResults = suggestions.Count;

                await writer.WriteArrayAsync(context, nameof(result.Results), suggestions, (w, c, suggestion) => w.WriteSuggestionResult(c, suggestion));
                await writer.WriteCommaAsync();
            }
            else
                throw new NotSupportedException($"Cannot write query result of '{typeof(TResult)}' type in '{result.GetType()}'.");

            var includes = (object)result.Includes;
            if (includes is List<Document> includeDocuments)
            {
                await writer.WritePropertyNameAsync(nameof(result.Includes));
                await writer.WriteIncludes(context, includeDocuments);
                await writer.WriteCommaAsync();
            }
            else if (includes is List<BlittableJsonReaderObject> includeObjects)
            {
                if (includeObjects.Count != 0)
                    throw new NotSupportedException("Cannot write query includes of List<BlittableJsonReaderObject>, but got non zero response");

                await writer.WritePropertyNameAsync(nameof(result.Includes));
                await writer.WriteStartObjectAsync();
                await writer.WriteEndObjectAsync();
                await writer.WriteCommaAsync();
            }
            else
                throw new NotSupportedException($"Cannot write query includes of '{typeof(TInclude)}' type in '{result.GetType()}'.");

            await writer.WritePropertyNameAsync(nameof(result.IndexTimestamp));
            await writer.WriteStringAsync(result.IndexTimestamp.ToString(DefaultFormat.DateTimeFormatsToWrite));
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(result.LastQueryTime));
            await writer.WriteStringAsync(result.LastQueryTime.ToString(DefaultFormat.DateTimeFormatsToWrite));
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(result.IsStale));
            await writer.WriteBoolAsync(result.IsStale);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(result.ResultEtag));
            await writer.WriteIntegerAsync(result.ResultEtag);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(result.NodeTag));
            await writer.WriteStringAsync(result.NodeTag);

            if (partial == false)
                await writer.WriteEndObjectAsync();

            return numberOfResults;
        }

        public static async ValueTask<long> WriteQueryResultAsync<TResult>(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, QueryResultServerSide<TResult> result, bool metadataOnly, bool partial = false)
        {
            var numberOfResults = 0L;

            if (partial == false)
                await writer.WriteStartObjectAsync();

            await writer.WritePropertyNameAsync(nameof(result.IndexName));
            await writer.WriteStringAsync(result.IndexName);
            await writer.WriteCommaAsync();

            var results = (object)result.Results;
            if (results is List<Document> documents)
            {
                await writer.WritePropertyNameAsync(nameof(result.Results));
                numberOfResults = await writer.WriteDocumentsAsync(context, documents, metadataOnly);
                await writer.WriteCommaAsync();
            }
            else if (results is List<BlittableJsonReaderObject> objects)
            {
                await writer.WritePropertyNameAsync(nameof(result.Results));
                numberOfResults = await writer.WriteObjectsAsync(context, objects);
                await writer.WriteCommaAsync();
            }
            else if (results is List<FacetResult> facets)
            {
                numberOfResults = facets.Count;

                await writer.WriteArrayAsync(context, nameof(result.Results), facets, (w, c, facet) => w.WriteFacetResult(c, facet));
                await writer.WriteCommaAsync();
                await writer.MaybeOuterFlushAsync();
            }
            else if (results is List<SuggestionResult> suggestions)
            {
                numberOfResults = suggestions.Count;

                await writer.WriteArrayAsync(context, nameof(result.Results), suggestions, (w, c, suggestion) => w.WriteSuggestionResult(c, suggestion));
                await writer.WriteCommaAsync();
                await writer.MaybeOuterFlushAsync();
            }
            else
                throw new NotSupportedException($"Cannot write query result of '{typeof(TResult)}' type in '{result.GetType()}'.");

            var includes = (object)result.Includes;
            if (includes is List<Document> includeDocuments)
            {
                await writer.WritePropertyNameAsync(nameof(result.Includes));
                await writer.WriteIncludesAsync(context, includeDocuments);
                await writer.WriteCommaAsync();
            }
            else if (includes is List<BlittableJsonReaderObject> includeObjects)
            {
                if (includeObjects.Count != 0)
                    throw new NotSupportedException("Cannot write query includes of List<BlittableJsonReaderObject>, but got non zero response");

                await writer.WritePropertyNameAsync(nameof(result.Includes));
                await writer.WriteStartObjectAsync();
                await writer.WriteEndObjectAsync();
                await writer.WriteCommaAsync();
            }
            else
                throw new NotSupportedException($"Cannot write query includes of '{includes.GetType()}' type in '{result.GetType()}'.");

            await writer.WritePropertyNameAsync(nameof(result.IndexTimestamp));
            await writer.WriteStringAsync(result.IndexTimestamp.ToString(DefaultFormat.DateTimeFormatsToWrite));
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(result.LastQueryTime));
            await writer.WriteStringAsync(result.LastQueryTime.ToString(DefaultFormat.DateTimeFormatsToWrite));
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(result.IsStale));
            await writer.WriteBoolAsync(result.IsStale);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(result.ResultEtag));
            await writer.WriteIntegerAsync(result.ResultEtag);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(result.NodeTag));
            await writer.WriteStringAsync(result.NodeTag);

            if (result.Timings != null)
            {
                await writer.WriteCommaAsync();
                await writer.WritePropertyNameAsync(nameof(result.Timings));
                await writer.WriteQueryTimings(context, result.Timings);
            }

            if (result.TimeSeriesFields != null)
            {
                await writer.WriteCommaAsync();
                await writer.WriteArrayAsync(nameof(result.TimeSeriesFields), result.TimeSeriesFields);
            }

            if (partial == false)
                await writer.WriteEndObjectAsync();

            return numberOfResults;
        }

        public static async ValueTask WriteQueryTimings(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, QueryTimings queryTimings)
        {
            await writer.WriteStartObjectAsync();

            await writer.WritePropertyNameAsync(nameof(QueryTimings.DurationInMs));
            await writer.WriteIntegerAsync(queryTimings.DurationInMs);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(QueryTimings.Timings));
            if (queryTimings.Timings != null)
            {
                await writer.WriteStartObjectAsync();
                var first = true;

                foreach (var kvp in queryTimings.Timings)
                {
                    if (first == false)
                        await writer.WriteCommaAsync();

                    first = false;

                    await writer.WritePropertyNameAsync(kvp.Key);
                    await writer.WriteQueryTimings(context, kvp.Value);
                }

                await writer.WriteEndObjectAsync();
            }
            else
                await writer.WriteNullAsync();

            await writer.WriteEndObjectAsync();
        }

        public static async ValueTask WriteTermsQueryResult(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, TermsQueryResultServerSide queryResult)
        {
            await writer.WriteStartObjectAsync();

            await writer.WritePropertyNameAsync(nameof(queryResult.IndexName));
            await writer.WriteStringAsync(queryResult.IndexName);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(queryResult.ResultEtag));
            await writer.WriteIntegerAsync(queryResult.ResultEtag);
            await writer.WriteCommaAsync();

            await writer.WriteArrayAsync(nameof(queryResult.Terms), queryResult.Terms);

            await writer.WriteEndObjectAsync();
        }

        public static async ValueTask WriteIndexingPerformanceStats(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, IndexingPerformanceStats stats)
        {
            var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(stats);
            await writer.WriteObjectAsync(context.ReadObject(djv, "index/performance"));
        }

        public static async ValueTask WriteEtlPerformanceStats(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, EtlPerformanceStats stats)
        {
            var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(stats);
            await writer.WriteObjectAsync(context.ReadObject(djv, "etl/performance"));
        }

        public static async ValueTask WriteIndexQuery(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, IIndexQuery query)
        {
            var indexQuery = query as IndexQueryServerSide;
            if (indexQuery != null)
            {
                await writer.WriteIndexQuery(context, indexQuery);
                return;
            }

            throw new NotSupportedException($"Not supported query type: {query.GetType()}");
        }

        private static async ValueTask WriteIndexQuery(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, IndexQueryServerSide query)
        {
            await writer.WriteStartObjectAsync();

            await writer.WritePropertyNameAsync(nameof(query.PageSize));
            await writer.WriteIntegerAsync(query.PageSize);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(query.Query));
            if (query.Query != null)
                await writer.WriteStringAsync(query.Query);
            else
                await writer.WriteNullAsync();
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(query.SkipDuplicateChecking));
            await writer.WriteBoolAsync(query.SkipDuplicateChecking);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(query.Start));
            await writer.WriteIntegerAsync(query.Start);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(query.WaitForNonStaleResults));
            await writer.WriteBoolAsync(query.WaitForNonStaleResults);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(query.WaitForNonStaleResultsTimeout));
            if (query.WaitForNonStaleResultsTimeout.HasValue)
                await writer.WriteStringAsync(query.WaitForNonStaleResultsTimeout.Value.ToString());
            else
                await writer.WriteNullAsync();

            await writer.WriteEndObjectAsync();
        }

        public static async ValueTask WriteDetailedDatabaseStatistics(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, DetailedDatabaseStatistics statistics)
        {
            await writer.WriteStartObjectAsync();

            await writer.WritePropertyNameAsync(nameof(statistics.CountOfIdentities));
            await writer.WriteIntegerAsync(statistics.CountOfIdentities);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(statistics.CountOfCompareExchange));
            await writer.WriteIntegerAsync(statistics.CountOfCompareExchange);
            await writer.WriteCommaAsync();

            await WriteDatabaseStatisticsInternal(writer, statistics);

            await writer.WriteEndObjectAsync();
        }

        public static async ValueTask WriteDatabaseStatistics(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, DatabaseStatistics statistics)
        {
            await writer.WriteStartObjectAsync();

            await WriteDatabaseStatisticsInternal(writer, statistics);

            await writer.WriteEndObjectAsync();
        }

        private static async ValueTask WriteDatabaseStatisticsInternal(AsyncBlittableJsonTextWriter writer, DatabaseStatistics statistics)
        {
            await writer.WritePropertyNameAsync(nameof(statistics.CountOfIndexes));
            await writer.WriteIntegerAsync(statistics.CountOfIndexes);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(statistics.CountOfDocuments));
            await writer.WriteIntegerAsync(statistics.CountOfDocuments);
            await writer.WriteCommaAsync();

            if (statistics.CountOfRevisionDocuments > 0)
            {
                await writer.WritePropertyNameAsync(nameof(statistics.CountOfRevisionDocuments));
                await writer.WriteIntegerAsync(statistics.CountOfRevisionDocuments);
                await writer.WriteCommaAsync();
            }

            await writer.WritePropertyNameAsync(nameof(statistics.CountOfTombstones));
            await writer.WriteIntegerAsync(statistics.CountOfTombstones);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(statistics.CountOfDocumentsConflicts));
            await writer.WriteIntegerAsync(statistics.CountOfDocumentsConflicts);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(statistics.CountOfConflicts));
            await writer.WriteIntegerAsync(statistics.CountOfConflicts);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(statistics.CountOfAttachments));
            await writer.WriteIntegerAsync(statistics.CountOfAttachments);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(statistics.CountOfCounterEntries));
            await writer.WriteIntegerAsync(statistics.CountOfCounterEntries);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(statistics.CountOfTimeSeriesSegments));
            await writer.WriteIntegerAsync(statistics.CountOfTimeSeriesSegments);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(statistics.CountOfUniqueAttachments));
            await writer.WriteIntegerAsync(statistics.CountOfUniqueAttachments);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(statistics.DatabaseChangeVector));
            await writer.WriteStringAsync(statistics.DatabaseChangeVector);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(statistics.DatabaseId));
            await writer.WriteStringAsync(statistics.DatabaseId);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(statistics.NumberOfTransactionMergerQueueOperations));
            await writer.WriteIntegerAsync(statistics.NumberOfTransactionMergerQueueOperations);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(statistics.Is64Bit));
            await writer.WriteBoolAsync(statistics.Is64Bit);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(statistics.Pager));
            await writer.WriteStringAsync(statistics.Pager);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(statistics.LastDocEtag));
            if (statistics.LastDocEtag.HasValue)
                await writer.WriteIntegerAsync(statistics.LastDocEtag.Value);
            else
                await writer.WriteNullAsync();
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(statistics.LastDatabaseEtag));
            if (statistics.LastDatabaseEtag.HasValue)
                await writer.WriteIntegerAsync(statistics.LastDatabaseEtag.Value);
            else
                await writer.WriteNullAsync();
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync((nameof(statistics.DatabaseChangeVector)));
            await writer.WriteStringAsync(statistics.DatabaseChangeVector);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(statistics.LastIndexingTime));
            if (statistics.LastIndexingTime.HasValue)
                await writer.WriteDateTimeAsync(statistics.LastIndexingTime.Value, isUtc: true);
            else
                await writer.WriteNullAsync();
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(statistics.SizeOnDisk));
            await writer.WriteStartObjectAsync();

            await writer.WritePropertyNameAsync(nameof(statistics.SizeOnDisk.HumaneSize));
            await writer.WriteStringAsync(statistics.SizeOnDisk.HumaneSize);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(statistics.SizeOnDisk.SizeInBytes));
            await writer.WriteIntegerAsync(statistics.SizeOnDisk.SizeInBytes);

            await writer.WriteEndObjectAsync();
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(statistics.TempBuffersSizeOnDisk));
            await writer.WriteStartObjectAsync();

            await writer.WritePropertyNameAsync(nameof(statistics.TempBuffersSizeOnDisk.HumaneSize));
            await writer.WriteStringAsync(statistics.TempBuffersSizeOnDisk.HumaneSize);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(statistics.TempBuffersSizeOnDisk.SizeInBytes));
            await writer.WriteIntegerAsync(statistics.TempBuffersSizeOnDisk.SizeInBytes);

            await writer.WriteEndObjectAsync();
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(statistics.Indexes));
            await writer.WriteStartArrayAsync();
            var isFirstInternal = true;
            foreach (var index in statistics.Indexes)
            {
                if (isFirstInternal == false)
                    await writer.WriteCommaAsync();

                isFirstInternal = false;

                await writer.WriteStartObjectAsync();

                await writer.WritePropertyNameAsync(nameof(index.IsStale));
                await writer.WriteBoolAsync(index.IsStale);
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(index.Name));
                await writer.WriteStringAsync(index.Name);
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(index.LockMode));
                await writer.WriteStringAsync(index.LockMode.ToString());
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(index.Priority));
                await writer.WriteStringAsync(index.Priority.ToString());
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(index.State));
                await writer.WriteStringAsync(index.State.ToString());
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(index.Type));
                await writer.WriteStringAsync(index.Type.ToString());
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(index.SourceType));
                await writer.WriteStringAsync(index.SourceType.ToString());
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(index.LastIndexingTime));
                if (index.LastIndexingTime.HasValue)
                    await writer.WriteDateTimeAsync(index.LastIndexingTime.Value, isUtc: true);
                else
                    await writer.WriteNullAsync();

                await writer.WriteEndObjectAsync();
            }

            await writer.WriteEndArrayAsync();
        }

        public static async ValueTask WriteIndexDefinition(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, IndexDefinition indexDefinition, bool removeAnalyzers = false)
        {
            await writer.WriteStartObjectAsync();

            await writer.WritePropertyNameAsync(nameof(indexDefinition.Name));
            await writer.WriteStringAsync(indexDefinition.Name);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(indexDefinition.SourceType));
            await writer.WriteStringAsync(indexDefinition.SourceType.ToString());
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(indexDefinition.Type));
            await writer.WriteStringAsync(indexDefinition.Type.ToString());
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(indexDefinition.LockMode));
            if (indexDefinition.LockMode.HasValue)
                await writer.WriteStringAsync(indexDefinition.LockMode.ToString());
            else
                await writer.WriteNullAsync();
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(indexDefinition.Priority));
            if (indexDefinition.Priority.HasValue)
                await writer.WriteStringAsync(indexDefinition.Priority.ToString());
            else
                await writer.WriteNullAsync();
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(indexDefinition.OutputReduceToCollection));
            await writer.WriteStringAsync(indexDefinition.OutputReduceToCollection);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(indexDefinition.ReduceOutputIndex));

            if (indexDefinition.ReduceOutputIndex.HasValue)
                await writer.WriteIntegerAsync(indexDefinition.ReduceOutputIndex.Value);
            else
                await writer.WriteNullAsync();
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(indexDefinition.PatternForOutputReduceToCollectionReferences));
            await writer.WriteStringAsync(indexDefinition.PatternForOutputReduceToCollectionReferences);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(indexDefinition.PatternReferencesCollectionName));
            await writer.WriteStringAsync(indexDefinition.PatternReferencesCollectionName);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(indexDefinition.Configuration));
            await writer.WriteStartObjectAsync();
            var isFirstInternal = true;
            foreach (var kvp in indexDefinition.Configuration)
            {
                if (isFirstInternal == false)
                    await writer.WriteCommaAsync();

                isFirstInternal = false;

                await writer.WritePropertyNameAsync(kvp.Key);
                await writer.WriteStringAsync(kvp.Value);
            }
            await writer.WriteEndObjectAsync();
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(indexDefinition.AdditionalSources));
            await writer.WriteStartObjectAsync();
            isFirstInternal = true;
            foreach (var kvp in indexDefinition.AdditionalSources)
            {
                if (isFirstInternal == false)
                    await writer.WriteCommaAsync();

                isFirstInternal = false;

                await writer.WritePropertyNameAsync(kvp.Key);
                await writer.WriteStringAsync(kvp.Value);
            }
            await writer.WriteEndObjectAsync();
            await writer.WriteCommaAsync();

#if FEATURE_TEST_INDEX
            await writer.WritePropertyName(nameof(indexDefinition.IsTestIndex));
            await writer.WriteBool(indexDefinition.IsTestIndex);
            await writer.WriteComma();
#endif

            await writer.WritePropertyNameAsync(nameof(indexDefinition.Reduce));
            if (string.IsNullOrWhiteSpace(indexDefinition.Reduce) == false)
                await writer.WriteStringAsync(indexDefinition.Reduce);
            else
                await writer.WriteNullAsync();
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(indexDefinition.Maps));
            await writer.WriteStartArrayAsync();
            isFirstInternal = true;
            foreach (var map in indexDefinition.Maps)
            {
                if (isFirstInternal == false)
                    await writer.WriteCommaAsync();

                isFirstInternal = false;
                await writer.WriteStringAsync(map);
            }
            await writer.WriteEndArrayAsync();
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(indexDefinition.Fields));
            await writer.WriteStartObjectAsync();
            isFirstInternal = true;
            foreach (var kvp in indexDefinition.Fields)
            {
                if (isFirstInternal == false)
                    await writer.WriteCommaAsync();

                isFirstInternal = false;
                await writer.WritePropertyNameAsync(kvp.Key);
                if (kvp.Value != null)
                    await writer.WriteIndexFieldOptions(context, kvp.Value, removeAnalyzers);
                else
                    await writer.WriteNullAsync();
            }
            await writer.WriteEndObjectAsync();

            await writer.WriteEndObjectAsync();
        }

        public static async ValueTask WriteIndexProgress(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, IndexProgress progress)
        {
            await writer.WriteStartObjectAsync();

            await writer.WritePropertyNameAsync(nameof(progress.IsStale));
            await writer.WriteBoolAsync(progress.IsStale);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(progress.IndexRunningStatus));
            await writer.WriteStringAsync(progress.IndexRunningStatus.ToString());
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(progress.ProcessedPerSecond));
            await writer.WriteDoubleAsync(progress.ProcessedPerSecond);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(progress.Collections));
            if (progress.Collections == null)
            {
                await writer.WriteNullAsync();
            }
            else
            {
                await writer.WriteStartObjectAsync();
                var isFirst = true;
                foreach (var kvp in progress.Collections)
                {
                    if (isFirst == false)
                        await writer.WriteCommaAsync();

                    isFirst = false;

                    await writer.WritePropertyNameAsync(kvp.Key);

                    await writer.WriteStartObjectAsync();

                    await writer.WritePropertyNameAsync(nameof(kvp.Value.LastProcessedDocumentEtag));
                    await writer.WriteIntegerAsync(kvp.Value.LastProcessedDocumentEtag);
                    await writer.WriteCommaAsync();

                    await writer.WritePropertyNameAsync(nameof(kvp.Value.LastProcessedTombstoneEtag));
                    await writer.WriteIntegerAsync(kvp.Value.LastProcessedTombstoneEtag);
                    await writer.WriteCommaAsync();

                    await writer.WritePropertyNameAsync(nameof(kvp.Value.NumberOfDocumentsToProcess));
                    await writer.WriteIntegerAsync(kvp.Value.NumberOfDocumentsToProcess);
                    await writer.WriteCommaAsync();

                    await writer.WritePropertyNameAsync(nameof(kvp.Value.NumberOfTombstonesToProcess));
                    await writer.WriteIntegerAsync(kvp.Value.NumberOfTombstonesToProcess);
                    await writer.WriteCommaAsync();

                    await writer.WritePropertyNameAsync(nameof(kvp.Value.TotalNumberOfDocuments));
                    await writer.WriteIntegerAsync(kvp.Value.TotalNumberOfDocuments);
                    await writer.WriteCommaAsync();

                    await writer.WritePropertyNameAsync(nameof(kvp.Value.TotalNumberOfTombstones));
                    await writer.WriteIntegerAsync(kvp.Value.TotalNumberOfTombstones);

                    await writer.WriteEndObjectAsync();
                }
                await writer.WriteEndObjectAsync();
            }

            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(progress.Name));
            await writer.WriteStringAsync(progress.Name);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(progress.Type));
            await writer.WriteStringAsync(progress.Type.ToString());
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(progress.SourceType));
            await writer.WriteStringAsync(progress.SourceType.ToString());

            await writer.WriteEndObjectAsync();
        }

        public static async ValueTask WriteIndexStats(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, IndexStats stats)
        {
            var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(stats);
            await writer.WriteObjectAsync(context.ReadObject(djv, "index/stats"));
        }

        private static async ValueTask WriteIndexFieldOptions(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, IndexFieldOptions options, bool removeAnalyzers)
        {
            await writer.WriteStartObjectAsync();

            await writer.WritePropertyNameAsync(nameof(options.Analyzer));
            if (string.IsNullOrWhiteSpace(options.Analyzer) == false && !removeAnalyzers)
                await writer.WriteStringAsync(options.Analyzer);
            else
                await writer.WriteNullAsync();
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(options.Indexing));
            if (options.Indexing.HasValue)
                await writer.WriteStringAsync(options.Indexing.ToString());
            else
                await writer.WriteNullAsync();
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(options.Storage));
            if (options.Storage.HasValue)
                await writer.WriteStringAsync(options.Storage.ToString());
            else
                await writer.WriteNullAsync();
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(options.Suggestions));
            if (options.Suggestions.HasValue)
                await writer.WriteBoolAsync(options.Suggestions.Value);
            else
                await writer.WriteNullAsync();
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(options.TermVector));
            if (options.TermVector.HasValue)
                await writer.WriteStringAsync(options.TermVector.ToString());
            else
                await writer.WriteNullAsync();
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(options.Spatial));
            if (options.Spatial != null)
            {
                await writer.WriteStartObjectAsync();

                await writer.WritePropertyNameAsync(nameof(options.Spatial.Type));
                await writer.WriteStringAsync(options.Spatial.Type.ToString());
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(options.Spatial.MaxTreeLevel));
                await writer.WriteIntegerAsync(options.Spatial.MaxTreeLevel);
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(options.Spatial.MaxX));
                LazyStringValue lazyStringValue;
                using (lazyStringValue = context.GetLazyString(CharExtensions.ToInvariantString(options.Spatial.MaxX)))
                    await writer.WriteDoubleAsync(new LazyNumberValue(lazyStringValue));
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(options.Spatial.MaxY));
                using (lazyStringValue = context.GetLazyString(CharExtensions.ToInvariantString(options.Spatial.MaxY)))
                    await writer.WriteDoubleAsync(new LazyNumberValue(lazyStringValue));
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(options.Spatial.MinX));
                using (lazyStringValue = context.GetLazyString(CharExtensions.ToInvariantString(options.Spatial.MinX)))
                    await writer.WriteDoubleAsync(new LazyNumberValue(lazyStringValue));
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(options.Spatial.MinY));
                using (lazyStringValue = context.GetLazyString(CharExtensions.ToInvariantString(options.Spatial.MinY)))
                    await writer.WriteDoubleAsync(new LazyNumberValue(lazyStringValue));
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(options.Spatial.Strategy));
                await writer.WriteStringAsync(options.Spatial.Strategy.ToString());
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(options.Spatial.Units));
                await writer.WriteStringAsync(options.Spatial.Units.ToString());

                await writer.WriteEndObjectAsync();
            }
            else
                await writer.WriteNullAsync();

            await writer.WriteEndObjectAsync();
        }

        public static ValueTask<long> WriteDocuments(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, IEnumerable<Document> documents, bool metadataOnly)
        {
            return WriteDocuments(writer, context, documents.GetEnumerator(), metadataOnly);
        }

        public static async ValueTask<long> WriteDocuments(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, IEnumerator<Document> documents, bool metadataOnly)
        {
            var numberOfResults = 0L;

            await writer.WriteStartArrayAsync();

            var first = true;

            while (documents.MoveNext())
            {
                numberOfResults++;

                if (first == false)
                    await writer.WriteCommaAsync();
                first = false;

                await WriteDocument(writer, context, documents.Current, metadataOnly);
            }

            await writer.WriteEndArrayAsync();

            return numberOfResults;
        }

        public static async ValueTask<long> WriteDocumentsAsync(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, IEnumerable<Document> documents, bool metadataOnly)
        {
            var numberOfResults = 0L;

            await writer.WriteStartArrayAsync();

            var first = true;
            foreach (var document in documents)
            {
                numberOfResults++;

                if (first == false)
                    await writer.WriteCommaAsync();
                first = false;

                await WriteDocument(writer, context, document, metadataOnly);
                await writer.MaybeOuterFlushAsync();
            }

            await writer.WriteEndArrayAsync();
            return numberOfResults;
        }

        public static async ValueTask WriteDocument(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, Document document, bool metadataOnly, Func<LazyStringValue, bool> filterMetadataProperty = null)
        {
            if (document == null)
            {
                await writer.WriteNullAsync();
                return;
            }

            if (document == Document.ExplicitNull)
            {
                await writer.WriteNullAsync();
                return;
            }

            // Explicitly not disposing it, a single document can be
            // used multiple times in a single query, for example, due to projections
            // so we will let the context handle it, rather than handle it directly ourselves
            //using (document.Data)
            {
                if (metadataOnly == false)
                    await writer.WriteDocumentInternal(context, document, filterMetadataProperty);
                else
                    await writer.WriteDocumentMetadata(context, document, filterMetadataProperty);
            }
        }

        public static async ValueTask WriteIncludes(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, List<Document> includes)
        {
            await writer.WriteStartObjectAsync();

            var first = true;
            foreach (var document in includes)
            {
                if (first == false)
                    await writer.WriteCommaAsync();
                first = false;

                if (document is IncludeDocumentsCommand.ConflictDocument conflict)
                {
                    await writer.WritePropertyNameAsync(conflict.Id);
                    await WriteConflict(writer, conflict);
                    continue;
                }

                await writer.WritePropertyNameAsync(document.Id);
                await WriteDocument(writer, context, metadataOnly: false, document: document);
            }

            await writer.WriteEndObjectAsync();
        }

        public static async ValueTask WriteIncludesAsync(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, List<Document> includes)
        {
            await writer.WriteStartObjectAsync();

            var first = true;
            foreach (var document in includes)
            {
                if (first == false)
                    await writer.WriteCommaAsync();
                first = false;

                if (document is IncludeDocumentsCommand.ConflictDocument conflict)
                {
                    await writer.WritePropertyNameAsync(conflict.Id);
                    await WriteConflict(writer, conflict);
                    await writer.MaybeOuterFlushAsync();
                    continue;
                }

                await writer.WritePropertyNameAsync(document.Id);
                await WriteDocument(writer, context, metadataOnly: false, document: document);
                await writer.MaybeOuterFlushAsync();
            }

            await writer.WriteEndObjectAsync();
        }

        private static async ValueTask WriteConflict(AbstractBlittableJsonTextWriter writer, IncludeDocumentsCommand.ConflictDocument conflict)
        {
            await writer.WriteStartObjectAsync();

            await writer.WritePropertyNameAsync(Constants.Documents.Metadata.Key);
            await writer.WriteStartObjectAsync();

            await writer.WritePropertyNameAsync(Constants.Documents.Metadata.Id);
            await writer.WriteStringAsync(conflict.Id);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(Constants.Documents.Metadata.ChangeVector);
            await writer.WriteStringAsync(string.Empty);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(Constants.Documents.Metadata.Conflict);
            await writer.WriteBoolAsync(true);

            await writer.WriteEndObjectAsync();

            await writer.WriteEndObjectAsync();
        }

        public static async ValueTask<long> WriteObjects(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, IEnumerable<BlittableJsonReaderObject> objects)
        {
            var numberOfResults = 0L;

            await writer.WriteStartArrayAsync();

            var first = true;
            foreach (var o in objects)
            {
                numberOfResults++;

                if (first == false)
                    await writer.WriteCommaAsync();
                first = false;

                if (o == null)
                {
                    await writer.WriteNullAsync();
                    continue;
                }

                using (o)
                {
                    await writer.WriteObjectAsync(o);
                }
            }

            await writer.WriteEndArrayAsync();

            return numberOfResults;
        }

        public static async ValueTask<long> WriteObjectsAsync(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, IEnumerable<BlittableJsonReaderObject> objects)
        {
            var numberOfResults = 0L;

            await writer.WriteStartArrayAsync();

            var first = true;
            foreach (var o in objects)
            {
                numberOfResults++;

                if (first == false)
                    await writer.WriteCommaAsync();
                first = false;

                if (o == null)
                {
                    await writer.WriteNullAsync();
                    continue;
                }

                using (o)
                {
                    await writer.WriteObjectAsync(o);
                }

                await writer.MaybeOuterFlushAsync();
            }

            await writer.WriteEndArrayAsync();
            return numberOfResults;
        }

        public static async ValueTask WriteCounters(this AsyncBlittableJsonTextWriter writer, Dictionary<string, List<CounterDetail>> counters)
        {
            await writer.WriteStartObjectAsync();

            var first = true;
            foreach (var kvp in counters)
            {
                if (first == false)
                    await writer.WriteCommaAsync();

                first = false;

                await writer.WritePropertyNameAsync(kvp.Key);

                await writer.WriteCountersForDocument(kvp.Value);
            }

            await writer.WriteEndObjectAsync();
        }

        public static async ValueTask WriteCountersAsync(this AsyncBlittableJsonTextWriter writer, Dictionary<string, List<CounterDetail>> counters)
        {
            await writer.WriteStartObjectAsync();

            var first = true;
            foreach (var kvp in counters)
            {
                if (first == false)
                    await writer.WriteCommaAsync();

                first = false;

                await writer.WritePropertyNameAsync(kvp.Key);

                await writer.WriteCountersForDocumentAsync(kvp.Value);
            }

            await writer.WriteEndObjectAsync();
        }

        private static async ValueTask WriteCountersForDocument(this AsyncBlittableJsonTextWriter writer, List<CounterDetail> counters)
        {
            await writer.WriteStartArrayAsync();

            var first = true;
            foreach (var counter in counters)
            {
                if (first == false)
                    await writer.WriteCommaAsync();
                first = false;

                await writer.WriteStartObjectAsync();

                await writer.WritePropertyNameAsync(nameof(CounterDetail.DocumentId));
                await writer.WriteStringAsync(counter.DocumentId);
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(CounterDetail.CounterName));
                await writer.WriteStringAsync(counter.CounterName);
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(CounterDetail.TotalValue));
                await writer.WriteIntegerAsync(counter.TotalValue);

                await writer.WriteEndObjectAsync();
            }

            await writer.WriteEndArrayAsync();
        }

        private static async ValueTask WriteCountersForDocumentAsync(this AsyncBlittableJsonTextWriter writer, List<CounterDetail> counters)
        {
            await writer.WriteStartArrayAsync();

            var first = true;
            foreach (var counter in counters)
            {
                if (first == false)
                    await writer.WriteCommaAsync();
                first = false;

                if (counter == null)
                {
                    await writer.WriteNullAsync();
                    await writer.MaybeOuterFlushAsync();
                    continue;
                }

                await writer.WriteStartObjectAsync();

                await writer.WritePropertyNameAsync(nameof(CounterDetail.DocumentId));
                await writer.WriteStringAsync(counter.DocumentId);
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(CounterDetail.CounterName));
                await writer.WriteStringAsync(counter.CounterName);
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(CounterDetail.TotalValue));
                await writer.WriteIntegerAsync(counter.TotalValue);

                await writer.WriteEndObjectAsync();

                await writer.MaybeOuterFlushAsync();
            }

            await writer.WriteEndArrayAsync();
        }

        public static async ValueTask WriteCompareExchangeValues(this AsyncBlittableJsonTextWriter writer, Dictionary<string, CompareExchangeValue<BlittableJsonReaderObject>> compareExchangeValues)
        {
            await writer.WriteStartObjectAsync();

            var first = true;
            foreach (var kvp in compareExchangeValues)
            {
                if (first == false)
                    await writer.WriteCommaAsync();

                first = false;

                await writer.WritePropertyNameAsync(kvp.Key);

                await writer.WriteStartObjectAsync();

                await writer.WritePropertyNameAsync(nameof(kvp.Value.Key));
                await writer.WriteStringAsync(kvp.Key);
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(kvp.Value.Index));
                await writer.WriteIntegerAsync(kvp.Value.Index);
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(kvp.Value));
                await writer.WriteObjectAsync(kvp.Value.Value);

                await writer.WriteEndObjectAsync();

                await writer.MaybeOuterFlushAsync();
            }

            await writer.WriteEndObjectAsync();
        }

        public static async ValueTask WriteTimeSeriesAsync(this AsyncBlittableJsonTextWriter writer,
            Dictionary<string, Dictionary<string, List<TimeSeriesRangeResult>>> timeSeries)
        {
            await writer.WriteStartObjectAsync();

            var first = true;
            foreach (var kvp in timeSeries)
            {
                if (first == false)
                    await writer.WriteCommaAsync();

                first = false;

                await writer.WritePropertyNameAsync(kvp.Key);

                await TimeSeriesHandler.WriteTimeSeriesRangeResults(context: null, writer, documentId: null, kvp.Value);
            }

            await writer.WriteEndObjectAsync();
        }

        public static async ValueTask WriteDocumentMetadata(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context,
            Document document, Func<LazyStringValue, bool> filterMetadataProperty = null)
        {
            await writer.WriteStartObjectAsync();
            document.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata);
            await WriteMetadata(writer, document, metadata, filterMetadataProperty);

            await writer.WriteEndObjectAsync();
        }

        public static async ValueTask WriteMetadata(this AbstractBlittableJsonTextWriter writer, Document document, BlittableJsonReaderObject metadata, Func<LazyStringValue, bool> filterMetadataProperty = null)
        {
            await writer.WritePropertyNameAsync(Constants.Documents.Metadata.Key);
            await writer.WriteStartObjectAsync();
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
                        await writer.WriteCommaAsync();
                    }
                    first = false;
                    await writer.WritePropertyNameAsync(prop.Name);
                    await writer.WriteValueAsync(prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value);
                }
            }

            if (first == false)
            {
                await writer.WriteCommaAsync();
            }
            await writer.WritePropertyNameAsync(Constants.Documents.Metadata.ChangeVector);
            await writer.WriteStringAsync(document.ChangeVector);

            if (document.Flags != DocumentFlags.None)
            {
                await writer.WriteCommaAsync();
                await writer.WritePropertyNameAsync(Constants.Documents.Metadata.Flags);
                await writer.WriteStringAsync(document.Flags.ToString());
            }
            if (document.Id != null)
            {
                await writer.WriteCommaAsync();
                await writer.WritePropertyNameAsync(Constants.Documents.Metadata.Id);
                await writer.WriteStringAsync(document.Id);
            }
            if (document.IndexScore != null)
            {
                await writer.WriteCommaAsync();
                await writer.WritePropertyNameAsync(Constants.Documents.Metadata.IndexScore);
                await writer.WriteDoubleAsync(document.IndexScore.Value);
            }
            if (document.Distance != null)
            {
                await writer.WriteCommaAsync();
                var result = document.Distance.Value;
                await writer.WritePropertyNameAsync(Constants.Documents.Metadata.SpatialResult);
                await writer.WriteStartObjectAsync();
                await writer.WritePropertyNameAsync(nameof(result.Distance));
                await writer.WriteDoubleAsync(result.Distance);
                await writer.WriteCommaAsync();
                await writer.WritePropertyNameAsync(nameof(result.Latitude));
                await writer.WriteDoubleAsync(result.Latitude);
                await writer.WriteCommaAsync();
                await writer.WritePropertyNameAsync(nameof(result.Longitude));
                await writer.WriteDoubleAsync(result.Longitude);
                await writer.WriteEndObjectAsync();
            }
            if (document.LastModified != DateTime.MinValue)
            {
                await writer.WriteCommaAsync();
                await writer.WritePropertyNameAsync(Constants.Documents.Metadata.LastModified);
                await writer.WriteDateTimeAsync(document.LastModified, isUtc: true);
            }
            await writer.WriteEndObjectAsync();
        }

        private static readonly StringSegment MetadataKeySegment = new StringSegment(Constants.Documents.Metadata.Key);

        private static async ValueTask WriteDocumentInternal(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, Document document, Func<LazyStringValue, bool> filterMetadataProperty = null)
        {
            await writer.WriteStartObjectAsync();
            WriteDocumentProperties(writer, context, document, filterMetadataProperty);
            await writer.WriteEndObjectAsync();
        }

        private static async ValueTask WriteDocumentProperties(this AbstractBlittableJsonTextWriter writer, JsonOperationContext context, Document document, Func<LazyStringValue, bool> filterMetadataProperty = null)
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
                        await writer.WriteCommaAsync();
                    }
                    first = false;
                    await writer.WritePropertyNameAsync(prop.Name);
                    await writer.WriteValueAsync(prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value);
                }
            }

            if (first == false)
                await writer.WriteCommaAsync();
            await WriteMetadata(writer, document, metadata, filterMetadataProperty);
        }

        public static async ValueTask WriteDocumentPropertiesWithoutMetadata(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, Document document)
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
                        await writer.WriteCommaAsync();
                    }
                    first = false;
                    await writer.WritePropertyNameAsync(prop.Name);
                    await writer.WriteValueAsync(prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value);
                }
            }
        }

        public static async ValueTask WriteOperationIdAndNodeTag(this AsyncBlittableJsonTextWriter writer, JsonOperationContext context, long operationId, string nodeTag)
        {
            await writer.WriteStartObjectAsync();

            await writer.WritePropertyNameAsync(nameof(OperationIdResult.OperationId));
            await writer.WriteIntegerAsync(operationId);

            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(OperationIdResult.OperationNodeTag));
            await writer.WriteStringAsync(nodeTag);

            await writer.WriteEndObjectAsync();
        }

        public static async ValueTask WriteArrayOfResultsAndCount(this AsyncBlittableJsonTextWriter writer, IEnumerable<string> results)
        {
            await writer.WriteStartObjectAsync();
            await writer.WritePropertyNameAsync("Results");
            await writer.WriteStartArrayAsync();

            var first = true;
            var count = 0;

            foreach (var id in results)
            {
                if (first == false)
                    await writer.WriteCommaAsync();

                await writer.WriteStringAsync(id);
                count++;

                first = false;
            }

            await writer.WriteEndArrayAsync();
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync("Count");
            await writer.WriteIntegerAsync(count);

            await writer.WriteEndObjectAsync();
        }

        public static async ValueTask WriteReduceTrees(this AsyncBlittableJsonTextWriter writer, IEnumerable<ReduceTree> trees)
        {
            await writer.WriteStartObjectAsync();
            await writer.WritePropertyNameAsync("Results");

            await writer.WriteStartArrayAsync();

            var first = true;

            foreach (var tree in trees)
            {
                if (first == false)
                    await writer.WriteCommaAsync();

                await writer.WriteStartObjectAsync();

                await writer.WritePropertyNameAsync(nameof(ReduceTree.Name));
                await writer.WriteStringAsync(tree.Name);
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(ReduceTree.DisplayName));
                await writer.WriteStringAsync(tree.DisplayName);
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(ReduceTree.Depth));
                await writer.WriteIntegerAsync(tree.Depth);
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(ReduceTree.PageCount));
                await writer.WriteIntegerAsync(tree.PageCount);
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(ReduceTree.NumberOfEntries));
                await writer.WriteIntegerAsync(tree.NumberOfEntries);
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(ReduceTree.Root));
                await writer.WriteTreePagesRecursively(new[] { tree.Root });

                await writer.WriteEndObjectAsync();

                first = false;
            }

            await writer.WriteEndArrayAsync();

            await writer.WriteEndObjectAsync();
        }

        public static async ValueTask WriteTreePagesRecursively(this AsyncBlittableJsonTextWriter writer, IEnumerable<ReduceTreePage> pages)
        {
            var first = true;

            foreach (var page in pages)
            {
                if (first == false)
                    await writer.WriteCommaAsync();

                await writer.WriteStartObjectAsync();

                await writer.WritePropertyNameAsync(nameof(TreePage.PageNumber));
                await writer.WriteIntegerAsync(page.PageNumber);
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(ReduceTreePage.AggregationResult));
                if (page.AggregationResult != null)
                    await writer.WriteObjectAsync(page.AggregationResult);
                else
                    await writer.WriteNullAsync();
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(ReduceTreePage.Children));
                if (page.Children != null)
                {
                    await writer.WriteStartArrayAsync();
                    WriteTreePagesRecursively(writer, page.Children);
                    await writer.WriteEndArrayAsync();
                }
                else
                    await writer.WriteNullAsync();
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(ReduceTreePage.Entries));
                if (page.Entries != null)
                {
                    await writer.WriteStartArrayAsync();

                    var firstEntry = true;
                    foreach (var entry in page.Entries)
                    {
                        if (firstEntry == false)
                            await writer.WriteCommaAsync();

                        await writer.WriteStartObjectAsync();

                        await writer.WritePropertyNameAsync(nameof(MapResultInLeaf.Data));
                        await writer.WriteObjectAsync(entry.Data);
                        await writer.WriteCommaAsync();

                        await writer.WritePropertyNameAsync(nameof(MapResultInLeaf.Source));
                        await writer.WriteStringAsync(entry.Source);

                        await writer.WriteEndObjectAsync();

                        firstEntry = false;
                    }

                    await writer.WriteEndArrayAsync();
                }
                else
                    await writer.WriteNullAsync();

                await writer.WriteEndObjectAsync();
                first = false;
            }
        }
    }
}
