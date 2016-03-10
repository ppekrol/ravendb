using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Document.Async;
using Raven.Client.Document.Batches;
using Raven.Client.Listeners;
using Raven.Client.Spatial;
using Raven.Json.Linq;

namespace Raven.Client.Document
{
    /// <summary>
    /// A query against a Raven index
    /// </summary>
    public class AsyncDocumentQuery<T> : AbstractDocumentQuery<T, AsyncDocumentQuery<T>>, IAsyncDocumentQuery<T>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncDocumentQuery{T}"/> class.
        /// </summary>
        public AsyncDocumentQuery(InMemoryDocumentSessionOperations session, IDatabaseCommands databaseCommands,
            IAsyncDatabaseCommands asyncDatabaseCommands, string indexName, string[] fieldsToFetch, string[] projectionFields, IDocumentQueryListener[] queryListeners, bool  isMapReduce)
            : base(session, databaseCommands, asyncDatabaseCommands, indexName, fieldsToFetch, projectionFields, queryListeners, isMapReduce)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AbstractDocumentQuery{T,TSelf}"/> class.
        /// </summary>
        public AsyncDocumentQuery(AsyncDocumentQuery<T> other)
            : base(other)
        {
        }

        /// <summary>
        /// Includes the specified path in the query, loading the document specified in that path
        /// </summary>
        /// <param name="path">The path.</param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Include(string path)
        {
            Include(path);
            return this;
        }

        /// <summary>
        /// Includes the specified path in the query, loading the document specified in that path
        /// </summary>
        /// <param name="path">The path.</param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Include(Expression<Func<T, object>> path)
        {
            Include(path);
            return this;
        }

        /// <summary>
        /// Negate the next operation
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Not
        {
            get
            {
                NegateNext();
                return this;
            }
        }

        /// <summary>
        /// Takes the specified count.
        /// </summary>
        /// <param name="count">The count.</param>
        /// <returns></returns>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Take(int count)
        {
            Take(count);
            return this;
        }

        /// <summary>
        /// Skips the specified count.
        /// </summary>
        /// <param name="count">The count.</param>
        /// <returns></returns>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Skip(int count)
        {
            Skip(count);
            return this;
        }

        /// <summary>
        /// Filter the results from the index using the specified where clause.
        /// </summary>
        /// <param name="whereClause">The where clause.</param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Where(string whereClause)
        {
            Where(whereClause);
            return this;
        }

        /// <summary>
        /// 	Matches exact value
        /// </summary>
        /// <remarks>
        /// 	Defaults to NotAnalyzed
        /// </remarks>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereEquals(string fieldName, object value)
        {
            WhereEquals(fieldName, value);
            return this;
        }

        /// <summary>
        ///   Matches exact value
        /// </summary>
        /// <remarks>
        ///   Defaults to NotAnalyzed
        /// </remarks>
        public IAsyncDocumentQuery<T> WhereEquals<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value)
        {
            WhereEquals(GetMemberQueryPath(propertySelector.Body), value);
            return this;
        }

        /// <summary>
        /// 	Matches exact value
        /// </summary>
        /// <remarks>
        /// 	Defaults to allow wildcards only if analyzed
        /// </remarks>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereEquals(string fieldName, object value, bool isAnalyzed)
        {
            WhereEquals(fieldName, value, isAnalyzed);
            return this;
        }

        /// <summary>
        ///   Matches exact value
        /// </summary>
        /// <remarks>
        ///   Defaults to allow wildcards only if analyzed
        /// </remarks>
        public IAsyncDocumentQuery<T> WhereEquals<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool isAnalyzed)
        {
            WhereEquals(GetMemberQueryPath(propertySelector.Body), value, isAnalyzed);
            return this;
        }

        /// <summary>
        /// Matches exact value
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereEquals(WhereParams whereParams)
        {
            WhereEquals(whereParams);
            return this;
        }

        /// <summary>
        /// Check that the field has one of the specified value
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereIn(string fieldName, IEnumerable<object> values)
        {
            WhereIn(fieldName, values);
            return this;
        }

        /// <summary>
        /// Check that the field has one of the specified value
        /// </summary>
        public IAsyncDocumentQuery<T> WhereIn<TValue>(Expression<Func<T, TValue>> propertySelector, IEnumerable<TValue> values)
        {
            WhereIn(GetMemberQueryPath(propertySelector.Body), values.Cast<object>());
            return this;
        }

        /// <summary>
        /// Matches fields which starts with the specified value.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="value">The value.</param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereStartsWith(string fieldName, object value)
        {
            WhereStartsWith(fieldName, value);
            return this;
        }

        /// <summary>
        ///   Matches fields which starts with the specified value.
        /// </summary>
        /// <param name = "propertySelector">Property selector for the field.</param>
        /// <param name = "value">The value.</param>
        public IAsyncDocumentQuery<T> WhereStartsWith<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value)
        {
            WhereStartsWith(GetMemberQueryPath(propertySelector.Body), value);
            return this;
        }

        /// <summary>
        /// Matches fields which ends with the specified value.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="value">The value.</param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereEndsWith(string fieldName, object value)
        {
            WhereEndsWith(fieldName, value);
            return this;
        }

        /// <summary>
        ///   Matches fields which ends with the specified value.
        /// </summary>
        /// <param name = "propertySelector">Property selector for the field.</param>
        /// <param name = "value">The value.</param>
        public IAsyncDocumentQuery<T> WhereEndsWith<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value)
        {
            WhereEndsWith(GetMemberQueryPath(propertySelector.Body), value);
            return this;
        }

        /// <summary>
        /// Matches fields where the value is between the specified start and end, exclusive
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="start">The start.</param>
        /// <param name="end">The end.</param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereBetween(string fieldName, object start, object end)
        {
            WhereBetween(fieldName, start, end);
            return this;
        }

        /// <summary>
        ///   Matches fields where the value is between the specified start and end, exclusive
        /// </summary>
        /// <param name = "propertySelector">Property selector for the field.</param>
        /// <param name = "start">The start.</param>
        /// <param name = "end">The end.</param>
        public IAsyncDocumentQuery<T> WhereBetween<TValue>(Expression<Func<T, TValue>> propertySelector, TValue start, TValue end)
        {
            WhereBetween(GetMemberQueryPath(propertySelector.Body), start, end);
            return this;
        }

        /// <summary>
        /// Matches fields where the value is between the specified start and end, inclusive
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="start">The start.</param>
        /// <param name="end">The end.</param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereBetweenOrEqual(string fieldName, object start, object end)
        {
            WhereBetweenOrEqual(fieldName, start, end);
            return this;
        }

        /// <summary>
        ///   Matches fields where the value is between the specified start and end, inclusive
        /// </summary>
        /// <param name = "propertySelector">Property selector for the field.</param>
        /// <param name = "start">The start.</param>
        /// <param name = "end">The end.</param>
        public IAsyncDocumentQuery<T> WhereBetweenOrEqual<TValue>(Expression<Func<T, TValue>> propertySelector, TValue start, TValue end)
        {
            WhereBetweenOrEqual(GetMemberQueryPath(propertySelector.Body), start, end);
            return this;
        }

        /// <summary>
        /// Matches fields where the value is greater than the specified value
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="value">The value.</param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereGreaterThan(string fieldName, object value)
        {
            WhereGreaterThan(fieldName, value);
            return this;
        }

        /// <summary>
        ///   Matches fields where the value is greater than the specified value
        /// </summary>
        /// <param name = "propertySelector">Property selector for the field.</param>
        /// <param name = "value">The value.</param>
        public IAsyncDocumentQuery<T> WhereGreaterThan<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value)
        {
            WhereGreaterThan(GetMemberQueryPath(propertySelector.Body), value);
            return this;
        }

        /// <summary>
        /// Matches fields where the value is greater than or equal to the specified value
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="value">The value.</param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereGreaterThanOrEqual(string fieldName, object value)
        {
            WhereGreaterThanOrEqual(fieldName, value);
            return this;
        }

        /// <summary>
        ///   Matches fields where the value is greater than or equal to the specified value
        /// </summary>
        /// <param name = "propertySelector">Property selector for the field.</param>
        /// <param name = "value">The value.</param>
        public IAsyncDocumentQuery<T> WhereGreaterThanOrEqual<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value)
        {
            WhereGreaterThanOrEqual(GetMemberQueryPath(propertySelector.Body), value);
            return this;
        }

        /// <summary>
        /// Matches fields where the value is less than the specified value
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="value">The value.</param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereLessThan(string fieldName, object value)
        {
            WhereLessThan(fieldName, value);
            return this;
        }

        /// <summary>
        ///   Matches fields where the value is less than the specified value
        /// </summary>
        /// <param name = "propertySelector">Property selector for the field.</param>
        /// <param name = "value">The value.</param>
        public IAsyncDocumentQuery<T> WhereLessThan<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value)
        {
            WhereLessThan(GetMemberQueryPath(propertySelector.Body), value);
            return this;
        }

        /// <summary>
        /// Matches fields where the value is less than or equal to the specified value
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="value">The value.</param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereLessThanOrEqual(string fieldName, object value)
        {
            WhereLessThanOrEqual(fieldName, value);
            return this;
        }

        /// <summary>
        ///   Matches fields where the value is less than or equal to the specified value
        /// </summary>
        /// <param name = "propertySelector">Property selector for the field.</param>
        /// <param name = "value">The value.</param>
        public IAsyncDocumentQuery<T> WhereLessThanOrEqual<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value)
        {
            WhereLessThanOrEqual(GetMemberQueryPath(propertySelector.Body), value);
            return this;
        }

        /// <summary>
        /// Add an AND to the query
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.AndAlso()
        {
            AndAlso();
            return this;
        }

        /// <summary>
        /// Add an OR to the query
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.OrElse()
        {
            OrElse();
            return this;
        }

        /// <summary>
        /// Specifies a boost weight to the last where clause.
        /// The higher the boost factor, the more relevant the term will be.
        /// </summary>
        /// <param name="boost">boosting factor where 1.0 is default, less than 1.0 is lower weight, greater than 1.0 is higher weight</param>
        /// <returns></returns>
        /// <remarks>
        /// http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Boosting%20a%20Term
        /// </remarks>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Boost(decimal boost)
        {
            Boost(boost);
            return this;
        }

        /// <summary>
        /// Specifies a fuzziness factor to the single word term in the last where clause
        /// </summary>
        /// <param name="fuzzy">0.0 to 1.0 where 1.0 means closer match</param>
        /// <returns></returns>
        /// <remarks>
        /// http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Fuzzy%20Searches
        /// </remarks>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Fuzzy(decimal fuzzy)
        {
            Fuzzy(fuzzy);
            return this;
        }

        /// <summary>
        /// Specifies a proximity distance for the phrase in the last where clause
        /// </summary>
        /// <param name="proximity">number of words within</param>
        /// <returns></returns>
        /// <remarks>
        /// http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Proximity%20Searches
        /// </remarks>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Proximity(int proximity)
        {
            Proximity(proximity);
            return this;
        }

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.AlphaNumericOrdering(string fieldName, bool descending)
        {
            AlphaNumericOrdering(fieldName, descending);
            return this;
        }

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.AlphaNumericOrdering<TResult>(Expression<Func<TResult, object>> propertySelector, bool descending)
        {
            var fieldName = GetMemberQueryPath(propertySelector);
            AlphaNumericOrdering(fieldName, descending);
            return this;
        }

        /// <summary>
        /// Order the search results randomly
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.RandomOrdering()
        {
            RandomOrdering();
            return this;
        }

        /// <summary>
        /// Order the search results randomly using the specified seed
        /// this is useful if you want to have repeatable random queries
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.RandomOrdering(string seed)
        {
            RandomOrdering(seed);
            return this;
        }

        /// <summary>
        /// Order the search results randomly
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.CustomSortUsing(string typeName, bool descending)
        {
            CustomSortUsing(typeName, descending);
            return this;
        }

        /// <summary>
        /// Filter matches to be inside the specified radius
        /// </summary>
        /// <param name="radius">The radius.</param>
        /// <param name="latitude">The latitude.</param>
        /// <param name="longitude">The longitude.</param>
        /// <param name="radiusUnits">The units of the <paramref name="radius"/>.</param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WithinRadiusOf(double radius, double latitude, double longitude, SpatialUnits radiusUnits)
        {
            return GenerateQueryWithinRadiusOf(Constants.DefaultSpatialFieldName, radius, latitude, longitude, radiusUnits: radiusUnits);
        }

        /// <summary>
        /// Filter matches to be inside the specified radius
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WithinRadiusOf(string fieldName, double radius, double latitude, double longitude, SpatialUnits radiusUnits)
        {
            return GenerateQueryWithinRadiusOf(fieldName, radius, latitude, longitude, radiusUnits: radiusUnits);
        }

        public IAsyncDocumentQuery<T> RelatesToShape(string fieldName, string shapeWKT, SpatialRelation rel, double distanceErrorPct = 0.025)
        {
            return GenerateSpatialQueryData(fieldName, shapeWKT, rel, distanceErrorPct);
        }

        /// <summary>
        /// Sorts the query results by distance.
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.SortByDistance()
        {
            OrderBy(Constants.DistanceFieldName);
            return this;
        }

        /// <summary>
        /// Sorts the query results by distance.
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.SortByDistance(double lat, double lng)
        {
            OrderBy(string.Format("{0};{1};{2}", Constants.DistanceFieldName, lat.ToInvariantString(), lng.ToInvariantString()));
            return this;
        }
        /// <summary>
        /// Sorts the query results by distance.
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.SortByDistance(double lat, double lng, string spatialFieldName)
        {
            OrderBy(string.Format("{0};{1};{2};{3}", Constants.DistanceFieldName, lat.ToInvariantString(), lng.ToInvariantString(), spatialFieldName));
            return this;
        }

        /// <summary>
        /// Order the results by the specified fields
        /// The fields are the names of the fields to sort, defaulting to sorting by ascending.
        /// You can prefix a field name with '-' to indicate sorting by descending or '+' to sort by ascending
        /// </summary>
        /// <param name="fields">The fields.</param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.OrderBy(params string[] fields)
        {
            OrderBy(fields);
            return this;
        }

        /// <summary>
        ///   Order the results by the specified fields
        ///   The fields are the names of the fields to sort, defaulting to sorting by ascending.
        ///   You can prefix a field name with '-' to indicate sorting by descending or '+' to sort by ascending
        /// </summary>
        /// <param name = "propertySelectors">Property selectors for the fields.</param>
        public IAsyncDocumentQuery<T> OrderBy<TValue>(params Expression<Func<T, TValue>>[] propertySelectors)
        {
            OrderBy(propertySelectors.Select(GetMemberQueryPathForOrderBy).ToArray());
            return this;
        }

        /// <summary>
        /// Order the results by the specified fields
        /// The fields are the names of the fields to sort, defaulting to sorting by descending.
        /// You can prefix a field name with '-' to indicate sorting by descending or '+' to sort by ascending
        /// </summary>
        /// <param name="fields">The fields.</param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.OrderByDescending(params string[] fields)
        {
            OrderByDescending(fields);
            return this;
        }

        /// <summary>
        ///   Order the results by the specified fields
        ///   The fields are the names of the fields to sort, defaulting to sorting by descending.
        ///   You can prefix a field name with '-' to indicate sorting by descending or '+' to sort by ascending
        /// </summary>
        /// <param name = "propertySelectors">Property selectors for the fields.</param>
        public IAsyncDocumentQuery<T> OrderByDescending<TValue>(params Expression<Func<T, TValue>>[] propertySelectors)
        {
            OrderByDescending(propertySelectors.Select(GetMemberQueryPathForOrderBy).ToArray());
            return this;
        }		

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Highlight(
            string fieldName, int fragmentLength, int fragmentCount, string fragmentsField)
        {
            Highlight(fieldName, fragmentLength, fragmentCount, fragmentsField);
            return this;
        }

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Highlight(string fieldName, int fragmentLength, int fragmentCount,
            out FieldHighlightings fieldHighlightings)
        {
            this.Highlight(fieldName, fragmentLength, fragmentCount, out fieldHighlightings);
            return this;
        }

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Highlight(string fieldName, string fieldKeyName, int fragmentLength, int fragmentCount,
            out FieldHighlightings fieldHighlightings)
        {
            this.Highlight(fieldName, fieldKeyName, fragmentLength, fragmentCount, out fieldHighlightings);
            return this;
        }

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Highlight<TValue>(
            Expression<Func<T, TValue>> propertySelector, 
            int fragmentLength, 
            int fragmentCount,
            Expression<Func<T, IEnumerable>> fragmentsPropertySelector)
        {
            var fieldName = this.GetMemberQueryPath(propertySelector);
            var fragmentsField = this.GetMemberQueryPath(fragmentsPropertySelector);
            this.Highlight(fieldName, fragmentLength, fragmentCount, fragmentsField);
            return this;
        }

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Highlight<TValue>(
            Expression<Func<T, TValue>> propertySelector, int fragmentLength, int fragmentCount,
            out FieldHighlightings fieldHighlightings)
        {
            this.Highlight(this.GetMemberQueryPath(propertySelector), fragmentLength, fragmentCount, out fieldHighlightings);
            return this;
        }

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Highlight<TValue>(
            Expression<Func<T, TValue>> propertySelector, Expression<Func<T, TValue>> keyPropertySelector, int fragmentLength, int fragmentCount,
            out FieldHighlightings fieldHighlightings)
        {
            this.Highlight(this.GetMemberQueryPath(propertySelector), this.GetMemberQueryPath(keyPropertySelector), fragmentLength, fragmentCount, out fieldHighlightings);
            return this;
        }

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.SetHighlighterTags(string preTag, string postTag)
        {
            this.SetHighlighterTags(new[]{preTag}, new[]{postTag});
            return this;
        }

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.SetHighlighterTags(string[] preTags, string[] postTags)
        {
            this.SetHighlighterTags(preTags, postTags);
            return this;
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of now.
        /// </summary>
        /// <returns></returns>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WaitForNonStaleResultsAsOfNow()
        {
            WaitForNonStaleResultsAsOfNow();
            return this;
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of now for the specified timeout.
        /// </summary>
        /// <param name="waitTimeout">The wait timeout.</param>
        /// <returns></returns>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WaitForNonStaleResultsAsOfNow(TimeSpan waitTimeout)
        {
            WaitForNonStaleResultsAsOfNow(waitTimeout);
            return this;
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of the cutoff date.
        /// </summary>
        /// <param name="cutOff">The cut off.</param>
        /// <returns></returns>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WaitForNonStaleResultsAsOf(DateTime cutOff)
        {
            WaitForNonStaleResultsAsOf(cutOff);
            return this;
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of the cutoff date for the specified timeout
        /// </summary>
        /// <param name="cutOff">The cut off.</param>
        /// <param name="waitTimeout">The wait timeout.</param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WaitForNonStaleResultsAsOf(DateTime cutOff, TimeSpan waitTimeout)
        {
            WaitForNonStaleResultsAsOf(cutOff, waitTimeout);
            return this;
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of the cutoff etag.
        /// </summary>
        /// <param name="cutOffEtag">The cut off etag.</param>
        /// <returns></returns>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WaitForNonStaleResultsAsOf(long? cutOffEtag)
        {
            WaitForNonStaleResultsAsOf(cutOffEtag);
            return this;
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of the cutoff etag for the specified timeout.
        /// </summary>
        /// <param name="cutOffEtag">The cut off etag.</param>
        /// <param name="waitTimeout">The wait timeout.</param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WaitForNonStaleResultsAsOf(long? cutOffEtag, TimeSpan waitTimeout)
        {
            WaitForNonStaleResultsAsOf(cutOffEtag, waitTimeout);
            return this;
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of the last write made by any session belonging to the 
        /// current document store.
        /// This ensures that you'll always get the most relevant results for your scenarios using simple indexes (map only or dynamic queries).
        /// However, when used to query map/reduce indexes, it does NOT guarantee that the document that this etag belong to is actually considered for the results. 
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WaitForNonStaleResultsAsOfLastWrite()
        {
            WaitForNonStaleResultsAsOfLastWrite();
            return this;
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of the last write made by any session belonging to the 
        /// current document store.
        /// This ensures that you'll always get the most relevant results for your scenarios using simple indexes (map only or dynamic queries).
        /// However, when used to query map/reduce indexes, it does NOT guarantee that the document that this etag belong to is actually considered for the results. 
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WaitForNonStaleResultsAsOfLastWrite(TimeSpan waitTimeout)
        {
            WaitForNonStaleResultsAsOfLastWrite(waitTimeout);
            return this;
        }

        /// <summary>
        /// EXPERT ONLY: Instructs the query to wait for non stale results.
        /// This shouldn't be used outside of unit tests unless you are well aware of the implications
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WaitForNonStaleResults()
        {
            WaitForNonStaleResults();
            return this;
        }

        /// <summary>
        /// Allows you to modify the index query before it is sent to the server
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.BeforeQueryExecution(Action<IndexQuery> beforeQueryExecution)
        {
            BeforeQueryExecution(beforeQueryExecutionAction);
            return this;
        }

        /// <summary>
        /// EXPERT ONLY: Instructs the query to wait for non stale results for the specified wait timeout.
        /// This shouldn't be used outside of unit tests unless you are well aware of the implications
        /// </summary>
        /// <param name="waitTimeout">The wait timeout.</param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WaitForNonStaleResults(TimeSpan waitTimeout)
        {
            WaitForNonStaleResults(waitTimeout);
            return this;
        }

        /// <summary>
        /// Selects all the projection fields directly from the index
        /// </summary>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        public virtual IAsyncDocumentQuery<TProjection> SelectFields<TProjection>()
        {
            return SelectFields<TProjection>(ReflectionUtil.GetPropertiesAndFieldsFor<TProjection>(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public).Select(x => x.Name).ToArray());
        }

        /// <summary>
        /// Selects the specified fields directly from the index
        /// </summary>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        public IAsyncDocumentQuery<TProjection> SelectFields<TProjection>(params string[] fields)
        {
            return SelectFields<TProjection>(fields, fields);
        }

        /// <summary>
        /// Selects the specified fields directly from the index
        /// </summary>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        public virtual IAsyncDocumentQuery<TProjection> SelectFields<TProjection>(string[] fields, string[] projections)
        {
            var asyncDocumentQuery = new AsyncDocumentQuery<TProjection>(theSession,
                                                                         theDatabaseCommands,
                                                                         theAsyncDatabaseCommands,
                                                                         indexName, fields, projections, queryListeners,
                                                                         isMapReduce)
                                        {
                                            pageSize = pageSize,
                                            queryText = new StringBuilder(queryText.ToString()),
                                            start = start,
                                            timeout = timeout,
                                            cutoff = cutoff,
                                            cutoffEtag = cutoffEtag,
                                            queryStats = queryStats,
                                            theWaitForNonStaleResults = theWaitForNonStaleResults,
                                            theWaitForNonStaleResultsAsOfNow = theWaitForNonStaleResultsAsOfNow,
                                            orderByFields = orderByFields,
                                            isDistinct = isDistinct,
                                            allowMultipleIndexEntriesForSameDocumentToResultTransformer = allowMultipleIndexEntriesForSameDocumentToResultTransformer,
                                            negate = negate,
                                            transformResultsFunc = transformResultsFunc,
                                            includes = new HashSet<string>(includes),
                                            isSpatialQuery = isSpatialQuery,
                                            spatialFieldName = spatialFieldName,
                                            queryShape = queryShape,
                                            spatialRelation = spatialRelation,
                                            spatialUnits = spatialUnits,
                                            distanceErrorPct = distanceErrorPct,
                                            rootTypes = { typeof(T) },
                                            defaultField = defaultField,
                                            beforeQueryExecutionAction = beforeQueryExecutionAction,
                                            afterQueryExecutedCallback = afterQueryExecutedCallback,
                                            afterStreamExecutedCallback = afterStreamExecutedCallback,
                                            highlightedFields = new List<HighlightedField>(highlightedFields),
                                            highlighterPreTags = highlighterPreTags,
                                            highlighterPostTags = highlighterPostTags,
                                            resultsTransformer = resultsTransformer,
                                            transformerParameters = transformerParameters,
                                            disableEntitiesTracking = disableEntitiesTracking,
                                            disableCaching = disableCaching,
                                            showQueryTimings = showQueryTimings,
                                            lastEquality = lastEquality,
                                            shouldExplainScores = shouldExplainScores
                                        };
            asyncDocumentQuery.AfterQueryExecuted(afterQueryExecutedCallback);
            return asyncDocumentQuery;
        }

        public IAsyncDocumentQuery<T> Spatial(Expression<Func<T, object>> path, Func<SpatialCriteriaFactory, SpatialCriteria> clause)
        {
            return Spatial(path.ToPropertyPath(), clause);
        }

        public IAsyncDocumentQuery<T> Spatial(string fieldName, Func<SpatialCriteriaFactory, SpatialCriteria> clause)
        {
            var criteria = clause(new SpatialCriteriaFactory());
            return GenerateSpatialQueryData(fieldName, criteria);
        }

        public void SetQueryInputs(Dictionary<string, RavenJToken> queryInputs)
        {
            SetTransformerParameters(queryInputs);
        }

        public void SetTransformerParameters(Dictionary<string, RavenJToken> parameters)
        {
            transformerParameters = parameters;
        }


        /// <summary>
        /// Register the query as a lazy-count query in the session and return a lazy
        /// instance that will evaluate the query only when needed
        /// </summary>
        public Lazy<Task<int>> CountLazilyAsync(CancellationToken token = default (CancellationToken))
        {
            if (queryOperation == null)
            {
                ExecuteBeforeQueryListeners();
                Take(0);
                queryOperation = InitializeQueryOperation();
            }

            var lazyQueryOperation = new LazyQueryOperation<T>(queryOperation, afterQueryExecutedCallback, includes, AsyncDatabaseCommands.OperationsHeaders);

            return ((AsyncDocumentSession)theSession).AddLazyCountOperation(lazyQueryOperation,token);
        }

        /// <summary>
        /// Adds an ordering for a specific field to the query
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="descending">if set to <c>true</c> [descending].</param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.AddOrder(string fieldName, bool descending)
        {
            AddOrder(fieldName, descending);
            return this;
        }

        /// <summary>
        /// Adds an ordering by score for a specific field to the query
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.OrderByScore()
        {
            AddOrder(Constants.TemporaryScoreValue, false);
            return this;
        }

        /// <summary>
        /// Adds an ordering by score descending for a specific field to the query
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.OrderByScoreDescending()
        {
            AddOrder(Constants.TemporaryScoreValue, true);
            return this;
        }

        /// <summary>
        ///   Adds an ordering for a specific field to the query
        /// </summary>
        /// <param name = "propertySelector">Property selector for the field.</param>
        /// <param name = "descending">if set to <c>true</c> [descending].</param>
        public IAsyncDocumentQuery<T> AddOrder<TValue>(Expression<Func<T, TValue>> propertySelector, bool descending)
        {
            AddOrder(GetMemberQueryPath(propertySelector.Body), descending);
            return this;
        }

        /// <summary>
        /// Adds an ordering for a specific field to the query and specifies the type of field for sorting purposes
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="descending">if set to <c>true</c> [descending].</param>
        /// <param name="fieldType">the type of the field to be sorted.</param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.AddOrder(string fieldName, bool descending, Type fieldType)
        {
            AddOrder(fieldName, descending, fieldType);
            return this;
        }

        /// <summary>
        /// Simplified method for opening a new clause within the query
        /// </summary>
        /// <returns></returns>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.OpenSubclause()
        {
            OpenSubclause();
            return this;
        }

        /// <summary>
        /// Perform a search for documents which fields that match the searchTerms.
        /// If there is more than a single term, each of them will be checked independently.
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Search(string fieldName, string searchTerms, EscapeQueryOptions escapeQueryOptions)
        {
            Search(fieldName, searchTerms, escapeQueryOptions);
            return this;
        }

        /// <summary>
        /// Perform a search for documents which fields that match the searchTerms.
        /// If there is more than a single term, each of them will be checked independently.
        /// </summary>
        public IAsyncDocumentQuery<T> Search<TValue>(Expression<Func<T, TValue>> propertySelector, string searchTerms, EscapeQueryOptions escapeQueryOptions = EscapeQueryOptions.RawQuery)
        {
            Search(GetMemberQueryPath(propertySelector.Body), searchTerms, escapeQueryOptions);
            return this;
        }

        /// <summary>
        /// Simplified method for closing a clause within the query
        /// </summary>
        /// <returns></returns>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.CloseSubclause()
        {
            CloseSubclause();
            return this;
        }

        /// <summary>
        /// Partition the query so we can intersect different parts of the query
        /// across different index entries.
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Intersect()
        {
            Intersect();
            return this;
        }

        /// <summary>
        /// Performs a query matching ANY of the provided values against the given field (OR)
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.ContainsAny(string fieldName, IEnumerable<object> values)
        {
            ContainsAny(fieldName, values);
            return this;
        }

        /// <summary>
        /// Performs a query matching ANY of the provided values against the given field (OR)
        /// </summary>
        public IAsyncDocumentQuery<T> ContainsAny<TValue>(Expression<Func<T, TValue>> propertySelector, IEnumerable<TValue> values)
        {
            ContainsAny(GetMemberQueryPath(propertySelector.Body), values.Cast<object>());
            return this;
        }

        /// <summary>
        /// Performs a query matching ALL of the provided values against the given field (AND)
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.ContainsAll(string fieldName, IEnumerable<object> values)
        {
            ContainsAll(fieldName, values);
            return this;
        }

        /// <summary>
        /// Performs a query matching ALL of the provided values against the given field (AND)
        /// </summary>
        public IAsyncDocumentQuery<T> ContainsAll<TValue>(Expression<Func<T, TValue>> propertySelector, IEnumerable<TValue> values)
        {
            ContainsAll(GetMemberQueryPath(propertySelector.Body), values.Cast<object>());
            return this;
        }

        /// <summary>
        /// Provide statistics about the query, such as total count of matching records
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Statistics(out RavenQueryStatistics stats)
        {
            Statistics(out stats);
            return this;
        }

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.UsingDefaultField(string field)
        {
            UsingDefaultField(field);
            return this;
        }

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.UsingDefaultOperator(QueryOperator queryOperator)
        {
            UsingDefaultOperator(queryOperator);
            return this;
        }

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.NoTracking()
        {
            NoTracking();
            return this;
        }

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.NoCaching()
        {
            NoCaching();
            return this;
        }

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.ShowTimings()
        {
            ShowTimings();
            return this;
        }

        IAsyncDocumentQuery<T>  IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Distinct()
        {
            Distinct();
            return this;
        }

        /// <summary>
        /// Sets a transformer to use after executing a query
        /// </summary>
        /// <param name="resultsTransformer"></param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.SetResultTransformer(string resultsTransformer)
        {
            base.SetResultTransformer(resultsTransformer);
            return this;
        }

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.ExplainScores()
        {
            shouldExplainScores = true;
            return this;

    }

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.SetAllowMultipleIndexEntriesForSameDocumentToResultTransformer(bool val)
        {
            base.SetAllowMultipleIndexEntriesForSameDocumentToResultTransformer(val);
            return this;
}	}
}
