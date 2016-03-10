//-----------------------------------------------------------------------
// <copyright file="SpatialIndexQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Globalization;
using Raven.Abstractions.Indexing;

namespace Raven.Abstractions.Data
{
    /// <summary>
    /// A query using spatial filtering
    /// </summary>
    public class SpatialIndexQuery : IndexQuery
    {
        public static string GetQueryShapeFromLatLon(double lat, double lng, double radius)
        {
            return "Circle(" +
                   lng.ToString("F6", CultureInfo.InvariantCulture) + " " +
                   lat.ToString("F6", CultureInfo.InvariantCulture) + " " +
                   "d=" + radius.ToString("F6", CultureInfo.InvariantCulture) +
                   ")";
        }

        /// <summary>
        /// Shape in WKT format.
        /// </summary>
        public string QueryShape { get; set; }

        /// <summary>
        /// Spatial relation (Within, Contains, Disjoint, Intersects, Nearby)
        /// </summary>
        public SpatialRelation SpatialRelation { get; set; }

        /// <summary>
        /// A measure of acceptable error of the shape as a fraction. This effectively
        /// inflates the size of the shape but should not shrink it.
        /// </summary>
        /// <value>Default value is 0.025</value>
        public double DistanceErrorPercentage { get; set; }

        /// <summary>
        /// Overrides the units defined in the spatial index
        /// </summary>
        public SpatialUnits? RadiusUnitOverride { get; set; }

        private string spatialFieldName = Constants.DefaultSpatialFieldName;
        public string SpatialFieldName
        {
            get { return spatialFieldName; }
            set { spatialFieldName = value; }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SpatialIndexQuery"/> class.
        /// </summary>
        /// <param name="query">The query.</param>
        public SpatialIndexQuery(IndexQuery query) : this()
        {
            Query = query.Query;
            Start = query.Start;
            Cutoff = query.Cutoff;
            WaitForNonStaleResultsAsOfNow = query.WaitForNonStaleResultsAsOfNow;
            CutoffEtag = query.CutoffEtag;
            PageSize = query.PageSize;
            FieldsToFetch = query.FieldsToFetch;
            DefaultField = query.DefaultField; // keep?
            DefaultOperator = query.DefaultOperator;
            SortedFields = query.SortedFields;
            HighlightedFields = query.HighlightedFields;
            HighlighterPreTags = query.HighlighterPreTags;
            HighlighterPostTags = query.HighlighterPostTags;
            HighlighterKeyName = query.HighlighterKeyName;
            ResultsTransformer = query.ResultsTransformer;
            TransformerParameters = query.TransformerParameters;
            ExplainScores = query.ExplainScores;
            IsDistinct = query.IsDistinct;
            AllowMultipleIndexEntriesForSameDocumentToResultTransformer =
                query.AllowMultipleIndexEntriesForSameDocumentToResultTransformer;
            
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SpatialIndexQuery"/> class.
        /// </summary>
        public SpatialIndexQuery()
        {
            DistanceErrorPercentage = Constants.DefaultSpatialDistanceErrorPct;
        }

        /// <summary>
        /// Gets the custom query string variables.
        /// </summary>
        /// <returns></returns>
        protected override string GetCustomQueryStringVariables()
        {
            var unitsParam = string.Empty;
            if (RadiusUnitOverride.HasValue)
                unitsParam = string.Format("&spatialUnits={0}", RadiusUnitOverride.Value);

            return string.Format("queryShape={0}&spatialRelation={1}&spatialField={2}&distErrPrc={3}{4}",
                Uri.EscapeDataString(QueryShape),
                SpatialRelation,
                spatialFieldName,
                DistanceErrorPercentage.ToString(CultureInfo.InvariantCulture),
                unitsParam);
        }
    }
}
