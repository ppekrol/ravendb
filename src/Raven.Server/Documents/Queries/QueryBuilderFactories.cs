﻿using System;
using Raven.Server.Documents.Indexes.Static.Spatial;

namespace Raven.Server.Documents.Queries
{
    internal sealed class QueryBuilderFactories
    {
        public Func<string, SpatialField> GetSpatialFieldFactory { get; set; }
        public Func<string, System.Text.RegularExpressions.Regex> GetRegexFactory { get; set; }
    }
}
