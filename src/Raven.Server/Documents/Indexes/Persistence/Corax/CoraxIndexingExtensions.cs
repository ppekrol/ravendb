﻿using System;
using Corax.Analyzers;
using Raven.Server.Documents.Indexes.Persistence.Lucene;


namespace Raven.Server.Documents.Indexes.Persistence.Corax
{
    public sealed class CoraxIndexingExtensions
    {
        public static Analyzer CreateAnalyzerInstance(string fieldName, Type analyzerType)
        {
            return LuceneAnalyzerAdapter.Create(LuceneIndexingExtensions.CreateAnalyzerInstance(fieldName, analyzerType));
        }
    }
}
