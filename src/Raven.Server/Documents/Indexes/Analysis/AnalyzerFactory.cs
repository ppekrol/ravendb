﻿using System;
using Lucene.Net.Analysis;
using Raven.Server.Documents.Indexes.Persistence.Lucene;

namespace Raven.Server.Documents.Indexes.Analysis
{
    interal class AnalyzerFactory
    {
        public readonly Type Type;

        public AnalyzerFactory(Type analyzerType)
        {
            Type = analyzerType ?? throw new ArgumentNullException(nameof(analyzerType));
        }

        public virtual Analyzer CreateInstance(string fieldName)
        {
            return LuceneIndexingExtensions.CreateAnalyzerInstance(fieldName, Type);
        }
    }
}
