﻿using System;
using Lucene.Net.Analysis;

namespace Raven.Server.Documents.Indexes.Analysis
{
    internal sealed class FaultyAnalyzerFactory : AnalyzerFactory
    {
        private readonly string _name;
        private readonly Exception _e;

        public FaultyAnalyzerFactory(string name, Exception e)
            : base(typeof(Analyzer))
        {
            _name = name;
            _e = e;
        }

        public override Analyzer CreateInstance(string fieldName)
        {
            throw new NotSupportedException($"Analyzer {_name} is an implementation of a faulty analyzer", _e);
        }
    }
}
