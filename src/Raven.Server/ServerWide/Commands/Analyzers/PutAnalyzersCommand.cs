﻿using System.Collections.Generic;
using Raven.Client.Documents.Indexes.Analysis;
using Raven.Client.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Analyzers
{
    internal sealed class PutAnalyzersCommand : UpdateDatabaseCommand
    {
        public List<AnalyzerDefinition> Analyzers = new List<AnalyzerDefinition>();

        public PutAnalyzersCommand()
        {
            // for deserialization
        }

        public PutAnalyzersCommand(string databaseName, string uniqueRequestId)
            : base(databaseName, uniqueRequestId)
        {
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (Analyzers != null)
            {
                foreach (var definition in Analyzers)
                    record.AddAnalyzer(definition);
            }
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Analyzers)] = TypeConverter.ToBlittableSupportedType(Analyzers);
        }
    }
}
