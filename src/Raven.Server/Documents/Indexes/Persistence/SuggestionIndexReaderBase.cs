﻿using System.Threading;
using NLog;
using Raven.Client.Documents.Queries.Suggestions;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Suggestions;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence
{
    public abstract class SuggestionIndexReaderBase : IndexOperationBase
    {
        protected SuggestionIndexReaderBase(Index index, Logger logger) : base(index, logger)
        {
        }

        public abstract SuggestionResult Suggestions(IndexQueryServerSide query, SuggestionField field, JsonOperationContext documentsContext, CancellationToken token);
    }
}
