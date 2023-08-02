﻿using Raven.Client.Documents.Queries.Suggestions;
using Raven.Server.Documents.Indexes.Persistence;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Sharding.Queries;
using Raven.Server.Indexing;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.Sharding.Persistence.Lucene;

internal sealed class ShardedLuceneSuggestionIndexReader : LuceneSuggestionIndexReader
{
    public ShardedLuceneSuggestionIndexReader(Index index, LuceneVoronDirectory directory, LuceneIndexSearcherHolder searcherHolder, Transaction readTransaction) : base(index, directory, searcherHolder, readTransaction)
    {
    }

    internal override void AddPopularity(SuggestWord suggestion, ref SuggestionResult result)
    {
        result = result.AddPopularity(suggestion);
    }
}
