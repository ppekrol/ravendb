﻿using Corax.Mappings;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Queries;
using Raven.Server.Indexing;
using Sparrow.Logging;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.Persistence;

internal sealed class DatabaseIndexReadOperationFactory : IIndexReadOperationFactory
{
    public LuceneIndexReadOperation CreateLuceneIndexReadOperation(Index index, LuceneVoronDirectory directory, LuceneIndexSearcherHolder searcherHolder,
        QueryBuilderFactories queryBuilderFactories, Transaction readTransaction, IndexQueryServerSide query)
    {
        return new LuceneIndexReadOperation(index, directory, searcherHolder, queryBuilderFactories, readTransaction, query);
    }

    public CoraxIndexReadOperation CreateCoraxIndexReadOperation(Index index, Logger logger, Transaction readTransaction, QueryBuilderFactories queryBuilderFactories,
        IndexFieldsMapping fieldsMapping, IndexQueryServerSide query)
    {
        return new CoraxIndexReadOperation(index, logger, readTransaction, queryBuilderFactories, fieldsMapping, query);
    }

    public LuceneSuggestionIndexReader CreateLuceneSuggestionIndexReader(Index index, LuceneVoronDirectory directory, LuceneIndexSearcherHolder searcherHolder,
        Transaction readTransaction)
    {
        return new LuceneSuggestionIndexReader(index, directory, searcherHolder, readTransaction);
    }
}
