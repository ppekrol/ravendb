﻿using Corax.Mappings;
using NLog;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Queries;
using Raven.Server.Indexing;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.Persistence;

public interface IIndexReadOperationFactory
{
    public LuceneIndexReadOperation CreateLuceneIndexReadOperation(Index index, LuceneVoronDirectory directory, LuceneIndexSearcherHolder searcherHolder,
        QueryBuilderFactories queryBuilderFactories, Transaction readTransaction, IndexQueryServerSide query);

    public CoraxIndexReadOperation CreateCoraxIndexReadOperation(Index index, Logger logger, Transaction readTransaction, QueryBuilderFactories queryBuilderFactories,
        IndexFieldsMapping fieldsMapping, IndexQueryServerSide query);

    public LuceneSuggestionIndexReader CreateLuceneSuggestionIndexReader(Index index, LuceneVoronDirectory directory, LuceneIndexSearcherHolder searcherHolder,
        Transaction readTransaction);

}
