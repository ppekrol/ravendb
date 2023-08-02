﻿using System;
using System.Collections.Generic;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Exceptions.Documents;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries.Facets
{
    internal sealed class FacetQuery
    {
        public readonly IndexQueryServerSide Query;

        public readonly Dictionary<string, FacetSetup> Facets;

        public readonly long FacetsEtag;

        public bool Legacy;

        private FacetQuery(IndexQueryServerSide query, Dictionary<string, FacetSetup> facets, long facetsEtag)
        {
            Query = query;
            Facets = facets;
            FacetsEtag = facetsEtag;
        }

        public static FacetQuery Create(DocumentsOperationContext context, IndexQueryServerSide query)
        {
            long? facetsEtag = null;
            DocumentsTransaction tx = null;
            try
            {
                var facets = new Dictionary<string, FacetSetup>(StringComparer.OrdinalIgnoreCase);
                foreach (var selectField in query.Metadata.SelectFields)
                {
                    if (selectField.IsFacet == false)
                        continue;

                    var facetField = (FacetField)selectField;
                    if (facetField.FacetSetupDocumentId == null || facets.ContainsKey(facetField.FacetSetupDocumentId))
                        continue;

                    if (tx == null)
                        tx = context.OpenReadTransaction();

                    var documentJson = context.DocumentDatabase.DocumentsStorage.Get(context, facetField.FacetSetupDocumentId);
                    if (documentJson == null)
                    {
                        // it the query comes from the sharding orchestrator then we send it via query parameters

                        if (query.QueryParameters.TryGet(facetField.FacetSetupDocumentId, out BlittableJsonReaderObject doc) == false)
                            throw new DocumentDoesNotExistException(facetField.FacetSetupDocumentId);

                        documentJson = new Document
                        {
                            Data = doc
                        };
                    }

                    if (facetsEtag.HasValue == false)
                        facetsEtag = documentJson.Etag;
                    else
                        facetsEtag = facetsEtag.Value ^ documentJson.Etag;

                    var document = FacetSetup.Create(facetField.FacetSetupDocumentId, documentJson.Data);

                    facets[facetField.FacetSetupDocumentId] = document;
                }

                return new FacetQuery(query, facets, facetsEtag ?? 0)
                {
                    Legacy = string.IsNullOrEmpty(query.ClientVersion) == false && query.ClientVersion[0] == '4'
                };
            }
            finally
            {
                tx?.Dispose();
            }
        }
    }
}
