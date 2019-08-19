﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Configuration;
using Sparrow;

namespace Raven.Server.Documents.Queries
{
    public class FieldsToFetch
    {
        public readonly Dictionary<string, FieldToFetch> Fields;

        public readonly bool ExtractAllFromIndex;

        public readonly bool AnyExtractableFromIndex;

        public readonly bool SingleBodyOrMethodWithNoAlias;

        public readonly bool IsProjection;

        public readonly bool IsDistinct;

        public FieldsToFetch(IndexQueryServerSide query, IndexDefinitionBase indexDefinition, IndexingConfiguration configuration = null)
        {
            Fields = GetFieldsToFetch(query.Metadata, indexDefinition, configuration, out AnyExtractableFromIndex, out bool extractAllStoredFields, out SingleBodyOrMethodWithNoAlias);
            IsProjection = Fields != null && Fields.Count > 0;

            if (extractAllStoredFields)
            {
                AnyExtractableFromIndex = true;
                ExtractAllFromIndex = true; // we want to add dynamic fields also to the result (stored only)
                IsProjection = true;
            }

            IsDistinct = query.Metadata.IsDistinct && IsProjection;
        }

        private static FieldToFetch GetFieldToFetch(
            IndexDefinitionBase indexDefinition,
            SelectField selectField,
            Dictionary<string, FieldToFetch> results,
            out string selectFieldKey,
            ref bool anyExtractableFromIndex,
            ref bool extractAllStoredFields)
        {
            selectFieldKey = selectField.Alias ?? selectField.Name;
            var selectFieldName = selectField.Name;

            if (selectField.ValueTokenType != null)
            {
                return new FieldToFetch(string.Empty, selectField, selectField.Alias,
                    canExtractFromIndex: false, isDocumentId: false);
            }

            if (selectField.Function != null)
            {
                var fieldToFetch = new FieldToFetch(selectField.Name, selectField, selectField.Alias,
                    canExtractFromIndex: false, isDocumentId: false)
                {
                    FunctionArgs = new FieldToFetch[selectField.FunctionArgs.Length]
                };
                for (int j = 0; j < selectField.FunctionArgs.Length; j++)
                {
                    var ignored = false;
                    fieldToFetch.FunctionArgs[j] = GetFieldToFetch(indexDefinition,
                        selectField.FunctionArgs[j],
                        null,
                        out _,
                        ref ignored,
                        ref ignored
                    );
                }
                return fieldToFetch;
            }

            if (selectField.IsCounter)
            {
                var fieldToFetch = new FieldToFetch(selectField.Name, selectField, selectField.Alias ?? selectField.Name,
                    canExtractFromIndex: false, isDocumentId: false);
                if (selectField.FunctionArgs != null)
                {
                    fieldToFetch.FunctionArgs = new FieldToFetch[0];
                }

                return fieldToFetch;
            }

            if (selectFieldName == null)
            {
                if (selectField.IsGroupByKey == false)
                    return null;

                if (selectField.GroupByKeys.Length == 1)
                {
                    selectFieldName = selectField.GroupByKeys[0].Name;

                    if (selectFieldKey == null)
                        selectFieldKey = selectFieldName;
                }
                else
                {
                    selectFieldKey = selectFieldKey ?? "Key";
                    return new FieldToFetch(selectFieldKey, selectField.GroupByKeyNames);
                }
            }

            if (indexDefinition == null)
            {
                return new FieldToFetch(selectFieldName, selectField, selectField.Alias, canExtractFromIndex: false, isDocumentId: false);
            }

            if (selectFieldName.Value.Length > 0)
            {
                if (selectFieldName == Constants.Documents.Indexing.Fields.DocumentIdFieldName)
                {
                    anyExtractableFromIndex = true;
                    return new FieldToFetch(selectFieldName, selectField, selectField.Alias, canExtractFromIndex: false, isDocumentId: true);
                }

                if (selectFieldName.Value[0] == '_')
                {
                    if (selectFieldName == Constants.Documents.Indexing.Fields.AllStoredFields)
                    {
                        if (results == null)
                            ThrowInvalidFetchAllStoredDocuments();
                        Debug.Assert(results != null);
                        results.Clear(); // __all_stored_fields should only return stored fields so we are ensuring that no other fields will be returned

                        extractAllStoredFields = true;

                        foreach (var kvp in indexDefinition.MapFields)
                        {
                            var stored = kvp.Value.Storage == FieldStorage.Yes;
                            if (stored == false)
                                continue;

                            anyExtractableFromIndex = true;
                            results[kvp.Key] = new FieldToFetch(kvp.Key, null, null, canExtractFromIndex: true, isDocumentId: false);
                        }

                        return null;
                    }
                }
            }

            var bySourceAlias = ShouldTryToExtractBySourceAliasName(selectFieldName.Value, selectField);
            var key = bySourceAlias
                    ? selectField.SourceAlias
                    : selectFieldName;

            var extract = indexDefinition.MapFields.TryGetValue(key, out var value) &&
                          value.Storage == FieldStorage.Yes;

            if (extract)
                anyExtractableFromIndex = true;

            if (bySourceAlias == false)
            {
                extract |= indexDefinition.HasDynamicFields;
            }

            return new FieldToFetch(selectFieldName, selectField, selectField.Alias, extract, isDocumentId: false);
        }

        private static bool ShouldTryToExtractBySourceAliasName(string selectFieldName, SelectField selectField)
        {
            return selectFieldName.Length == 0 &&
                   selectField.HasSourceAlias &&
                   selectField.SourceAlias != null;
        }

        private static void ThrowInvalidFetchAllStoredDocuments()
        {
            throw new InvalidOperationException("Cannot fetch all stored path from a nested method");
        }

        private static Dictionary<string, FieldToFetch> GetFieldsToFetch(
            QueryMetadata metadata,
            IndexDefinitionBase indexDefinition,
            IndexingConfiguration configuration,
            out bool anyExtractableFromIndex,
            out bool extractAllStoredFields,
            out bool singleFieldNoAlias)
        {
            anyExtractableFromIndex = false;
            extractAllStoredFields = false;
            singleFieldNoAlias = false;

            if (metadata.SelectFields == null || metadata.SelectFields.Length == 0)
                return null;

            if (metadata.SelectFields.Length == 1)
            {
                var selectField = metadata.SelectFields[0];
                singleFieldNoAlias = ((selectField.Alias == null && selectField.Function != null) || (selectField.Name == string.Empty && selectField.Function == null));
                if (singleFieldNoAlias && metadata.Query.From.Alias != null && metadata.Query.From.Alias == selectField.Alias)
                    return null; // from 'Index' as doc select doc -> we do not want to treat this as a projection
            }

            var result = new Dictionary<string, FieldToFetch>(StringComparer.Ordinal);

            for (var i = 0; i < metadata.SelectFields.Length; i++)
            {
                var selectField = metadata.SelectFields[i];
                var val = GetFieldToFetch(indexDefinition, selectField, result,
                    out var key, ref anyExtractableFromIndex, ref extractAllStoredFields);
                if (extractAllStoredFields)
                    return result;
                if (val == null)
                    continue;
                if (val.CanExtractFromIndex == false && metadata.IsDynamic == false && metadata.IsCollectionQuery == false && configuration.ThrowIfProjectedFieldCannotBeExtractedFromIndex)
                    throw new InvalidQueryException($"Field '{val.Name}' cannot be extracted from index");
                result[key] = val;
            }

            if (indexDefinition != null)
                anyExtractableFromIndex |= indexDefinition.HasDynamicFields;

            return result;
        }

        public bool ContainsField(string name)
        {
            return Fields == null || Fields.ContainsKey(name);
        }

        public class FieldToFetch
        {
            public FieldToFetch(string name, SelectField queryField, string projectedName, bool canExtractFromIndex, bool isDocumentId)
            {
                Name = name;
                QueryField = queryField;
                ProjectedName = projectedName;
                IsDocumentId = isDocumentId;
                CanExtractFromIndex = canExtractFromIndex;
            }

            public FieldToFetch(string projectedName, string[] components)
            {
                ProjectedName = projectedName;
                Components = components;
                IsCompositeField = true;
                CanExtractFromIndex = false;
            }

            public readonly StringSegment Name;

            public readonly SelectField QueryField;

            public readonly string ProjectedName;

            public readonly bool CanExtractFromIndex;

            public readonly bool IsCompositeField;

            public readonly bool IsDocumentId;

            public readonly string[] Components;

            public FieldToFetch[] FunctionArgs;
        }
    }
}
