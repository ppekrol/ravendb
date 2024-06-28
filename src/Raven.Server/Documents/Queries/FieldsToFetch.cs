﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Results;
using Sparrow;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents.Queries
{
    public class FieldsToFetch
    {
        public readonly Dictionary<string, FieldToFetch> Fields;

        public readonly Dictionary<string, IndexField> IndexFields;

        public readonly bool AnyDynamicIndexFields;

        public readonly bool ExtractAllFromIndex;

        public readonly bool AnyExtractableFromIndex;

        public readonly bool AnyTimeSeries;

        public readonly bool SingleBodyOrMethodWithNoAlias;

        public readonly bool IsProjection;

        public readonly bool IsDistinct;

        public readonly ProjectionOptions Projection;

        public FieldsToFetch(IndexQueryServerSide query, IndexDefinitionBaseServerSide indexDefinition, IndexType indexType, SearchEngineType searchEngineType = SearchEngineType.None)
        {
            Projection = new ProjectionOptions(query);
            Fields = GetFieldsToFetch(query.Metadata, query.ProjectionBehavior, indexDefinition, searchEngineType, indexType, out AnyExtractableFromIndex, out bool extractAllStoredFields, out SingleBodyOrMethodWithNoAlias, out AnyTimeSeries);
            IsProjection = Fields != null && Fields.Count > 0;
            IndexFields = indexDefinition?.IndexFields;
            AnyDynamicIndexFields = indexDefinition != null && indexDefinition.HasDynamicFields;

            if (extractAllStoredFields)
            {
                AnyExtractableFromIndex = true;
                ExtractAllFromIndex = true; // we want to add dynamic fields also to the result (stored only)
                IsProjection = true;
            }

            IsDistinct = query.Metadata.IsDistinct && IsProjection;
        }

        private static FieldToFetch GetFieldToFetch(
            IndexDefinitionBaseServerSide indexDefinition,
            QueryMetadata metadata,
            ProjectionBehavior? projectionBehavior,
            SelectField selectField,
            Dictionary<string, FieldToFetch> results,
            IndexType indexType,
            out string selectFieldKey,
            ref bool anyExtractableFromIndex,
            ref bool extractAllStoredFields,
            ref bool anyTimeSeries)
        {
            var mustExtractFromIndex = projectionBehavior.FromIndexOnly();
            var maybeExtractFromIndex = projectionBehavior.FromIndexOrDefault();
            
            selectFieldKey = selectField.Alias ?? selectField.Name;
            var selectFieldName = selectField.Name;

            if (selectField.ValueTokenType != null)
            {
                return new FieldToFetch(string.Empty, selectField, selectField.Alias,
                    canExtractFromIndex: false, isDocumentId: false, isTimeSeries: false);
            }

            if (selectField.Function != null)
            {
                var isTimeSeries = metadata.DeclaredFunctions != null && metadata.DeclaredFunctions.TryGetValue(selectField.Function, out var func) && func.Type == DeclaredFunction.FunctionType.TimeSeries;
                if (isTimeSeries)
                    anyTimeSeries = true;

                var fieldToFetch = new FieldToFetch(
                    selectField.Name,
                    selectField,
                    selectField.Alias,
                    canExtractFromIndex: false,
                    isDocumentId: false,
                    isTimeSeries: isTimeSeries)
                {
                    FunctionArgs = new FieldToFetch[selectField.FunctionArgs.Length]
                };

                for (int j = 0; j < selectField.FunctionArgs.Length; j++)
                {
                    var ignored = false;
                    fieldToFetch.FunctionArgs[j] = GetFieldToFetch(
                        indexDefinition,
                        metadata,
                        projectionBehavior,
                        selectField.FunctionArgs[j],
                        null,
                        indexType,
                        out _,
                        ref ignored,
                        ref ignored,
                        ref ignored
                    );
                }
                return fieldToFetch;
            }

            if (selectField.IsCounter)
            {
                var fieldToFetch = new FieldToFetch(selectField.Name, selectField, selectField.Alias ?? selectField.Name,
                    canExtractFromIndex: false, isDocumentId: false, isTimeSeries: false);
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
                return new FieldToFetch(selectFieldName, selectField, selectField.Alias, canExtractFromIndex: false, isDocumentId: false, isTimeSeries: false);
            }

            if (selectFieldName.Value.Length > 0)
            {
                if (selectFieldName == Constants.Documents.Indexing.Fields.DocumentIdFieldName)
                {
                    anyExtractableFromIndex = maybeExtractFromIndex;
                    
                    return new FieldToFetch(selectFieldName, selectField, selectField.Alias, 
                        canExtractFromIndex: (indexDefinition is MapReduceIndexDefinition or AutoMapReduceIndexDefinition), 
                        isDocumentId: true, isTimeSeries: false);
                }

                if (selectFieldName.Value[0] == '_')
                {
                    if (selectFieldName == Constants.Documents.Indexing.Fields.AllStoredFields)
                    {
                        if (results == null)
                            ThrowInvalidFetchAllStoredDocuments();
                        Debug.Assert(results != null);
                        results.Clear(); // __all_stored_fields should only return stored fields so we are ensuring that no other fields will be returned

                        extractAllStoredFields = maybeExtractFromIndex;

                        foreach (var kvp in indexDefinition.MapFields)
                        {
                            var stored = kvp.Value.Storage == FieldStorage.Yes;
                            if (stored == false)
                                continue;

                            anyExtractableFromIndex = maybeExtractFromIndex;
                            results[kvp.Key] = new FieldToFetch(kvp.Key, null, null, canExtractFromIndex: maybeExtractFromIndex, isDocumentId: false, isTimeSeries: false);
                        }

                        return null;
                    }
                }
            }

            var bySourceAlias = ShouldTryToExtractBySourceAliasName(selectFieldName.Value, selectField);
            var key = bySourceAlias
                    ? selectField.SourceAlias
                    : selectFieldName;

            var extract = mustExtractFromIndex || (maybeExtractFromIndex && indexDefinition.MapFields.TryGetValue(key, out var value) && (value.Storage == FieldStorage.Yes));

            if (extract)
                anyExtractableFromIndex = true;

            if (bySourceAlias == false && maybeExtractFromIndex)
            {
                extract |= indexDefinition.HasDynamicFields;
            }

            return new FieldToFetch(selectFieldName, selectField, selectField.Alias, extract, isDocumentId: false, isTimeSeries: false);
        }

        private static bool ShouldTryToExtractBySourceAliasName(string selectFieldName, SelectField selectField)
        {
            return selectField.HasSourceAlias &&
                   selectField.SourceAlias != null;
        }

        [DoesNotReturn]
        private static void ThrowInvalidFetchAllStoredDocuments()
        {
            throw new InvalidOperationException("Cannot fetch all stored path from a nested method");
        }

        private static Dictionary<string, FieldToFetch> GetFieldsToFetch(
            QueryMetadata metadata,
            ProjectionBehavior? projectionBehavior,
            IndexDefinitionBaseServerSide indexDefinition,
            SearchEngineType searchEngineType,
            IndexType indexType,
            out bool anyExtractableFromIndex,
            out bool extractAllStoredFields,
            out bool singleFieldNoAlias,
            out bool anyTimeSeries)
        {
            anyExtractableFromIndex = false;
            extractAllStoredFields = false;
            singleFieldNoAlias = false;
            anyTimeSeries = false;

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
                var val = GetFieldToFetch(indexDefinition, metadata, projectionBehavior, selectField, result, indexType,
                    out var key, ref anyExtractableFromIndex, ref extractAllStoredFields, ref anyTimeSeries);
                if (extractAllStoredFields)
                    return result;
                if (val == null)
                    continue;
                result[key] = val;
            }

            if (indexDefinition != null)
                anyExtractableFromIndex |= (indexDefinition.HasDynamicFields && projectionBehavior.FromIndexOrDefault());

            return result;
        }

        public bool ContainsField(string name)
        {
            return Fields == null || Fields.ContainsKey(name);
        }

        public class FieldToFetch
        {
            public FieldToFetch(string name, SelectField queryField, string projectedName, bool canExtractFromIndex, bool isDocumentId, bool isTimeSeries)
            {
                Name = name;
                QueryField = queryField;
                ProjectedName = projectedName;
                IsDocumentId = isDocumentId;
                CanExtractFromIndex = canExtractFromIndex;
                IsTimeSeries = isTimeSeries;
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

            public readonly bool IsTimeSeries;

            public readonly bool IsDocumentId;

            public readonly string[] Components;

            public FieldToFetch[] FunctionArgs;
        }
    }
}
