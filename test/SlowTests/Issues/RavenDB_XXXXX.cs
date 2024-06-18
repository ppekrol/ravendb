using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using FastTests;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Formatting;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.Indexes;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_XXXXX : RavenTestBase
{
    public RavenDB_XXXXX(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void T1()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Query<Order>()
                    .Where(x => x.Employee == "HR" && x.ShipTo.City == "NY")
                    .Search(x => x.Company, "abc")
                    .ToList();
            }

            var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));

            var autoIndex = record.AutoIndexes.Values.First();

            var result = AutoToStaticIndexConverter.ConvertToAbstractIndexCreationTask(autoIndex);

            var def = AutoToStaticIndexConverter.ConvertToIndexDefinition(autoIndex);
        }
    }

    public static class AutoToStaticIndexConverter
    {
        public static IndexDefinition ConvertToIndexDefinition(AutoIndexDefinition autoIndex)
        {
            if (autoIndex == null)
                throw new ArgumentNullException(nameof(autoIndex));

            var indexDefinition = new IndexDefinition();
            indexDefinition.Name = $"Index/{autoIndex.Name[AutoIndexNameFinder.AutoIndexPrefix.Length..]}";

            indexDefinition.Maps = ConstructMaps(autoIndex);
            indexDefinition.Reduce = ConstructReduce(autoIndex);
            indexDefinition.Fields = ConstructFields(autoIndex);

            return indexDefinition;

            static HashSet<string> ConstructMaps(AutoIndexDefinition autoIndex)
            {
                var itemName = Inflector.Singularize(autoIndex.Collection);
                var sb = new StringBuilder();
                sb
                    .AppendLine($"from item in docs.{autoIndex.Collection}")
                    .AppendLine("select new")
                    .AppendLine("{");

                foreach (var kvp in autoIndex.MapFields)
                {
                    var fieldName = GenerateFieldName(kvp.Key);
                    var fieldPath = $"{itemName}.{kvp.Key}";

                    sb.AppendLine($"    {fieldName} = {fieldPath},");
                }

                sb.AppendLine("};");

                return [sb.ToString()];
            }

            static string ConstructReduce(AutoIndexDefinition autoIndex)
            {
                if (autoIndex.GroupByFields == null || autoIndex.GroupByFields.Count == 0)
                    return null;

                throw new NotImplementedException();
            }

            static Dictionary<string, IndexFieldOptions> ConstructFields(AutoIndexDefinition autoIndex)
            {
                Dictionary<string, IndexFieldOptions> fields = null;
                if (autoIndex.MapFields is { Count: > 0 })
                {
                    foreach (var kvp in autoIndex.MapFields)
                    {
                        var fieldName = GenerateFieldName(kvp.Key);

                        HandleFieldIndexing(fieldName, kvp.Value.Indexing);
                        HandleSpatial(fieldName, kvp.Value.Spatial);
                        HandleStorage(fieldName, kvp.Value.Storage);
                        HandleSuggestions(fieldName, kvp.Value.Suggestions);
                    }
                }

                return fields;

                IndexFieldOptions GetFieldOptions(string fieldName)
                {
                    fields ??= new Dictionary<string, IndexFieldOptions>();
                    if (fields.TryGetValue(fieldName, out var value) == false)
                        fields[fieldName] = value = new IndexFieldOptions();

                    return value;
                }

                void HandleFieldIndexing(string fieldName, AutoFieldIndexing? fieldIndexing)
                {
                    if (fieldIndexing.HasValue == false)
                        return;

                    if (fieldIndexing.Value.HasFlag(AutoFieldIndexing.Search))
                    {
                        var options = GetFieldOptions(fieldName);
                        options.Indexing = FieldIndexing.Search;
                        return;
                    }

                    if (fieldIndexing.Value.HasFlag(AutoFieldIndexing.Exact))
                    {
                        var options = GetFieldOptions(fieldName);
                        options.Indexing = FieldIndexing.Exact;
                        return;
                    }

                    if (fieldIndexing.Value.HasFlag(AutoFieldIndexing.Highlighting))
                    {
                        var options = GetFieldOptions(fieldName);
                        options.Indexing = FieldIndexing.Search;
                        options.Storage = FieldStorage.Yes;
                        options.TermVector = FieldTermVector.WithPositionsAndOffsets;
                        return;
                    }
                }

                void HandleStorage(string fieldName, FieldStorage? fieldStorage)
                {
                    if (fieldStorage.HasValue == false)
                        return;

                    var options = GetFieldOptions(fieldName);

                    switch (fieldStorage.Value)
                    {
                        case FieldStorage.Yes:
                            options.Storage = FieldStorage.Yes;
                            break;
                        case FieldStorage.No:
                            options.Storage = FieldStorage.No;
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }

                void HandleSuggestions(string fieldName, bool? suggestions)
                {
                    if (suggestions.HasValue == false)
                        return;

                    throw new NotSupportedException();
                }

                void HandleSpatial(string fieldName, AutoSpatialOptions spatialOptions)
                {
                    if (spatialOptions == null)
                        return;

                    throw new NotSupportedException();
                }
            }
        }

        public static string ConvertToAbstractIndexCreationTask(AutoIndexDefinition autoIndex)
        {
            if (autoIndex == null)
                throw new ArgumentNullException(nameof(autoIndex));

            var sb = new StringBuilder();

            var name = GenerateClassName(autoIndex);
            var className = Inflector.Singularize(autoIndex.Collection);

            sb
                .AppendLine($"public class {name} : {typeof(AbstractIndexCreationTask).FullName}<{className}>") // TODO handle reduce
                .AppendLine("{")
                .AppendLine($"public {name}()")
                .AppendLine("{");

            ConstructMap(autoIndex, sb);
            ConstructReduce(autoIndex, sb);
            ConstructFieldOptions(autoIndex, sb);

            sb
            .AppendLine("}")
            .AppendLine("}");

            using (var workspace = new AdhocWorkspace())
            {
                var syntaxTree = SyntaxFactory
                    .ParseSyntaxTree(sb.ToString());

                var result = Formatter.Format(syntaxTree.GetRoot(), workspace);
                return result.ToString();
            }


            static void ConstructMap(AutoIndexDefinition autoIndex, StringBuilder sb)
            {
                sb
                    .AppendLine("Map = items => from item in items")
                    .AppendLine("select new")
                    .AppendLine("{");

                foreach (var kvp in autoIndex.MapFields)
                {
                    var fieldName = GenerateFieldName(kvp.Key);
                    var fieldPath = $"item.{kvp.Key}";

                    sb.AppendLine($"{fieldName} = {fieldPath},");
                }

                sb.AppendLine("};");
            }

            static void ConstructReduce(AutoIndexDefinition autoIndex, StringBuilder sb)
            {
                if (autoIndex.GroupByFields == null || autoIndex.GroupByFields.Count == 0)
                    return;

                sb
                    .AppendLine("Reduce = results => from result in results")
                    .AppendLine($"group result by {GenerateGroupBy(autoIndex)} into g")
                    .AppendLine("select new")
                    .AppendLine("{");

                foreach (var kvp in autoIndex.GroupByFields)
                {
                    var fieldName = GenerateFieldName(kvp.Key);
                    var fieldPath = $"item.{kvp.Key}";

                    sb.AppendLine($"{fieldName} = {fieldPath},");
                }

                sb.AppendLine("};");

                return;

                static string GenerateGroupBy(AutoIndexDefinition autoIndex)
                {
                    if (autoIndex.GroupByFieldNames.Count == 1)
                        return $"result.{autoIndex.GroupByFieldNames[0]}";

                    return null;
                }
            }

            static void ConstructFieldOptions(AutoIndexDefinition autoIndex, StringBuilder sb)
            {
                sb.AppendLine();
                foreach (var kvp in autoIndex.MapFields)
                {
                    var fieldName = GenerateFieldName(kvp.Key);
                    var indexing = kvp.Value.Indexing;
                    if (indexing.HasValue && (indexing.Value & AutoFieldIndexing.Search) == AutoFieldIndexing.Search)
                        sb.AppendLine($"Index(\"{fieldName}\", {typeof(FieldIndexing).FullName}.{nameof(FieldIndexing.Search)});");
                }
            }

            static string GenerateClassName(AutoIndexDefinition autoIndex)
            {
                var name = autoIndex.Name[AutoIndexNameFinder.AutoIndexPrefix.Length..];
                name = Regex.Replace(name, @"[^\w\d]", "_");
                return "Index_" + name;
            }
        }

        private static string GenerateFieldName(string name)
        {
            return name
                .Replace(".", "_");
        }
    }
}
