﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Formatting;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal class IndexHandlerProcessorForConvertAutoIndex<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler>
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    where TOperationContext : JsonOperationContext
{
    public IndexHandlerProcessorForConvertAutoIndex([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    private string GetName() => RequestHandler.GetStringQueryString("name", required: true);

    private ConvertType GetConvertType()
    {
        var typeAsString = RequestHandler.GetStringQueryString("type", required: true);

        if (Enum.TryParse(typeAsString, ignoreCase: true, out ConvertType convertType) == false)
            throw new InvalidOperationException($"Could not parse '{typeAsString}' to any known conversion type.");

        return convertType;
    }

    public override async ValueTask ExecuteAsync()
    {
        var name = GetName();
        var type = GetConvertType();

        using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            var record = RequestHandler.ServerStore.Cluster.ReadRawDatabaseRecord(context, RequestHandler.DatabaseName);
            if (record.AutoIndexes.TryGetValue(name, out var autoIndex) == false)
                throw IndexDoesNotExistException.ThrowForAuto(name);

            switch (type)
            {
                case ConvertType.Csharp:
                    var result = AutoToStaticIndexConverter.Instance.ConvertToAbstractIndexCreationTask(autoIndex, out var csharpClassName);

                    csharpClassName = $"{csharpClassName}.cs";

                    HttpContext.Response.Headers[Constants.Headers.ContentDisposition] = $"attachment; filename=\"{csharpClassName}\"; filename*=UTF-8''{csharpClassName}";

                    await using (var writer = new StreamWriter(RequestHandler.ResponseBodyStream()))
                    {
                        await writer.WriteLineAsync(result);
                    }
                    break;
                case ConvertType.Export:
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                    {
                        var definition = AutoToStaticIndexConverter.Instance.ConvertToIndexDefinition(autoIndex);

                        writer.WriteStartObject();

                        writer.WritePropertyName("Indexes");
                        writer.WriteStartArray();

                        writer.WriteIndexDefinition(context, definition);

                        writer.WriteEndArray();

                        writer.WriteEndObject();
                    }
                    break;
                default:
                    throw new ArgumentOutOfRangeException();

            }
        }
    }

    private enum ConvertType
    {
        Csharp,
        Export
    }
}

public class AutoToStaticIndexConverter
{
    public static AutoToStaticIndexConverter Instance = new();

    private AutoToStaticIndexConverter()
    {

    }

    public IndexDefinition ConvertToIndexDefinition(AutoIndexDefinition autoIndex)
    {
        if (autoIndex == null)
            throw new ArgumentNullException(nameof(autoIndex));

        var context = new AutoIndexConversionContext();

        var indexDefinition = new IndexDefinition();
        indexDefinition.Name = GenerateName(autoIndex.Name);

        indexDefinition.Maps = ConstructMaps(autoIndex, context);
        indexDefinition.Reduce = ConstructReduce(autoIndex);
        indexDefinition.Fields = ConstructFields(autoIndex, context);

        return indexDefinition;

        static string GenerateName(string name)
        {
            name = name[AutoIndexNameFinder.AutoIndexPrefix.Length..];
            name = Regex.Replace(name, @"[^\w\d]", "/");

            while (true)
            {
                var newName = name.Replace("//", "/");
                if (newName == name)
                    break;

                name = newName;
            }

            return "Index/" + name;
        }

        static HashSet<string> ConstructMaps(AutoIndexDefinition autoIndex, AutoIndexConversionContext context)
        {
            var sb = new StringBuilder();
            sb
                .AppendLine($"from item in docs.{autoIndex.Collection}")
                .AppendLine("select new")
                .AppendLine("{");

            HandleMapFields(sb, autoIndex, context);

            sb.AppendLine("};");

            return [sb.ToString()];
        }

        static string ConstructReduce(AutoIndexDefinition autoIndex)
        {
            if (autoIndex.GroupByFields == null || autoIndex.GroupByFields.Count == 0)
                return null;

            var sb = new StringBuilder();

            ConstructReduceInternal(sb, autoIndex);

            return sb.ToString();
        }

        static Dictionary<string, IndexFieldOptions> ConstructFields(AutoIndexDefinition autoIndex, AutoIndexConversionContext context)
        {
            Dictionary<string, IndexFieldOptions> fields = null;
            if (autoIndex.MapFields is { Count: > 0 })
            {
                foreach (var kvp in autoIndex.MapFields)
                {
                    var fieldNames = GenerateFieldName(kvp.Key, kvp.Value.Indexing);

                    foreach (var f in fieldNames)
                    {
                        HandleFieldIndexing(f.FieldName, f.Indexing);
                        HandleSpatial(f.FieldName, kvp.Value.Spatial, context);
                        HandleStorage(f.FieldName, kvp.Value.Storage);
                        HandleSuggestions(f.FieldName, kvp.Value.Suggestions);
                    }
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
                if (fieldStorage.HasValue == false || fieldStorage == FieldStorage.No)
                    return;

                var options = GetFieldOptions(fieldName);

                switch (fieldStorage.Value)
                {
                    case FieldStorage.Yes:
                        options.Storage = FieldStorage.Yes;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }

            void HandleSuggestions(string fieldName, bool? suggestions)
            {
                if (suggestions.HasValue == false || suggestions == false)
                    return;

                throw new NotImplementedException();
            }

            void HandleSpatial(string fieldName, AutoSpatialOptions spatialOptions, AutoIndexConversionContext context)
            {
                if (spatialOptions == null)
                    return;

                var realFieldName = context.FieldNameMapping[fieldName];

                var options = GetFieldOptions(realFieldName);
                options.Spatial = spatialOptions;
            }
        }
    }

    public string ConvertToAbstractIndexCreationTask(AutoIndexDefinition autoIndex, out string csharpClassName)
    {
        if (autoIndex == null)
            throw new ArgumentNullException(nameof(autoIndex));

        var context = new AutoIndexConversionContext();

        var sb = new StringBuilder();

        csharpClassName = GenerateClassName(autoIndex);
        var className = Inflector.Singularize(autoIndex.Collection);

        sb
            .Append($"public class {csharpClassName} : {typeof(AbstractIndexCreationTask).FullName}<{className}");

        if (autoIndex.Type == IndexType.AutoMapReduce)
            sb.Append($", {csharpClassName}.Result");

        sb
            .AppendLine(">")
            .AppendLine("{")
            .AppendLine($"public {csharpClassName}()")
            .AppendLine("{");

        ConstructMap(autoIndex, sb, context);
        ConstructReduce(autoIndex, sb);
        ConstructFieldOptions(autoIndex, sb, context);

        sb
            .AppendLine("}");

        if (autoIndex.Type == IndexType.AutoMapReduce)
        {
            sb
                .AppendLine()
                .AppendLine("public class Result")
                .AppendLine("{");

            foreach (var kvp in autoIndex.MapFields)
            {
                var fieldNames = GenerateFieldName(kvp.Key, kvp.Value.Indexing);

                foreach (var f in fieldNames)
                {
                    switch (kvp.Value.Aggregation)
                    {
                        case AggregationOperation.Sum:
                        case AggregationOperation.Count:
                        {
                            sb.AppendLine($"public int {f.FieldName} {{ get; set; }}");
                            break;
                        }
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
            }

            foreach (var kvp in autoIndex.GroupByFields)
            {
                var fieldNames = GenerateFieldName(kvp.Key, kvp.Value.Indexing);

                foreach (var f in fieldNames)
                {
                    sb.AppendLine($"public object {f.FieldName} {{ get; set; }}");
                }
            }

            sb.AppendLine("}");
        }

        sb
            .AppendLine("}");

        using (var workspace = new AdhocWorkspace())
        {
            var syntaxTree = SyntaxFactory
                .ParseSyntaxTree(sb.ToString());

            var result = Formatter.Format(syntaxTree.GetRoot(), workspace);
            return result.ToString();
        }


        static void ConstructMap(AutoIndexDefinition autoIndex, StringBuilder sb, AutoIndexConversionContext context)
        {
            sb
                .AppendLine("Map = items => from item in items")
                .AppendLine("select new")
                .AppendLine("{");

            HandleMapFields(sb, autoIndex, context);

            sb.AppendLine("};");
        }

        static void ConstructReduce(AutoIndexDefinition autoIndex, StringBuilder sb)
        {
            if (autoIndex.GroupByFields == null || autoIndex.GroupByFields.Count == 0)
                return;

            sb
                .AppendLine()
                .Append("Reduce = results => ");

            ConstructReduceInternal(sb, autoIndex);
        }

        static void ConstructFieldOptions(AutoIndexDefinition autoIndex, StringBuilder sb, AutoIndexConversionContext context)
        {
            sb.AppendLine();
            foreach (var kvp in autoIndex.MapFields)
            {
                var fieldNames = GenerateFieldName(kvp.Key, kvp.Value.Indexing);

                foreach (var f in fieldNames)
                {
                    HandleFieldIndexing(f.FieldName, f.Indexing);
                    HandleStorage(f.FieldName, kvp.Value.Storage);
                    HandleSuggestions(f.FieldName, kvp.Value.Suggestions);
                    HandleSpatial(f.FieldName, kvp.Value.Spatial, context);
                }
            }

            return;

            void HandleFieldIndexing(string fieldName, AutoFieldIndexing? indexing)
            {
                if (indexing.HasValue == false)
                    return;

                if (indexing.Value.HasFlag(AutoFieldIndexing.Search))
                {
                    sb.AppendLine($"Index(\"{fieldName}\", {typeof(FieldIndexing).FullName}.{nameof(FieldIndexing.Search)});");

                    if (indexing.Value.HasFlag(AutoFieldIndexing.Highlighting))
                        sb.AppendLine($"TermVector(\"{fieldName}\", {typeof(FieldTermVector).FullName}.{nameof(FieldTermVector.WithPositionsAndOffsets)});");
                }

                if (indexing.Value.HasFlag(AutoFieldIndexing.Exact))
                    sb.AppendLine($"Index(\"{fieldName}\", {typeof(FieldIndexing).FullName}.{nameof(FieldIndexing.Exact)});");
            }

            void HandleStorage(string fieldName, FieldStorage? fieldStorage)
            {
                if (fieldStorage.HasValue == false || fieldStorage == FieldStorage.No)
                    return;

                sb.AppendLine($"Store(\"{fieldName}\", {typeof(FieldStorage).FullName}.{nameof(FieldStorage.Yes)});");
            }

            void HandleSuggestions(string fieldName, bool? suggestions)
            {
                if (suggestions.HasValue == false || suggestions == false)
                    return;

                throw new NotImplementedException();
            }

            void HandleSpatial(string fieldName, AutoSpatialOptions spatial, AutoIndexConversionContext context)
            {
                if (spatial == null)
                    return;

                var realFieldName = context.FieldNameMapping[fieldName];

                sb.Append($"Spatial(\"{realFieldName}\", factory => factory.{spatial.Type}.");

                switch (spatial.Type)
                {
                    case SpatialFieldType.Cartesian:

                        switch (spatial.Strategy)
                        {
                            case SpatialSearchStrategy.QuadPrefixTree:
                                sb.Append($"{nameof(CartesianSpatialOptionsFactory.QuadPrefixTreeIndex)}({spatial.MaxTreeLevel}, new {nameof(SpatialBounds)} {{ {nameof(SpatialBounds.MaxX)} = {spatial.MaxX}, {nameof(SpatialBounds.MaxY)} = {spatial.MaxY}, {nameof(SpatialBounds.MinX)} = {spatial.MinX}, {nameof(SpatialBounds.MinY)} = {spatial.MinY} }})");
                                break;
                            case SpatialSearchStrategy.BoundingBox:
                                sb.Append($"{nameof(CartesianSpatialOptionsFactory.BoundingBoxIndex)}()");
                                break;
                            default:
                                throw new ArgumentOutOfRangeException();
                        }

                        break;
                    case SpatialFieldType.Geography:

                        switch (spatial.Strategy)
                        {
                            case SpatialSearchStrategy.QuadPrefixTree:
                                sb.Append($"{nameof(GeographySpatialOptionsFactory.QuadPrefixTreeIndex)}({spatial.MaxTreeLevel}, {typeof(SpatialUnits).FullName}.{spatial.Units})");
                                break;
                            case SpatialSearchStrategy.GeohashPrefixTree:
                                sb.Append($"{nameof(GeographySpatialOptionsFactory.GeohashPrefixTreeIndex)}({spatial.MaxTreeLevel}, {typeof(SpatialUnits).FullName}.{spatial.Units})");
                                break;
                            case SpatialSearchStrategy.BoundingBox:
                                sb.Append($"{nameof(GeographySpatialOptionsFactory.BoundingBoxIndex)}({typeof(SpatialUnits).FullName}.{spatial.Units})");
                                break;
                            default:
                                throw new ArgumentOutOfRangeException();
                        }

                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }

                sb.AppendLine(");");
            }
        }

        static string GenerateClassName(AutoIndexDefinition autoIndex)
        {
            var name = autoIndex.Name[AutoIndexNameFinder.AutoIndexPrefix.Length..];
            name = Regex.Replace(name, @"[^\w\d]", "_");
            return "Index_" + name;
        }
    }

    private static void HandleMapFields(StringBuilder sb, AutoIndexDefinition autoIndex, AutoIndexConversionContext context)
    {
        var countOfFields = autoIndex.MapFields.Count + autoIndex.GroupByFields.Count;
        if (countOfFields == 0)
            throw new NotSupportedException("Cannot convert auto index with 0 fields");

        var spatialCounter = 0;
        foreach (var kvp in autoIndex.MapFields)
        {
            var fieldNames = GenerateFieldName(kvp.Key, kvp.Value.Indexing);

            foreach (var f in fieldNames)
            {
                if (f.FieldName.Contains("[]"))
                    throw new NotSupportedException($"Invalid field name '{f.FieldName}'.");

                if (kvp.Value.Spatial == null)
                {
                    switch (kvp.Value.Aggregation)
                    {
                        case AggregationOperation.None:
                            {
                                var fieldPath = $"item.{kvp.Key}";

                                sb.AppendLine($"{f.FieldName} = {fieldPath},");
                                break;
                            }
                        case AggregationOperation.Count:
                        case AggregationOperation.Sum:
                            sb.AppendLine($"{f.FieldName} = 1,");
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
                else
                {
                    var newFieldName = spatialCounter == 0 ? "Coordinates" : $"Coordinates{spatialCounter}";
                    context.FieldNameMapping.Add(f.FieldName, newFieldName);

                    switch (kvp.Value.Spatial.MethodType)
                    {
                        case AutoSpatialOptions.AutoSpatialMethodType.Point:
                        case AutoSpatialOptions.AutoSpatialMethodType.Wkt:
                            sb.AppendLine($"{newFieldName} = {nameof(AbstractIndexCreationTask.CreateSpatialField)}({string.Join(", ", kvp.Value.Spatial.MethodArguments.Select(x => $"item.{x}"))}),");
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }

                    spatialCounter++;
                }
            }
        }

        if (autoIndex.Type == IndexType.AutoMapReduce)
        {
            foreach (var kvp in autoIndex.GroupByFields)
            {
                var fieldNames = GenerateFieldName(kvp.Key, kvp.Value.Indexing);
                foreach (var f in fieldNames)
                {
                    var fieldPath = $"item.{kvp.Key}";

                    sb.AppendLine($"{f.FieldName} = {fieldPath},");
                }
            }
        }
    }

    private static void ConstructReduceInternal(StringBuilder sb, AutoIndexDefinition autoIndex)
    {
        sb
            .AppendLine("from result in results")
            .AppendLine($"group result by new {{ {GenerateGroupBy(autoIndex)} }} into g")
            .AppendLine("select new")
            .AppendLine("{");

        foreach (var kvp in autoIndex.MapFields)
        {
            var fieldNames = GenerateFieldName(kvp.Key, kvp.Value.Indexing);

            foreach (var f in fieldNames)
            {
                switch (kvp.Value.Aggregation)
                {
                    case AggregationOperation.Count:
                        {
                            var fieldPath = $"g.Sum(x => x.{kvp.Key})";

                            sb.AppendLine($"{f.FieldName} = {fieldPath},");
                            break;
                        }
                    case AggregationOperation.Sum:
                        {
                            var fieldPath = $"g.Sum(x => x.{kvp.Key})";

                            sb.AppendLine($"{f.FieldName} = {fieldPath},");
                            break;
                        }
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
        }

        foreach (var kvp in autoIndex.GroupByFields)
        {
            var fieldNames = GenerateFieldName(kvp.Key, kvp.Value.Indexing);

            foreach (var f in fieldNames)
            {
                var fieldPath = $"g.Key.{f.FieldName}";

                sb.AppendLine($"{f.FieldName} = {fieldPath},");
            }
        }

        sb.AppendLine("};");

        return;

        static string GenerateGroupBy(AutoIndexDefinition autoIndex)
        {
            return string.Join(", ", autoIndex.GroupByFieldNames.Select(x => "result." + GenerateFieldName(x, indexing: null).Single().FieldName));
        }
    }

    private static IEnumerable<(string FieldName, AutoFieldIndexing Indexing)> GenerateFieldName(string name, AutoFieldIndexing? indexing)
    {
        name = name
            .Replace("@", "")
            .Replace("-", "_")
            .Replace(".", "_");

        if (indexing.HasValue == false)
        {
            yield return (name, AutoFieldIndexing.Default);
            yield break;
        }

        if (indexing.Value.HasFlag(AutoFieldIndexing.No))
            yield return (name, AutoFieldIndexing.No);
        else
            yield return (name, AutoFieldIndexing.Default);

        if (indexing.Value.HasFlag(AutoFieldIndexing.Search))
        {
            if (indexing.Value.HasFlag(AutoFieldIndexing.Highlighting))
                yield return ($"{name}_Search", AutoFieldIndexing.Search | AutoFieldIndexing.Highlighting);
            else
                yield return ($"{name}_Search", AutoFieldIndexing.Search);
        }

        if (indexing.Value.HasFlag(AutoFieldIndexing.Exact))
            yield return ($"{name}_Exact", AutoFieldIndexing.Exact);
    }

    public class AutoIndexConversionContext
    {
        public Dictionary<string, string> FieldNameMapping = new Dictionary<string, string>();
    }
}
