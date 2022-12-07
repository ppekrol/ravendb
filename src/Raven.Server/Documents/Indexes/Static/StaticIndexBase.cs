﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using Lucene.Net.Documents;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Sparrow.Json;
using Spatial4n.Shapes;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes.Configuration;
using Esprima;
using Raven.Client.ServerWide.JavaScript;
using Raven.Server.Documents.Indexes.Static.Counters;
using Raven.Server.Documents.Indexes.Static.TimeSeries;

namespace Raven.Server.Documents.Indexes.Static
{
    public delegate IEnumerable IndexingFunc(IEnumerable<dynamic> items);

    public abstract class StaticCountersIndexBase : StaticCountersAndTimeSeriesIndexBase
    {
    }

    public abstract class StaticTimeSeriesIndexBase : StaticCountersAndTimeSeriesIndexBase
    {
    }

    public abstract class StaticCountersAndTimeSeriesIndexBase : AbstractStaticIndexBase
    {
        public void AddMap(string collection, string name, IndexingFunc map)
        {
            AddMapInternal(collection, name, map);
        }
    }

    public abstract class StaticIndexBase : AbstractStaticIndexBase
    {
        public void AddMap(string collection, IndexingFunc map)
        {
            AddMapInternal(collection, collection, map);
        }

        public dynamic LoadAttachments(dynamic doc)
        {
            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException($"Indexing scope was not initialized.");

            if (doc is DynamicNullObject)
                return DynamicNullObject.Null;

            var document = doc as DynamicBlittableJson;
            if (document == null)
                throw new InvalidOperationException($"{nameof(LoadAttachments)} may only be called with a non-null entity as a parameter, but was called with a parameter of type {doc.GetType().FullName}: {doc}");

            return CurrentIndexingScope.Current.LoadAttachments(document);
        }

        public dynamic LoadAttachment(dynamic doc, object attachmentName)
        {
            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException($"Indexing scope was not initialized. Attachment Name: {attachmentName}");

            if (doc is DynamicNullObject)
                return DynamicNullObject.Null;

            var document = doc as DynamicBlittableJson;
            if (document == null)
                throw new InvalidOperationException($"{nameof(LoadAttachment)} may only be called with a non-null entity as a first parameter, but was called with a parameter of type {doc.GetType().FullName}: {doc}");

            if (attachmentName is LazyStringValue attachmentNameLazy)
                return CurrentIndexingScope.Current.LoadAttachment(document, attachmentNameLazy);

            if (attachmentName is string attachmentNameString)
                return CurrentIndexingScope.Current.LoadAttachment(document, attachmentNameString);

            if (attachmentName is DynamicNullObject)
                return DynamicNullObject.Null;

            throw new InvalidOperationException($"{nameof(LoadAttachment)} may only be called with a string, but was called with a parameter of type {attachmentName.GetType().FullName}: {attachmentName}");
        }

        public dynamic Id(dynamic doc)
        {
            if (doc is DynamicBlittableJson json)
                return json.GetId();

            if (doc is DynamicNullObject)
                return doc;

            ThrowInvalidDocType(doc, nameof(Id));

            // never hit
            return null;
        }

        public dynamic MetadataFor(dynamic doc)
        {
            if (doc is DynamicBlittableJson json)
            {
                json.EnsureMetadata();
                return doc[Constants.Documents.Metadata.Key];
            }

            if (doc is DynamicNullObject)
                return doc;

            ThrowInvalidDocType(doc, nameof(MetadataFor));

            // never hit
            return null;
        }

        public dynamic AsJson(dynamic doc)
        {
            if (doc is DynamicBlittableJson json)
            {
                json.EnsureMetadata();
                return json;
            }

            if (doc is DynamicNullObject)
                return doc;

            ThrowInvalidDocType(doc, nameof(AsJson));

            // never hit
            return null;
        }
        
        public dynamic AttachmentsFor(dynamic doc)
        {
            var metadata = MetadataFor(doc);
            var attachments = metadata is DynamicNullObject
                ? null : metadata[Constants.Documents.Metadata.Attachments];

            return attachments != null
                ? attachments
                : new DynamicArray(Enumerable.Empty<object>());
        }

        public dynamic CounterNamesFor(dynamic doc)
        {
            var metadata = MetadataFor(doc);
            var counters = metadata is DynamicNullObject
                ? null : metadata[Constants.Documents.Metadata.Counters];

            return counters != null
                ? counters
                : new DynamicArray(Enumerable.Empty<object>());
        }

        public dynamic TimeSeriesNamesFor(dynamic doc)
        {
            var metadata = MetadataFor(doc);
            var timeSeries = metadata is DynamicNullObject
                ? null : metadata[Constants.Documents.Metadata.TimeSeries];

            return timeSeries != null
                ? timeSeries
                : new DynamicArray(Enumerable.Empty<object>());
        }

        private static void ThrowInvalidDocType(dynamic doc, string funcName)
        {
            throw new InvalidOperationException(
                $"{funcName} may only be called with a document, " +
                $"but was called with a parameter of type {doc?.GetType().FullName}: {doc}");
        }
    }

    public abstract class AbstractStaticIndexBase
    {
        protected readonly Dictionary<string, CollectionName> _collectionsCache = new Dictionary<string, CollectionName>(StringComparer.OrdinalIgnoreCase);

        public readonly Dictionary<string, Dictionary<string, List<IndexingFunc>>> Maps = new Dictionary<string, Dictionary<string, List<IndexingFunc>>>();

        public readonly Dictionary<string, HashSet<CollectionName>> ReferencedCollections = new Dictionary<string, HashSet<CollectionName>>();

        public readonly HashSet<string> CollectionsWithCompareExchangeReferences = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        public bool HasDynamicFields { get; set; }

        public bool HasBoostedFields { get; set; }

        public string Source;

        public IndexingFunc Reduce;

        public string[] OutputFields;

        public CompiledIndexField[] GroupByFields;

        public void AddCompareExchangeReferenceToCollection(string collection)
        {
            if (collection is null)
                throw new ArgumentNullException(nameof(collection));

            CollectionsWithCompareExchangeReferences.Add(collection);
        }

        public void AddReferencedCollection(string collection, string referencedCollection)
        {
            if (_collectionsCache.TryGetValue(referencedCollection, out CollectionName referencedCollectionName) == false)
                _collectionsCache[referencedCollection] = referencedCollectionName = new CollectionName(referencedCollection);

            if (ReferencedCollections.TryGetValue(collection, out HashSet<CollectionName> set) == false)
                ReferencedCollections[collection] = set = new HashSet<CollectionName>();

            set.Add(referencedCollectionName);
        }

        protected void AddMapInternal(string collection, string subCollecction, IndexingFunc map)
        {
            if (Maps.TryGetValue(collection, out Dictionary<string, List<IndexingFunc>> collections) == false)
                Maps[collection] = collections = new Dictionary<string, List<IndexingFunc>>();

            if (collections.TryGetValue(subCollecction, out var funcs) == false)
                collections[subCollecction] = funcs = new List<IndexingFunc>();

            funcs.Add(map);
        }

        protected dynamic TryConvert<T>(object value)
            where T : struct
        {
            if (value == null || value is DynamicNullObject)
                return DynamicNullObject.Null;

            var type = typeof(T);
            if (type == typeof(double) || type == typeof(float))
            {
                var dbl = TryConvertToDouble(value);
                if (dbl.HasValue == false)
                    return DynamicNullObject.Null;

                if (type == typeof(float))
                    return (T)(object)Convert.ToSingle(dbl.Value);

                return (T)(object)dbl.Value;
            }

            if (type == typeof(long) || type == typeof(int))
            {
                var lng = TryConvertToLong(value);
                if (lng.HasValue == false)
                    return DynamicNullObject.Null;

                if (type == typeof(int))
                    return (T)(object)Convert.ToInt32(lng.Value);

                return (T)(object)lng.Value;
            }

            return DynamicNullObject.Null;

            static double? TryConvertToDouble(object v)
            {
                if (v is double d)
                    return d;
                if (v is LazyNumberValue lnv)
                    return lnv;
                if (v is int i)
                    return i;
                if (v is long l)
                    return l;
                if (v is float f)
                    return f;
                if (v is LazyStringValue lsv && double.TryParse(lsv, out var r))
                    return r;

                return null;
            }

            static long? TryConvertToLong(object v)
            {
                if (v is double d)
                    return (long)d;
                if (v is LazyNumberValue lnv)
                    return lnv;
                if (v is int i)
                    return i;
                if (v is long l)
                    return l;
                if (v is float f)
                    return (long)f;
                if (v is LazyStringValue lsv && long.TryParse(lsv, out var r))
                    return r;

                return null;
            }
        }

        public dynamic LoadDocument<TIgnored>(object keyOrEnumerable, string collectionName)
        {
            return LoadDocument(keyOrEnumerable, collectionName);
        }

        public dynamic LoadDocument(object keyOrEnumerable, string collectionName)
        {
            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException(
                    "Indexing scope was not initialized. Key: " + keyOrEnumerable);

            if (keyOrEnumerable is LazyStringValue keyLazy)
                return CurrentIndexingScope.Current.LoadDocument(keyLazy, null, collectionName);

            if (keyOrEnumerable is string keyString)
                return CurrentIndexingScope.Current.LoadDocument(null, keyString, collectionName);

            if (keyOrEnumerable is DynamicNullObject || keyOrEnumerable is null)
                return DynamicNullObject.Null;

            if (keyOrEnumerable is IEnumerable enumerable)
            {
                var enumerator = enumerable.GetEnumerator();
                using (enumerable as IDisposable)
                {
                    var items = new List<dynamic>();
                    while (enumerator.MoveNext())
                    {
                        items.Add(LoadDocument(enumerator.Current, collectionName));
                    }
                    if (items.Count == 0)
                        return DynamicNullObject.Null;

                    return new DynamicArray(items);
                }
            }

            throw new InvalidOperationException(
                "LoadDocument may only be called with a string or an enumerable, but was called with a parameter of type " +
                keyOrEnumerable.GetType().FullName + ": " + keyOrEnumerable);
        }

        public dynamic LoadCompareExchangeValue<TIgnored>(object keyOrEnumerable)
        {
            return LoadCompareExchangeValue(keyOrEnumerable);
        }

        public dynamic LoadCompareExchangeValue(object keyOrEnumerable)
        {
            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException(
                    "Indexing scope was not initialized. Key: " + keyOrEnumerable);

            if (keyOrEnumerable is LazyStringValue keyLazy)
                return CurrentIndexingScope.Current.LoadCompareExchangeValue(keyLazy, null);

            if (keyOrEnumerable is string keyString)
                return CurrentIndexingScope.Current.LoadCompareExchangeValue(null, keyString);

            if (keyOrEnumerable is DynamicNullObject)
                return DynamicNullObject.Null;

            if (keyOrEnumerable is IEnumerable enumerable)
            {
                var enumerator = enumerable.GetEnumerator();
                using (enumerable as IDisposable)
                {
                    var items = new List<dynamic>();
                    while (enumerator.MoveNext())
                    {
                        items.Add(LoadCompareExchangeValue(enumerator.Current));
                    }
                    if (items.Count == 0)
                        return DynamicNullObject.Null;

                    return new DynamicArray(items);
                }
            }

            throw new InvalidOperationException(
                "LoadCompareExchangeValue may only be called with a string or an enumerable, but was called with a parameter of type " +
                keyOrEnumerable.GetType().FullName + ": " + keyOrEnumerable);
        }

        public IEnumerable<dynamic> Recurse(object item, Func<dynamic, dynamic> func)
        {
            return new RecursiveFunction(item, func).Execute();
        }

        protected IEnumerable<AbstractField> CreateField(string name, object value, CreateFieldOptions options)
        {
            // IMPORTANT: Do not delete this method, it is used by the indexes code when using CreateField

            options = options ?? CreateFieldOptions.Default;

            IndexFieldOptions allFields = null;
            var scope = CurrentIndexingScope.Current;
            if (scope.IndexDefinition is MapIndexDefinition mapIndexDefinition)
                mapIndexDefinition.IndexDefinition.Fields.TryGetValue(Constants.Documents.Indexing.Fields.AllFields, out allFields);

            var field = IndexField.Create(name, new IndexFieldOptions
            {
                Storage = options.Storage,
                TermVector = options.TermVector,
                Indexing = options.Indexing
            }, allFields);

            if (scope.DynamicFields == null)
                scope.DynamicFields = new Dictionary<string, FieldIndexing>();

            scope.DynamicFields[name] = field.Indexing;

            if (scope.CreateFieldConverter == null)
                scope.CreateFieldConverter = new LuceneDocumentConverter(scope.Index, new IndexField[] { });

            using var i = scope.CreateFieldConverter.NestedField(scope.CreatedFieldsCount++);
            var result = new List<AbstractField>();
            scope.CreateFieldConverter.GetRegularFields(new StaticIndexLuceneDocumentWrapper(result), field, value, CurrentIndexingScope.Current.IndexContext, out _);
            return result;
        }

        protected IEnumerable<AbstractField> CreateField(string name, object value, bool stored = false, bool? analyzed = null)
        {
            // IMPORTANT: Do not delete this method, it is used by the indexes code when using CreateField

            FieldIndexing? indexing;

            switch (analyzed)
            {
                case true:
                    indexing = FieldIndexing.Search;
                    break;
                case false:
                    indexing = FieldIndexing.Exact;
                    break;
                default:
                    indexing = null;
                    break;
            }

            return CreateField(name, value, new CreateFieldOptions
            {
                Storage = stored ? FieldStorage.Yes : FieldStorage.No,
                Indexing = indexing,
                TermVector = FieldTermVector.No
            });
        }

        public unsafe dynamic AsDateOnly(dynamic field)
        {
            if (field is LazyStringValue lsv)
            {
                if (LazyStringParser.TryParseDateOnly(lsv.Buffer, lsv.Length, out var @do) == false) 
                    return DynamicNullObject.Null;
                
                return @do;
            }
            
            if (field is string str)
            {
                fixed (char* strBuffer = str.AsSpan())
                {
                    if (LazyStringParser.TryParseDateOnly(strBuffer, str.Length, out var to) == false)
                        return DynamicNullObject.Null;

                    return to;
                }
            }
            
            if (field is DateTime dt)
            {
                return DateOnly.FromDateTime(dt);
            }

            if (field is DateOnly dtO)
            {
                return dtO;
            }
            
            if (field is null)
            {
                return DynamicNullObject.ExplicitNull;
            }
            
            if (field is DynamicNullObject dno)
            {
                return dno;
            }
            
            throw new InvalidDataException($"Expected {nameof(DateTime)}, {nameof(DateOnly)}, null, string or JSON value.");
        }

        public unsafe dynamic AsTimeOnly(dynamic field)
        {
            if (field is LazyStringValue lsv)
            {
                if (LazyStringParser.TryParseTimeOnly(lsv.Buffer, lsv.Length, out var to) == false)
                    return DynamicNullObject.Null;

                return to;
            }

            if (field is string str)
            {
                fixed (char* strBuffer = str.AsSpan())
                {
                    if (LazyStringParser.TryParseTimeOnly(strBuffer, str.Length, out var to) == false)
                        return DynamicNullObject.Null;

                    return to;
                }
            }
            
            if (field is TimeSpan ts)
            {
                return TimeOnly.FromTimeSpan(ts);
            }

            if (field is TimeOnly toF)
            {
                return toF;
            }

            if (field is null)
            {
                return DynamicNullObject.ExplicitNull;
            }
            
            if (field is DynamicNullObject dno)
            {
                return dno;
            }
            
            throw new InvalidDataException($"Expected {nameof(TimeSpan)}, {nameof(TimeOnly)}, null, string or JSON value.");
        }

        public IEnumerable<object> CreateSpatialField(string name, object lat, object lng)
        {
            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException("Indexing scope was not initialized.");
            
            return CreateSpatialField(name, ConvertToDouble(lat), ConvertToDouble(lng));
        }

        public IEnumerable<object> CreateSpatialField(string name, double? lat, double? lng)
        {
            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException("Indexing scope was not initialized.");
            
            var spatialField = GetOrCreateSpatialField(name);

            return CreateSpatialField(spatialField, lat, lng);
        }

        public IEnumerable<object> CreateSpatialField(string name, object shapeWkt)
        {
            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException("Indexing scope was not initialized.");
            
            var spatialField = GetOrCreateSpatialField(name);
            return CreateSpatialField(spatialField, shapeWkt);
        }

        internal static IEnumerable<object> CreateSpatialField(SpatialField spatialField, object lat, object lng)
        {
            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException("Indexing scope was not initialized.");
            
            return CreateSpatialField(spatialField, ConvertToDouble(lat), ConvertToDouble(lng));
        }

        internal static IEnumerable<object> CreateSpatialField(SpatialField spatialField, double? lat, double? lng)
        {
            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException("Indexing scope was not initialized.");
            
            if (lng == null || double.IsNaN(lng.Value))
                return Enumerable.Empty<AbstractField>();
            if (lat == null || double.IsNaN(lat.Value))
                return Enumerable.Empty<AbstractField>();

            IShape shape = spatialField.GetContext().MakePoint(lng.Value, lat.Value);
            return CurrentIndexingScope.Current.Index.SearchEngineType is SearchEngineType.Lucene
                ? spatialField.LuceneCreateIndexableFields(shape)
                : Enumerable.Cast<object>(spatialField.CoraxCreateIndexableFields(shape));
        }

        internal static IEnumerable<object> CreateSpatialField(SpatialField spatialField, object shapeWkt)
        {
            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException("Indexing scope was not initialized.");
            
            return CurrentIndexingScope.Current.Index.SearchEngineType is SearchEngineType.Lucene
                ? spatialField.LuceneCreateIndexableFields(shapeWkt)
                : Enumerable.Cast<object>(spatialField.CoraxCreateIndexableFields(shapeWkt));
        }

        internal static SpatialField GetOrCreateSpatialField(string name)
        {
            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException("Indexing scope was not initialized.");
            
            return CurrentIndexingScope.Current.GetOrCreateSpatialField(name);
        }

        private static double? ConvertToDouble(object value)
        {
            if (value == null || value is DynamicNullObject)
                return null;

            if (value is LazyNumberValue lnv)
                return lnv.ToDouble(CultureInfo.InvariantCulture);

            return Convert.ToDouble(value);
        }

        private struct StaticIndexLuceneDocumentWrapper : ILuceneDocumentWrapper
        {
            private readonly List<AbstractField> _fields;

            public StaticIndexLuceneDocumentWrapper(List<AbstractField> fields)
            {
                _fields = fields;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Add(AbstractField field)
            {
                _fields.Add(field); 
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public IList<IFieldable> GetFields()
            {
                throw new NotImplementedException();
            }
        }
    }
    
    public abstract class AbstractJavaScriptIndexBase : AbstractStaticIndexBase
    {
        public const string Load = "load";
        public const string NoTracking = "noTracking";
        public const string CmpXchg = "cmpxchg";
        protected const string GlobalDefinitions = "globalDefinition";
        protected const string CollectionProperty = "collection";
        protected const string MethodProperty = "method";
        protected const string MoreArgsProperty = "moreArgs";

        protected const string MapsProperty = "maps";

        protected const string ReduceProperty = "reduce";
        protected const string AggregateByProperty = "aggregateBy";
        protected const string KeyProperty = "key";

        protected readonly IndexDefinition Definition;

        public static AbstractStaticIndexBase Create(IndexDefinition definition, RavenConfiguration configuration, long indexVersion, CancellationToken token)
        {
            switch (definition.SourceType)
            {
                case IndexSourceType.Documents:
                    switch (configuration.JavaScript.EngineType)
                    {
                        case JavaScriptEngineType.Jint:
                            return new JavaScriptIndexJint(definition, configuration, indexVersion);
                        case JavaScriptEngineType.V8:
                            return new JavaScriptIndexV8(definition, configuration, indexVersion, token);
                        default:
                            throw new ArgumentOutOfRangeException(nameof(configuration.JavaScript.EngineType));
                    }
                case IndexSourceType.TimeSeries:
                    switch (configuration.JavaScript.EngineType)
                    {
                        case JavaScriptEngineType.Jint:
                            return new TimeSeriesJavaScriptIndexJint(definition, configuration, indexVersion);
                        case JavaScriptEngineType.V8:
                            return new TimeSeriesJavaScriptIndexV8(definition, configuration, indexVersion, token);
                        default:
                            throw new ArgumentOutOfRangeException(nameof(configuration.JavaScript.EngineType));
                    }
                case IndexSourceType.Counters:
                    switch (configuration.JavaScript.EngineType)
                    {
                        case JavaScriptEngineType.Jint:
                            return new CountersJavaScriptIndexJint(definition, configuration, indexVersion);
                        case JavaScriptEngineType.V8:
                            return new CountersJavaScriptIndexV8(definition, configuration, indexVersion, token);
                        default:
                            throw new ArgumentOutOfRangeException(nameof(configuration.JavaScript.EngineType));
                    }
                default:
                    throw new NotSupportedException($"Not supported source type '{definition.SourceType}'.");
            }
        }

        protected AbstractJavaScriptIndexBase(IndexDefinition definition)
        {
            Definition = definition;
        }

        protected List<string> GetMappingFunctions(Action<List<string>> modifyMappingFunctions)
        {
            if (Definition.Maps == null || Definition.Maps.Count == 0)
                ThrowIndexCreationException("does not contain any mapping functions to process.");

            var mappingFunctions = Definition.Maps.ToList(); 
            modifyMappingFunctions?.Invoke(mappingFunctions);

            return mappingFunctions;
        }
        


        protected static readonly ParserOptions DefaultParserOptions = new ParserOptions();
        
        protected void ThrowIndexCreationException(string message)
        {
            throw new IndexCreationException($"JavaScript index {Definition.Name} {message}");
        }

        protected const string Code = @"
var globalDefinition =
{
    maps: [],
    reduce: null
}

function groupBy(lambda) {
    var reduce = globalDefinition.reduce = { };
    reduce.key = lambda;

    reduce.aggregate = function(reduceFunction){
        reduce.aggregateBy = reduceFunction;
    }
    return reduce;
}

// createSpatialField(wkt: string)
// createSpatialField(lat: number, lng: number)

function createSpatialField() {
    if(arguments.length == 1) {
        return { $spatial: arguments[0] }
}

    return { $spatial: {Lng: arguments[1], Lat: arguments[0]} }
}

function createField(name, value, options) {
    return { $name: name, $value: value, $options: options }
}

function boost(value, boost) {
    return { $value: value, $boost: boost }
}
";


    }

}
