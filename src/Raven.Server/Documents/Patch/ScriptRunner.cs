﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using Jint;
using Jint.Native;
using Jint.Native.Array;
using Jint.Native.Function;
using Jint.Native.Object;
using Jint.Runtime.Interop;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Exceptions.Documents.Patching;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Queries;
using Raven.Server.Extensions;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Spatial4n.Core.Distance;
using JavaScriptException = Jint.Runtime.JavaScriptException;

namespace Raven.Server.Documents.Patch
{
    public class ScriptRunner
    {
        private readonly ConcurrentQueue<SingleRun> _cache = new ConcurrentQueue<SingleRun>();
        private readonly DocumentDatabase _db;
        private readonly RavenConfiguration _configuration;
        internal readonly bool _enableClr;
        private readonly DateTime _creationTime;
        public readonly List<string> ScriptsSource = new List<string>();

        public long Runs;
        DateTime _lastRun;

        public string ScriptType { get; internal set; }

        public ScriptRunner(DocumentDatabase db, RavenConfiguration configuration, bool enableClr)
        {
            _db = db;
            _configuration = configuration;
            _enableClr = enableClr;
            _creationTime = DateTime.UtcNow;
        }

        public DynamicJsonValue GetDebugInfo(bool detailed = false)
        {
            var djv = new DynamicJsonValue
            {
                ["Type"] = ScriptType,
                ["CreationTime"] = _creationTime,
                ["LastRun"] = _lastRun,
                ["Runs"] = Runs,
                ["CachedScriptsCount"] = _cache.Count
            };
            if (detailed)
                djv["ScriptsSource"] = ScriptsSource;

            return djv;
        }

        public void AddScript(string script)
        {
            ScriptsSource.Add(script);
        }

        public ReturnRun GetRunner(out SingleRun run)
        {
            _lastRun = DateTime.UtcNow;
            if (_cache.TryDequeue(out run) == false)
                run = new SingleRun(_db, _configuration, this, ScriptsSource);
            Interlocked.Increment(ref Runs);
            return new ReturnRun(this, run);
        }

        public void TryCompileScript(string script)
        {
            try
            {
                var engine = new Engine(options =>
                {
                    options.MaxStatements(1).LimitRecursion(1);
                });
                engine.Execute(script);
            }
            catch (Exception e)
            {
                throw new JavaScriptParseException("Failed to parse:" + Environment.NewLine + script, e);
            }
        }

        public class SingleRun
        {
            private readonly DocumentDatabase _database;
            private readonly RavenConfiguration _configuration;

            private readonly ScriptRunner _runner;
            public readonly Engine ScriptEngine;
            private DocumentsOperationContext _docsCtx;
            private JsonOperationContext _jsonCtx;
            public PatchDebugActions DebugActions;
            public bool DebugMode;
            public List<string> DebugOutput;
            public bool PutOrDeleteCalled;
            public HashSet<string> Includes;
            private HashSet<string> _documentIds;

            public bool ReadOnly
            {
                get => JavaScriptUtils.ReadOnly;
                set => JavaScriptUtils.ReadOnly = value;
            }

            public string OriginalDocumentId;
            public bool RefreshOriginalDocument;
            private readonly ConcurrentLruRegexCache _regexCache = new ConcurrentLruRegexCache(1024);
            public HashSet<string> UpdatedDocumentCounterIds;
            public JavaScriptUtils JavaScriptUtils;

            public SingleRun(DocumentDatabase database, RavenConfiguration configuration, ScriptRunner runner, List<string> scriptsSource)
            {
                _database = database;
                _configuration = configuration;
                _runner = runner;
                ScriptEngine = new Engine(options =>
                {
                    options.LimitRecursion(64)
                        .SetReferencesResolver(_refResolver)
                        .MaxStatements(_configuration.Patching.MaxStepsForScript)
                        .Strict(_configuration.Patching.StrictMode)
                        .AddObjectConverter(new JintGuidConverter())
                        .AddObjectConverter(new JintStringConverter())
                        .AddObjectConverter(new JintEnumConverter())
                        .AddObjectConverter(new JintDateTimeConverter())
                        .AddObjectConverter(new JintTimeSpanConverter())
                        .LocalTimeZone(TimeZoneInfo.Utc);

                });

                JavaScriptUtils = new JavaScriptUtils(_runner, ScriptEngine);
                ScriptEngine.SetValue("getMetadata", new ClrFunctionInstance(ScriptEngine, "getMetadata", JavaScriptUtils.GetMetadata));
                ScriptEngine.SetValue("id", new ClrFunctionInstance(ScriptEngine, "id", JavaScriptUtils.GetDocumentId));

                ScriptEngine.SetValue("output", new ClrFunctionInstance(ScriptEngine, "output", OutputDebug));

                //console.log
                ObjectInstance consoleObject = new ObjectInstance(ScriptEngine);
                consoleObject.FastAddProperty("log", new ClrFunctionInstance(ScriptEngine, "log", OutputDebug), false, false, false);
                ScriptEngine.SetValue("console", consoleObject);


                //spatial.distance
                ObjectInstance spatialObject = new ObjectInstance(ScriptEngine);
                var spatialFunc = new ClrFunctionInstance(ScriptEngine, "distance", Spatial_Distance);
                spatialObject.FastAddProperty("distance", spatialFunc, false, false, false);
                ScriptEngine.SetValue("spatial", spatialObject);
                ScriptEngine.SetValue("spatial.distance", spatialFunc);

                ScriptEngine.SetValue("include", new ClrFunctionInstance(ScriptEngine, "include", IncludeDoc));
                ScriptEngine.SetValue("load", new ClrFunctionInstance(ScriptEngine, "load", LoadDocument));
                ScriptEngine.SetValue("LoadDocument", new ClrFunctionInstance(ScriptEngine, "LoadDocument", ThrowOnLoadDocument));
                ScriptEngine.SetValue("loadPath", new ClrFunctionInstance(ScriptEngine, "loadPath", LoadDocumentByPath));
                ScriptEngine.SetValue("del", new ClrFunctionInstance(ScriptEngine, "del", DeleteDocument));
                ScriptEngine.SetValue("DeleteDocument", new ClrFunctionInstance(ScriptEngine, "DeleteDocument", ThrowOnDeleteDocument));
                ScriptEngine.SetValue("put", new ClrFunctionInstance(ScriptEngine, "put", PutDocument));
                ScriptEngine.SetValue("PutDocument", new ClrFunctionInstance(ScriptEngine, "PutDocument", ThrowOnPutDocument));
                ScriptEngine.SetValue("cmpxchg", new ClrFunctionInstance(ScriptEngine, "cmpxchg", CompareExchange));

                ScriptEngine.SetValue("counter", new ClrFunctionInstance(ScriptEngine, "counter", GetCounter));
                ScriptEngine.SetValue("counterRaw", new ClrFunctionInstance(ScriptEngine, "counterRaw", GetCounterRaw));
                ScriptEngine.SetValue("incrementCounter", new ClrFunctionInstance(ScriptEngine, "incrementCounter", IncrementCounter));
                ScriptEngine.SetValue("deleteCounter", new ClrFunctionInstance(ScriptEngine, "deleteCounter", DeleteCounter));

                ScriptEngine.SetValue("lastModified", new ClrFunctionInstance(ScriptEngine, "lastModified", GetLastModified));

                ScriptEngine.SetValue("startsWith", new ClrFunctionInstance(ScriptEngine, "startsWith", StartsWith));
                ScriptEngine.SetValue("endsWith", new ClrFunctionInstance(ScriptEngine, "endsWith", EndsWith));
                ScriptEngine.SetValue("regex", new ClrFunctionInstance(ScriptEngine, "regex", Regex));

                ScriptEngine.SetValue("Raven_ExplodeArgs", new ClrFunctionInstance(ScriptEngine, "Raven_ExplodeArgs", ExplodeArgs));
                ScriptEngine.SetValue("Raven_Min", new ClrFunctionInstance(ScriptEngine, "Raven_Min", Raven_Min));
                ScriptEngine.SetValue("Raven_Max", new ClrFunctionInstance(ScriptEngine, "Raven_Max", Raven_Max));

                ScriptEngine.SetValue("convertJsTimeToTimeSpanString", new ClrFunctionInstance(ScriptEngine, "convertJsTimeToTimeSpanString", ConvertJsTimeToTimeSpanString));
                ScriptEngine.SetValue("compareDates", new ClrFunctionInstance(ScriptEngine, "compareDates", CompareDates));

                ScriptEngine.SetValue("toStringWithFormat", new ClrFunctionInstance(ScriptEngine, "toStringWithFormat", ToStringWithFormat));

                ScriptEngine.SetValue("scalarToRawString", new ClrFunctionInstance(ScriptEngine, "scalarToRawString", ScalarToRawString));

                ScriptEngine.Execute(ScriptRunnerCache.PolyfillJs);

                foreach (var script in scriptsSource)
                {
                    try
                    {
                        ScriptEngine.Execute(script);
                    }
                    catch (Exception e)
                    {
                        throw new JavaScriptParseException("Failed to parse: " + Environment.NewLine + script, e);
                    }
                }
            }

            private void GenericSortTwoElementArray(JsValue[] args, [CallerMemberName]string caller = null)
            {
                void Swap()
                {
                    var tmp = args[1];
                    args[1] = args[0];
                    args[0] = tmp;
                }

                // this is basically the same as Math.min / Math.max, but 
                // can also be applied to strings, numbers and nulls

                if (args.Length != 2)
                    throw new ArgumentException(caller + "must be called with exactly two arguments");

                switch (args[0].Type)
                {
                    case Jint.Runtime.Types.None:
                    case Jint.Runtime.Types.Undefined:
                    case Jint.Runtime.Types.Null:
                        // null sorts lowers, so that is fine (either the other one is null or
                        // already higher than us).
                        break;
                    case Jint.Runtime.Types.Boolean:
                    case Jint.Runtime.Types.Number:
                        var a = Jint.Runtime.TypeConverter.ToNumber(args[0]);
                        var b = Jint.Runtime.TypeConverter.ToNumber(args[1]);
                        if (a > b)
                            Swap();
                        break;
                    case Jint.Runtime.Types.String:
                        switch (args[1].Type)
                        {
                            case Jint.Runtime.Types.None:
                            case Jint.Runtime.Types.Undefined:
                            case Jint.Runtime.Types.Null:
                                Swap();// a value is bigger than no value
                                break;
                            case Jint.Runtime.Types.Boolean:
                            case Jint.Runtime.Types.Number:
                                // if the string value is a number that is smaller than 
                                // the numeric value, because Math.min(true, "-2") works :-(
                                if (double.TryParse(args[0].AsString(), out double d) == false ||
                                    d > Jint.Runtime.TypeConverter.ToNumber(args[1]))
                                {
                                    Swap();
                                }
                                break;
                            case Jint.Runtime.Types.String:
                                if (string.Compare(args[0].AsString(), args[1].AsString()) > 0)
                                    Swap();
                                break;
                        }
                        break;
                    case Jint.Runtime.Types.Object:
                        throw new ArgumentException(caller + " cannot be called on an object");
                }
            }

            private JsValue Raven_Max(JsValue self, JsValue[] args)
            {
                GenericSortTwoElementArray(args);
                return args[1];
            }

            private JsValue Raven_Min(JsValue self, JsValue[] args)
            {
                GenericSortTwoElementArray(args);
                return args[0];
            }

            private JsValue IncludeDoc(JsValue self, JsValue[] args)
            {
                if (args.Length != 1)
                    throw new InvalidOperationException("include(id) must be called with a single argument");

                if (args[0].IsNull() || args[0].IsUndefined())
                    return args[0];

                if (args[0].IsArray())// recursive call ourselves
                {
                    var array = args[0].AsArray();
                    foreach (var pair in array.GetOwnProperties())
                    {
                        args[0] = pair.Value.Value;
                        if (args[0].IsString())
                            IncludeDoc(self, args);
                    }
                    return self;
                }

                if (args[0].IsString() == false)
                    throw new InvalidOperationException("include(doc) must be called with an string or string array argument");

                var id = args[0].AsString();

                if (Includes == null)
                    Includes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                Includes.Add(id);

                return self;
            }

            public override string ToString()
            {
                return string.Join(Environment.NewLine, _runner.ScriptsSource);
            }

            private static JsValue GetLastModified(JsValue self, JsValue[] args)
            {
                if (args.Length != 1)
                    throw new InvalidOperationException("lastModified(doc) must be called with a single argument");

                if (args[0].IsNull() || args[0].IsUndefined())
                    return args[0];

                if (args[0].IsObject() == false)
                    throw new InvalidOperationException("lastModified(doc) must be called with an object argument");

                if (args[0].AsObject() is BlittableObjectInstance doc)
                {
                    if (doc.LastModified == null)
                        return Undefined.Instance;

                    // we use UTC because last modified is in UTC
                    var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                    var jsTime = doc.LastModified.Value.Subtract(epoch)
                        .TotalMilliseconds;
                    return jsTime;
                }
                return Undefined.Instance;
            }

            private JsValue Spatial_Distance(JsValue self, JsValue[] args)
            {
                if (args.Length < 4 && args.Length > 5)
                    throw new ArgumentException("Called with expected number of arguments, expected: spatial.distance(lat1, lng1, lat2, lng2, kilometers | miles | cartesian)");

                for (int i = 0; i < 4; i++)
                {
                    if (args[i].IsNumber() == false)
                        return Undefined.Instance;
                }

                var lat1 = args[0].AsNumber();
                var lng1 = args[1].AsNumber();
                var lat2 = args[2].AsNumber();
                var lng2 = args[3].AsNumber();

                var units = SpatialUnits.Kilometers;
                if (args.Length > 4 && args[4].IsString())
                {
                    if (string.Equals("cartesian", args[4].AsString(), StringComparison.OrdinalIgnoreCase))
                        return SpatialDistanceFieldComparatorSource.SpatialDistanceFieldComparator.CartesianDistance(lat1, lng1, lat2, lng2);

                    if (Enum.TryParse(args[4].AsString(), ignoreCase: true, out units) == false)
                        throw new ArgumentException("Unable to parse units " + args[5] + ", expected: 'kilomoters' or 'miles'");
                }

                var result = SpatialDistanceFieldComparatorSource.SpatialDistanceFieldComparator.HaverstineDistanceInMiles(lat1, lng1, lat2, lng2);
                if (units == SpatialUnits.Kilometers)
                    result *= DistanceUtils.MILES_TO_KM;

                return result;
            }


            private JsValue OutputDebug(JsValue self, JsValue[] args)
            {
                if (DebugMode == false)
                    return self;

                var obj = args[0];

                DebugOutput.Add(GetDebugValue(obj, false));
                return self;
            }

            private string GetDebugValue(JsValue obj, bool recursive)
            {
                if (obj.IsString())
                {
                    var debugValue = obj.ToString();
                    return recursive ? '"' + debugValue + '"' : debugValue;
                }
                if (obj.IsArray())
                {
                    var sb = new StringBuilder("[");
                    var array = obj.AsArray();
                    var jsValue = (int)array.Get("length").AsNumber();
                    for (var i = 0; i < jsValue; i++)
                    {
                        if (i != 0)
                            sb.Append(",");
                        sb.Append(GetDebugValue(array.Get(i.ToString()), true));
                    }
                    sb.Append("]");
                    return sb.ToString();
                }
                if (obj.IsObject())
                {
                    var result = new ScriptRunnerResult(this, obj);
                    using (var jsonObj = result.TranslateToObject(_jsonCtx))
                    {
                        return jsonObj.ToString();
                    }
                }
                if (obj.IsBoolean())
                    return obj.AsBoolean().ToString();
                if (obj.IsNumber())
                    return obj.AsNumber().ToString(CultureInfo.InvariantCulture);
                if (obj.IsNull())
                    return "null";
                if (obj.IsUndefined())
                    return "undefined";
                return obj.ToString();
            }

            public JsValue ExplodeArgs(JsValue self, JsValue[] args)
            {
                if (args.Length != 2)
                    throw new InvalidOperationException("Raven_ExplodeArgs(this, args) - must be called with 2 arguments");
                if (args[1].IsObject() && args[1].AsObject() is BlittableObjectInstance boi)
                {
                    _refResolver.ExplodeArgsOn(args[0], boi);
                    return self;
                }
                if (args[1].IsNull() || args[1].IsUndefined())
                    return self;// noop
                throw new InvalidOperationException("Raven_ExplodeArgs(this, args) second argument must be BlittableObjectInstance");
            }

            public JsValue PutDocument(JsValue self, JsValue[] args)
            {
                string changeVector = null;

                if (args.Length != 2 && args.Length != 3)
                    throw new InvalidOperationException("put(id, doc, changeVector) must be called with called with 2 or 3 arguments only");
                AssertValidDatabaseContext();
                AssertNotReadOnly();
                if (args[0].IsString() == false && args[0].IsNull() == false && args[0].IsUndefined() == false)
                    AssertValidId();

                var id = args[0].IsNull() || args[0].IsUndefined() ? null : args[0].AsString();

                if (args[1].IsObject() == false)
                    throw new InvalidOperationException(
                        $"Created document must be a valid object which is not null or empty. Document ID: '{id}'.");

                PutOrDeleteCalled = true;

                if (args.Length == 3)
                    if (args[2].IsString())
                        changeVector = args[2].AsString();
                    else if (args[2].IsNull() == false && args[0].IsUndefined() == false)
                        throw new InvalidOperationException(
                            $"The change vector must be a string or null. Document ID: '{id}'.");

                BlittableJsonReaderObject reader = null;
                try
                {
                    reader = JsBlittableBridge.Translate(_jsonCtx, ScriptEngine, args[1].AsObject(), usageMode: BlittableJsonDocumentBuilder.UsageMode.ToDisk);

                    var put = _database.DocumentsStorage.Put(
                        _docsCtx,
                        id,
                        _docsCtx.GetLazyString(changeVector),
                        reader,
                        //RavenDB-11391 Those flags were added to cause attachment/counter metadata table check & remove metadata properties if not necessary
                        nonPersistentFlags: NonPersistentDocumentFlags.ResolveAttachmentsConflict | NonPersistentDocumentFlags.ResolveCountersConflict
                        );

                    if (DebugMode)
                    {
                        DebugActions.PutDocument.Add(new DynamicJsonValue
                        {
                            ["Id"] = put.Id,
                            ["Data"] = reader
                        });
                    }

                    if (RefreshOriginalDocument == false && string.Equals(put.Id, OriginalDocumentId, StringComparison.OrdinalIgnoreCase))
                        RefreshOriginalDocument = true;

                    return put.Id;
                }
                finally
                {
                    if (DebugMode == false)
                        reader?.Dispose();
                }
            }

            private static void AssertValidId()
            {
                throw new InvalidOperationException("The first parameter to put(id, doc, changeVector) must be a string");
            }

            public JsValue DeleteDocument(JsValue self, JsValue[] args)
            {
                if (args.Length != 1 && args.Length != 2)
                    throw new InvalidOperationException("delete(id, changeVector) must be called with at least one parameter");

                if (args[0].IsString() == false)
                    throw new InvalidOperationException("delete(id, changeVector) id argument must be a string");

                var id = args[0].AsString();
                string changeVector = null;

                if (args.Length == 2 && args[1].IsString())
                    changeVector = args[1].AsString();

                PutOrDeleteCalled = true;
                AssertValidDatabaseContext();
                AssertNotReadOnly();
                if (DebugMode)
                    DebugActions.DeleteDocument.Add(id);
                var result = _database.DocumentsStorage.Delete(_docsCtx, id, changeVector);

                if (RefreshOriginalDocument && string.Equals(OriginalDocumentId, id, StringComparison.OrdinalIgnoreCase))
                    RefreshOriginalDocument = false;

                return result != null;
            }

            private void AssertNotReadOnly()
            {
                if (ReadOnly)
                    throw new InvalidOperationException("Cannot make modifications in readonly context");
            }

            private void AssertValidDatabaseContext()
            {
                if (_docsCtx == null)
                    throw new InvalidOperationException("Unable to put documents when this instance is not attached to a database operation");
            }

            private JsValue LoadDocumentByPath(JsValue self, JsValue[] args)
            {
                AssertValidDatabaseContext();


                if (args.Length != 2 ||
                    (args[0].IsNull() == false && args[0].IsUndefined() == false && args[0].IsObject() == false)
                    || args[1].IsString() == false)
                    throw new InvalidOperationException("loadPath(doc, path) must be called with a document and path");

                if (args[0].IsNull() || args[1].IsUndefined())
                    return args[0];

                if (args[0].AsObject() is BlittableObjectInstance b)
                {
                    var path = args[1].AsString();
                    if (_documentIds == null)
                        _documentIds = new HashSet<string>();

                    _documentIds.Clear();
                    IncludeUtil.GetDocIdFromInclude(b.Blittable, path, _documentIds);
                    if (path.IndexOf("[]", StringComparison.InvariantCulture) != -1) // array
                        return JsValue.FromObject(ScriptEngine, _documentIds.Select(LoadDocumentInternal).ToList());
                    if (_documentIds.Count == 0)
                        return JsValue.Null;

                    return LoadDocumentInternal(_documentIds.First());

                }

                throw new InvalidOperationException("loadPath(doc, path) must be called with a valid document instance, but got a JS object instead");
            }

            private JsValue CompareExchange(JsValue self, JsValue[] args)
            {
                AssertValidDatabaseContext();

                if (args.Length != 1 || args[0].IsString() == false)
                    throw new InvalidOperationException("cmpxchg(key) must be called with a single string argument");

                return CmpXchangeInternal(CompareExchangeCommandBase.GetActualKey(_database.Name, args[0].AsString()));
            }

            private JsValue LoadDocument(JsValue self, JsValue[] args)
            {
                AssertValidDatabaseContext();

                if (args.Length != 1)
                    throw new InvalidOperationException("load(id | ids) must be called with a single string argument");

                if (args[0].IsNull() || args[0].IsUndefined())
                    return args[0];

                if (args[0].IsArray())
                {
                    var results = (ArrayInstance)ScriptEngine.Array.Construct(Array.Empty<JsValue>());
                    var arrayInstance = args[0].AsArray();
                    foreach (var kvp in arrayInstance.GetOwnProperties())
                    {
                        if (kvp.Key == "length")
                            continue;
                        if (kvp.Value.Value.IsString() == false)
                            throw new InvalidOperationException("load(ids) must be called with a array of strings, but got " + kvp.Value.Value.Type + " - " + kvp.Value.Value);
                        var result = LoadDocumentInternal(kvp.Value.Value.AsString());
                        ScriptEngine.Array.PrototypeObject.Push(results, new[] { result });
                    }
                    return results;
                }

                if (args[0].IsString() == false)
                    throw new InvalidOperationException("load(id | ids) must be called with a single string or array argument");

                return LoadDocumentInternal(args[0].AsString());
            }

            private JsValue GetCounter(JsValue self, JsValue[] args)
            {
                return GetCounterInternal(args);
            }

            private JsValue GetCounterRaw(JsValue self, JsValue[] args)
            {
                return GetCounterInternal(args, true);
            }

            private JsValue GetCounterInternal(JsValue[] args, bool raw = false)
            {
                AssertValidDatabaseContext();
                var signature = raw ? "counterRaw(doc, name)" : "counter(doc, name)";
                if (args.Length != 2)
                    throw new InvalidOperationException($"{signature} must be called with exactly 2 arguments");

                string id;
                if (args[0].IsObject() && args[0].AsObject() is BlittableObjectInstance doc)
                {
                    id = doc.DocumentId;
                }
                else if (args[0].IsString())
                {
                    id = args[0].AsString();
                }
                else
                {
                    throw new InvalidOperationException($"{signature}: 'doc' must be a string argument (the document id) or the actual document instance itself");
                }

                if (args[1].IsString() == false)
                {
                    throw new InvalidOperationException($"{signature}: 'name' must be a string argument");
                }

                var name = args[1].AsString();
                if (string.IsNullOrEmpty(id) || string.IsNullOrEmpty(name))
                {
                    return JsValue.Undefined;
                }

                if (raw == false)
                {
                    return _database.DocumentsStorage.CountersStorage.GetCounterValue(_docsCtx, id, name) ?? JsValue.Null;
                }

                var rawValues = new ObjectInstance(ScriptEngine);
                foreach (var (cv, val) in _database.DocumentsStorage.CountersStorage.GetCounterValues(_docsCtx, id, name))
                {
                    rawValues.FastAddProperty(cv, val, true, false, false);
                }

                return rawValues;
            }

            private JsValue IncrementCounter(JsValue self, JsValue[] args)
            {
                AssertValidDatabaseContext();

                if (args.Length < 2 || args.Length > 3)
                {
                    ThrowInvalidIncrementCounterArgs(args);
                }

                var signature = args.Length == 2 ? "incrementCounter(doc, name)" : "incrementCounter(doc, name, value)";

                BlittableJsonReaderObject docBlittable = null;
                string id = null;

                if (args[0].IsObject() && args[0].AsObject() is BlittableObjectInstance doc)
                {
                    id = doc.DocumentId;
                    docBlittable = doc.Blittable;
                }
                else if (args[0].IsString())
                {
                    id = args[0].AsString();
                    var document = _database.DocumentsStorage.Get(_docsCtx, id);
                    if (document == null)
                    {
                        ThrowMissingDocument(id);
                        Debug.Assert(false); // never hit
                    }

                    docBlittable = document.Data;
                }
                else
                {
                    ThrowInvalidDocumentArgsType(signature);
                }

                Debug.Assert(id != null && docBlittable != null);

                if (args[1].IsString() == false)
                    ThrowInvalidCounterName(signature);

                var name = args[1].AsString();
                if (string.IsNullOrWhiteSpace(name))
                    ThrowInvalidCounterName(signature);

                double value = 1;
                if (args.Length == 3)
                {
                    if (args[2].IsNumber() == false)
                        ThrowInvalidCounterValue();
                    value = args[2].AsNumber();
                }

                _database.DocumentsStorage.CountersStorage.IncrementCounter(_docsCtx, id, CollectionName.GetCollectionName(docBlittable), name, (long)value, out var exists);

                if (exists == false)
                {
                    if (UpdatedDocumentCounterIds == null)
                        UpdatedDocumentCounterIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                    UpdatedDocumentCounterIds.Add(id);
                }

                return JsBoolean.True;
            }

            private static void ThrowInvalidIncrementCounterArgs(JsValue[] args)
            {
                throw new InvalidOperationException($"There is no overload of method 'incrementCounter' that takes {args.Length} arguments." +
                                                    "Supported overloads are : 'incrementCounter(doc, name)' , 'incrementCounter(doc, name, value)'");
            }

            private static void ThrowInvalidCounterValue()
            {
                throw new InvalidOperationException("incrementCounter(doc, name, value): 'value' must be a number argument");
            }

            private static void ThrowInvalidCounterName(string signature)
            {
                throw new InvalidOperationException($"{signature}: 'name' must be a non-empty string argument");
            }

            private static void ThrowInvalidDocumentArgsType(string signature)
            {
                throw new InvalidOperationException($"{signature}: 'doc' must be a string argument (the document id) or the actual document instance itself");
            }

            private static void ThrowMissingDocument(string id)
            {
                throw new DocumentDoesNotExistException(id, "Cannot operate on counters of a missing document.");
            }

            private JsValue DeleteCounter(JsValue self, JsValue[] args)
            {
                AssertValidDatabaseContext();

                if (args.Length != 2)
                {
                    ThrowInvalidDeleteCounterArgs();
                }

                string id = null;
                BlittableJsonReaderObject docBlittable = null;

                if (args[0].IsObject() && args[0].AsObject() is BlittableObjectInstance doc)
                {
                    id = doc.DocumentId;
                    docBlittable = doc.Blittable;
                }
                else if (args[0].IsString())
                {
                    id = args[0].AsString();
                    var document = _database.DocumentsStorage.Get(_docsCtx, id);
                    if (document == null)
                    {
                        ThrowMissingDocument(id);
                        Debug.Assert(false); // never hit
                    }

                    docBlittable = document.Data;
                }
                else
                {
                    ThrowInvalidDeleteCounterDocumentArg();
                }

                Debug.Assert(id != null && docBlittable != null);

                if (args[1].IsString() == false)
                {
                    ThrowDeleteCounterNameArg();
                }

                if (UpdatedDocumentCounterIds == null)
                    UpdatedDocumentCounterIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                UpdatedDocumentCounterIds.Add(id);

                var name = args[1].AsString();
                _database.DocumentsStorage.CountersStorage.DeleteCounter(_docsCtx, id, CollectionName.GetCollectionName(docBlittable), name);

                return JsBoolean.True;
            }

            private static void ThrowDeleteCounterNameArg()
            {
                throw new InvalidOperationException("deleteCounter(doc, name): 'name' must be a string argument");
            }

            private static void ThrowInvalidDeleteCounterDocumentArg()
            {
                throw new InvalidOperationException("deleteCounter(doc, name): 'doc' must be a string argument (the document id) or the actual document instance itself");
            }

            private static void ThrowInvalidDeleteCounterArgs()
            {
                throw new InvalidOperationException("deleteCounter(doc, name) must be called with exactly 2 arguments");
            }

            private static JsValue ThrowOnLoadDocument(JsValue self, JsValue[] args)
            {
                throw new MissingMethodException("The method LoadDocument was renamed to 'load'");
            }

            private static JsValue ThrowOnPutDocument(JsValue self, JsValue[] args)
            {
                throw new MissingMethodException("The method PutDocument was renamed to 'put'");
            }

            private static JsValue ThrowOnDeleteDocument(JsValue self, JsValue[] args)
            {
                throw new MissingMethodException("The method DeleteDocument was renamed to 'del'");
            }

            private static JsValue ConvertJsTimeToTimeSpanString(JsValue self, JsValue[] args)
            {
                if (args.Length != 1 || args[0].IsNumber() == false)
                    throw new InvalidOperationException("convertJsTimeToTimeSpanString(ticks) must be called with a single long argument");

                var ticks = Convert.ToInt64(args[0].AsNumber()) * 10000;

                var asTimeSpan = new TimeSpan(ticks);

                return asTimeSpan.ToString();
            }

            private static JsValue CompareDates(JsValue self, JsValue[] args)
            {
                if (args.Length < 1 || args.Length > 3)
                {
                    throw new InvalidOperationException($"No overload for method 'compareDates' takes {args.Length} arguments. " +
                                                        "Supported overloads are : compareDates(date1, date2), compareDates(date1, date2, operationType)");
                }

                ExpressionType binaryOperationType;
                if (args.Length == 2)
                {
                    binaryOperationType = ExpressionType.Subtract;
                }
                else if (args[2].IsString() == false ||
                    Enum.TryParse(args[2].AsString(), out binaryOperationType) == false)
                {
                    throw new InvalidOperationException("compareDates(date1, date2, operationType) : 'operationType' must be a string argument representing a valid 'ExpressionType'");
                }

                dynamic date1, date2;
                if ((binaryOperationType == ExpressionType.Equal ||
                     binaryOperationType == ExpressionType.NotEqual) &&
                    args[0].IsString() && args[1].IsString())
                {
                    date1 = args[0].AsString();
                    date2 = args[1].AsString();
                }
                else
                {
                    date1 = GetDateArg(args[0]);
                    date2 = GetDateArg(args[1]);
                }

                switch (binaryOperationType)
                {
                    case ExpressionType.Subtract:
                        return (date1 - date2).ToString();
                    case ExpressionType.GreaterThan:
                        return date1 > date2;
                    case ExpressionType.GreaterThanOrEqual:
                        return date1 >= date2;
                    case ExpressionType.LessThan:
                        return date1 < date2;
                    case ExpressionType.LessThanOrEqual:
                        return date1 <= date2;
                    case ExpressionType.Equal:
                        return date1 == date2;
                    case ExpressionType.NotEqual:
                        return date1 != date2;
                    default:
                        throw new InvalidOperationException($"compareDates(date1, date2, binaryOp) : unsupported binary operation '{binaryOperationType}'");

                }
            }

            private static unsafe DateTime GetDateArg(JsValue arg)
            {
                if (arg.IsDate())
                {
                    return arg.AsDate().ToDateTime();
                }

                if (arg.IsString() == false)
                {
                    ThrowInvalidArgumentForCompareDates();
                }

                var s = arg.AsString();
                fixed (char* pValue = s)
                {
                    var result = LazyStringParser.TryParseDateTime(pValue, s.Length, out DateTime dt, out _);
                    switch (result)
                    {
                        case LazyStringParser.Result.DateTime:
                            return dt;
                        default:
                            ThrowInvalidArgumentForCompareDates();
                            return DateTime.MinValue; // never hit
                    }
                }
            }

            private static void ThrowInvalidArgumentForCompareDates()
            {
                throw new InvalidOperationException("compareDates(date1, date2, binaryOp) : 'date1', 'date2' must be of type 'DateInstance' or a DateTime string");
            }

            private static unsafe JsValue ToStringWithFormat(JsValue self, JsValue[] args)
            {
                if (args.Length < 1 || args.Length > 3)
                {
                    throw new InvalidOperationException($"No overload for method 'toStringWithFormat' takes {args.Length} arguments. " +
                                                        "Supported overloads are : toStringWithFormat(object), toStringWithFormat(object, format), toStringWithFormat(object, culture), toStringWithFormat(object, format, culture).");
                }

                var cultureInfo = CultureInfo.InvariantCulture;
                string format = null;

                for (var i = 1; i < args.Length; i++)
                {
                    if (args[i].IsString() == false)
                    {
                        throw new InvalidOperationException("toStringWithFormat : 'format' and 'culture' must be string arguments");
                    }

                    var arg = args[i].AsString();
                    if (CultureHelper.Cultures.TryGetValue(arg, out var culture))
                    {
                        cultureInfo = culture;
                        continue;
                    }

                    format = arg;
                }

                if (args[0].IsDate())
                {
                    var date = args[0].AsDate().ToDateTime();
                    return format != null ?
                        date.ToString(format, cultureInfo) :
                        date.ToString(cultureInfo);

                }

                if (args[0].IsNumber())
                {
                    var num = args[0].AsNumber();
                    return format != null ?
                        num.ToString(format, cultureInfo) :
                        num.ToString(cultureInfo);
                }

                if (args[0].IsString())
                {
                    var s = args[0].AsString();
                    fixed (char* pValue = s)
                    {
                        var result = LazyStringParser.TryParseDateTime(pValue, s.Length, out DateTime dt, out _);
                        switch (result)
                        {
                            case LazyStringParser.Result.DateTime:
                                return format != null ?
                                    dt.ToString(format, cultureInfo) :
                                    dt.ToString(cultureInfo);
                            default:
                                throw new InvalidOperationException("toStringWithFormat(dateString) : 'dateString' is not a valid DateTime string");
                        }
                    }
                }

                if (args[0].IsBoolean() == false)
                {
                    throw new InvalidOperationException($"toStringWithFormat() is not supported for objects of type {args[0].Type} ");
                }

                var boolean = args[0].AsBoolean();
                return boolean.ToString(cultureInfo);
            }

            private static JsValue StartsWith(JsValue self, JsValue[] args)
            {
                if (args.Length != 2 || args[0].IsString() == false || args[1].IsString() == false)
                    throw new InvalidOperationException("startsWith(text, contained) must be called with two string parameters");

                return args[0].AsString().StartsWith(args[1].AsString(), StringComparison.OrdinalIgnoreCase);
            }

            private static JsValue EndsWith(JsValue self, JsValue[] args)
            {
                if (args.Length != 2 || args[0].IsString() == false || args[1].IsString() == false)
                    throw new InvalidOperationException("endsWith(text, contained) must be called with two string parameters");

                return args[0].AsString().EndsWith(args[1].AsString(), StringComparison.OrdinalIgnoreCase);
            }

            private JsValue Regex(JsValue self, JsValue[] args)
            {
                if (args.Length != 2 || args[0].IsString() == false || args[1].IsString() == false)
                    throw new InvalidOperationException("regex(text, regex) must be called with two string parameters");

                var regex = _regexCache.Get(args[1].AsString());

                return regex.IsMatch(args[0].AsString());
            }

            private static JsValue ScalarToRawString(JsValue self2, JsValue[] args)
            {
                if (args.Length != 2)
                    throw new InvalidOperationException("scalarToRawString(document, lambdaToField) may be called on with two parameters only");


                JsValue firstParam = args[0];
                if (firstParam.IsObject() && args[0].AsObject() is BlittableObjectInstance selfInstance)
                {
                    JsValue secondParam = args[1];
                    if (secondParam.IsObject() && secondParam.AsObject() is ArrowFunctionInstance lambda)
                    {

                        var functionAst = lambda.FunctionDeclaration;
                        var propName = functionAst.TryGetFieldFromSimpleLambdaExpression();

                        if (selfInstance.OwnValues.TryGetValue(propName, out var existingValue))
                        {
                            if (existingValue.Changed)
                            {
                                return existingValue.Value;
                            }
                        }

                        var propertyIndex = selfInstance.Blittable.GetPropertyIndex(propName);

                        if (propertyIndex == -1)
                        {
                            return new ObjectInstance(selfInstance.Engine)
                            {
                                Extensible = true
                            };
                        }

                        BlittableJsonReaderObject.PropertyDetails propDetails = new BlittableJsonReaderObject.PropertyDetails();
                        selfInstance.Blittable.GetPropertyByIndex(propertyIndex, ref propDetails);
                        var value = propDetails.Value;

                        switch (propDetails.Token & BlittableJsonReaderBase.TypesMask)
                        {
                            case BlittableJsonToken.Null:
                                return JsValue.Null;
                            case BlittableJsonToken.Boolean:
                                return (bool)propDetails.Value;
                            case BlittableJsonToken.Integer:
                                return new ObjectWrapper(selfInstance.Engine, value);
                            case BlittableJsonToken.LazyNumber:
                                return new ObjectWrapper(selfInstance.Engine, value);
                            case BlittableJsonToken.String:
                                return new ObjectWrapper(selfInstance.Engine, value);
                            case BlittableJsonToken.CompressedString:
                                return new ObjectWrapper(selfInstance.Engine, value);
                            default:
                                throw new InvalidOperationException("scalarToRawString(document, lambdaToField) lambda to field must return either raw numeric or raw string types");
                        }
                    }
                    else
                    {
                        throw new InvalidOperationException("scalarToRawString(document, lambdaToField) must be called with a second lambda argument");
                    }
                }
                else
                {
                    throw new InvalidOperationException("scalarToRawString(document, lambdaToField) may be called with a document first parameter only");
                }
            }

            private JsValue CmpXchangeInternal(string key)
            {
                if (string.IsNullOrEmpty(key))
                    return JsValue.Undefined;

                using (_database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var value = _database.ServerStore.Cluster.GetCompareExchangeValue(ctx, key).Value;
                    if (value == null)
                        return null;

                    var jsValue = JavaScriptUtils.TranslateToJs(ScriptEngine, _jsonCtx, value.Clone(_jsonCtx));
                    return jsValue.AsObject().Get("Object");
                }
            }

            private JsValue LoadDocumentInternal(string id)
            {
                if (string.IsNullOrEmpty(id))
                    return JsValue.Undefined;
                if (DebugMode)
                    DebugActions.LoadDocument.Add(id);
                var document = _database.DocumentsStorage.Get(_docsCtx, id);
                return JavaScriptUtils.TranslateToJs(ScriptEngine, _jsonCtx, document);
            }

            private JsValue[] _args = Array.Empty<JsValue>();
            private readonly JintPreventResolvingTasksReferenceResolver _refResolver = new JintPreventResolvingTasksReferenceResolver();

            public ScriptRunnerResult Run(JsonOperationContext jsonCtx, DocumentsOperationContext docCtx, string method, object[] args)
            {
                return Run(jsonCtx, docCtx, method, null, args);
            }

            public ScriptRunnerResult Run(JsonOperationContext jsonCtx, DocumentsOperationContext docCtx, string method, string documentId, object[] args)
            {
                _docsCtx = docCtx;
                _jsonCtx = jsonCtx ?? ThrowArgumentNull();
                JavaScriptUtils.Reset(_jsonCtx);

                Reset();
                OriginalDocumentId = documentId;

                SetArgs(jsonCtx, method, args);

                try
                {
                    var call = ScriptEngine.GetValue(method).TryCast<ICallable>();
                    var result = call.Call(Undefined.Instance, _args);
                    return new ScriptRunnerResult(this, result);
                }
                catch (JavaScriptException e)
                {
                    //ScriptRunnerResult is in charge of disposing of the disposible but it is not created (the clones did)
                    JavaScriptUtils.Clear();
                    throw CreateFullError(e);
                }
                catch (Exception)
                {
                    JavaScriptUtils.Clear();
                    throw;
                }
                finally
                {
                    _refResolver.ExplodeArgsOn(null, null);
                    _docsCtx = null;
                    _jsonCtx = null;
                }
            }

            private void SetArgs(JsonOperationContext jsonCtx, string method, object[] args)
            {
                if (_args.Length != args.Length)
                    _args = new JsValue[args.Length];
                for (var i = 0; i < args.Length; i++)
                    _args[i] = JavaScriptUtils.TranslateToJs(ScriptEngine, jsonCtx, args[i]);

                if (method != QueryMetadata.SelectOutput &&
                    _args.Length == 2 &&
                    _args[1].IsObject() &&
                    _args[1].AsObject() is BlittableObjectInstance boi)
                {
                    _refResolver.ExplodeArgsOn(null, boi);
                }
            }


            private static JsonOperationContext ThrowArgumentNull()
            {
                throw new ArgumentNullException("jsonCtx");
            }

            private Client.Exceptions.Documents.Patching.JavaScriptException CreateFullError(JavaScriptException e)
            {
                string msg;
                if (e.Error.IsString())
                    msg = e.Error.AsString();
                else if (e.Error.IsObject())
                    msg = JsBlittableBridge.Translate(_jsonCtx, ScriptEngine, e.Error.AsObject()).ToString();
                else
                    msg = e.Error.ToString();

                msg = "At " + e.Column + ":" + e.LineNumber + " " + msg;
                var javaScriptException = new Client.Exceptions.Documents.Patching.JavaScriptException(msg, e);
                return javaScriptException;
            }

            private void Reset()
            {
                if (DebugMode)
                {
                    if (DebugOutput == null)
                        DebugOutput = new List<string>();
                    if (DebugActions == null)
                        DebugActions = new PatchDebugActions();
                }

                Includes?.Clear();
                UpdatedDocumentCounterIds?.Clear();
                PutOrDeleteCalled = false;
                OriginalDocumentId = null;
                RefreshOriginalDocument = false;
                ScriptEngine.ResetCallStack();
                ScriptEngine.ResetStatementsCount();
                ScriptEngine.ResetTimeoutTicks();
            }

            public object Translate(JsonOperationContext context, object o)
            {
                return JavaScriptUtils.TranslateToJs(ScriptEngine, context, o);
            }

            public object CreateEmptyObject()
            {
                return ScriptEngine.Object.Construct(Array.Empty<JsValue>());
            }

            public object Translate(ScriptRunnerResult result, JsonOperationContext context, JsBlittableBridge.IResultModifier modifier = null, BlittableJsonDocumentBuilder.UsageMode usageMode = BlittableJsonDocumentBuilder.UsageMode.None)
            {
                var val = result.RawJsValue;
                if (val.IsString())
                    return val.AsString();
                if (val.IsBoolean())
                    return val.AsBoolean();
                if (val.IsObject())
                    return result.TranslateToObject(context, modifier, usageMode);
                if (val.IsNumber())
                    return val.AsNumber();
                if (val.IsNull() || val.IsUndefined())
                    return null;
                if (val.IsArray())
                    throw new InvalidOperationException("Returning arrays from scripts is not supported, only objects or primitives");
                throw new NotSupportedException("Unable to translate " + val.Type);
            }
        }

        public struct ReturnRun : IDisposable
        {
            private ScriptRunner _parent;
            private SingleRun _run;

            public ReturnRun(ScriptRunner parent, SingleRun run)
            {
                _parent = parent;
                _run = run;
            }

            public void Dispose()
            {
                if (_run == null)
                    return;

                _run.ReadOnly = false;

                _run.DebugMode = false;
                _run.DebugOutput?.Clear();
                _run.DebugActions?.Clear();

                _run.Includes?.Clear();

                _run.OriginalDocumentId = null;
                _run.RefreshOriginalDocument = false;

                _run.UpdatedDocumentCounterIds?.Clear();

                _parent._cache.Enqueue(_run);
                _run = null;
                _parent = null;
            }
        }
    }
}
