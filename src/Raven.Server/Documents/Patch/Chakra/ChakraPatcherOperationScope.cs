using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Scripting.JavaScript;
using Raven.Abstractions.Data;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Patch.Chakra
{
    public class ChakraPatcherOperationScope : IDisposable
    {
        private readonly DocumentDatabase _database;
        private readonly JavaScriptEngine _engine;
        private readonly DocumentsOperationContext _context;

        private readonly Dictionary<string, KeyValuePair<object, JavaScriptValue>> _propertiesByValue = new Dictionary<string, KeyValuePair<object, JavaScriptValue>>();

        public readonly DynamicJsonArray DebugInfo = new DynamicJsonArray();

        private static readonly List<string> InheritedProperties = new List<string>
        {
            "length",
            "Map",
            "Where",
            "RemoveWhere",
            "Remove"
        };

        public bool DebugMode { get; }

        public readonly PatchDebugActions DebugActions;

        private readonly JavaScriptExecutionContext _executionContext;

        public ChakraPatcherOperationScope(DocumentDatabase database, JavaScriptEngine engine, DocumentsOperationContext context, bool debugMode = false)
        {
            _database = database;
            _engine = engine;
            _context = context;
            _executionContext = engine.AcquireContext();
            DebugMode = debugMode;
            if (DebugMode)
                DebugActions = new PatchDebugActions();
        }

        public JavaScriptValue ToJsArray(BlittableJsonReaderArray json, string propertyKey)
        {
            var result = _engine.CreateArray(json.Length);
            for (var i = 0; i < json.Length; i++)
            {
                var value = json.GetValueTokenTupleByIndex(i);
                var index = i.ToString();
                var jsVal = ToJsValue(value.Item1, value.Item2, propertyKey + "[" + index + "]");
                result.SetAt(i, jsVal);
                //result.FastAddProperty(index, jsVal, true, true, true);
            }
            //result.FastSetProperty("length", new PropertyDescriptor
            //{
            //    Value = new JsValue(json.Length),
            //    Configurable = true,
            //    Enumerable = true,
            //    Writable = true,
            //});
            return result;
        }

        public JavaScriptObject ToJsObject(BlittableJsonReaderObject json, string propertyName = null)
        {
            var jsObject = _engine.CreateObject();
            //var jsObject = engine.Object.Construct(Arguments.Empty);
            for (int i = 0; i < json.Count; i++)
            {
                var property = json.GetPropertyByIndex(i);
                var name = property.Item1.ToString();
                var propertyKey = CreatePropertyKey(name, propertyName);
                var value = property.Item2;
                JavaScriptValue jsValue = ToJsValue(value, property.Item3, propertyKey);
                _propertiesByValue[propertyKey] = new KeyValuePair<object, JavaScriptValue>(value, jsValue);
                jsObject.SetPropertyByName(name, jsValue);
                //jsObject.FastAddProperty(name, jsValue, true, true, true);
            }
            return jsObject;
        }

        public JavaScriptValue ToJsValue(object value, BlittableJsonToken token, string propertyKey = null)
        {
            switch (token & BlittableJsonReaderBase.TypesMask)
            {
                case BlittableJsonToken.Null:
                    return _engine.NullValue;
                case BlittableJsonToken.Boolean:
                    return _engine.Converter.FromBoolean((bool)value);

                case BlittableJsonToken.Integer:
                    return _engine.Converter.FromInt32((int)(long)value);
                case BlittableJsonToken.Float:
                    return _engine.Converter.FromDouble((double)(LazyDoubleValue)value);
                case BlittableJsonToken.String:
                    return _engine.Converter.FromString(((LazyStringValue)value).ToString());
                case BlittableJsonToken.CompressedString:
                    return _engine.Converter.FromString(((LazyCompressedStringValue)value).ToString());

                case BlittableJsonToken.StartObject:
                    return ToJsObject((BlittableJsonReaderObject)value, propertyKey);
                case BlittableJsonToken.StartArray:
                    return ToJsArray((BlittableJsonReaderArray)value, propertyKey);

                default:
                    throw new ArgumentOutOfRangeException(token.ToString());
            }
        }

        private static string CreatePropertyKey(string key, string property)
        {
            if (string.IsNullOrEmpty(property))
                return key;

            return property + "." + key;
        }

        public DynamicJsonValue ToBlittable(JavaScriptValue jsValue, string propertyKey = null, bool recursiveCall = false)
        {
            if (jsValue.Type == JavaScriptValueType.Function)
            {
                // getting a Function instance here,
                // means that we couldn't evaluate it using Chakra
                return null;
            }

            var jsObject = (JavaScriptObject)jsValue;

            var obj = new DynamicJsonValue();
            foreach (var propertyNameJs in jsObject.GetOwnPropertyNames())
            {
                var propertyName = propertyNameJs.ToString();

                if (propertyName == Constants.Indexing.Fields.ReduceKeyFieldName || propertyName == Constants.Indexing.Fields.DocumentIdFieldName)
                    continue;

                var value = jsObject.GetPropertyByName(propertyName);
                //if (value.HasValue == false)
                //    continue;

                //if (value.Type.IsRegExp())
                //    continue;

                var recursive = jsObject.StrictEquals(value);
                if (recursiveCall && recursive)
                    obj[propertyName] = null;
                else
                    obj[propertyName] = ToBlittableValue(value, CreatePropertyKey(propertyName, propertyKey), recursive);
            }
            return obj;
        }

        private object ToBlittableValue(JavaScriptValue v, string propertyKey, bool recursiveCall)
        {
            if (v.Type == JavaScriptValueType.Boolean)
                return _engine.Converter.ToBoolean(v);

            if (v.IsTruthy == false)
                return null;

            if (v.Type == JavaScriptValueType.String)
            {
                const string RavenDataByteArrayToBase64 = "raven-data:byte[];base64,";
                var valueAsObject = _engine.Converter.ToObject(v);
                var value = valueAsObject?.ToString();
                if (value != null && value.StartsWith(RavenDataByteArrayToBase64))
                {
                    value = value.Remove(0, RavenDataByteArrayToBase64.Length);
                    var byteArray = Convert.FromBase64String(value);
                    return Encoding.UTF8.GetString(byteArray);
                }
                return value;
            }

            if (v.Type == JavaScriptValueType.Number)
            {
                var num = _engine.Converter.ToDouble(v);

                KeyValuePair<object, JavaScriptValue> property;
                if (_propertiesByValue.TryGetValue(propertyKey, out property))
                {
                    var originalValue = property.Key;
                    if (originalValue is float || originalValue is int)
                    {
                        // If the current value is exactly as the original value, we can return the original value before we made the JS conversion, 
                        // which will convert a Int64 to jsFloat.
                        var jsValue = property.Value;
                        if (jsValue.Type == JavaScriptValueType.Number)
                        {
                            var oldValue = _engine.Converter.ToDouble(jsValue);
                            if (Math.Abs(num - oldValue) < double.Epsilon)
                                return originalValue;
                        }

                        //We might have change the type of num from Integer to long in the script by design 
                        //Making sure the number isn't a real float before returning it as integer
                        if (originalValue is int && (Math.Abs(num - Math.Floor(num)) <= double.Epsilon || Math.Abs(num - Math.Ceiling(num)) <= double.Epsilon))
                            return (long)num;
                        return num; //float
                    }
                }

                // If we don't have the type, assume that if the number ending with ".0" it actually an integer.
                var integer = Math.Truncate(num);
                if (Math.Abs(num - integer) < double.Epsilon)
                    return (long)integer;
                return num;
            }
            if (v.Type == JavaScriptValueType.Array)
            {
                var jsArray = (JavaScriptArray)v;
                var array = new DynamicJsonArray();

                foreach (var propertyNameValue in jsArray.GetOwnPropertyNames())
                {
                    var propertyName = propertyNameValue.ToString();

                    if (InheritedProperties.Contains(propertyName))
                        continue;

                    var jsInstance = jsArray.GetPropertyByName(propertyName);
                    //if (!jsInstance.HasValue)
                    //    continue;

                    var ravenJToken = ToBlittableValue(jsInstance, propertyKey + "[" + propertyName + "]", recursiveCall);
                    if (ravenJToken == null)
                        continue;

                    array.Add(ravenJToken);
                }

                return array;
            }
            if (v.Type == JavaScriptValueType.Date)
            {
                throw new NotImplementedException();
                //return v.AsDate().ToDateTime();
            }
            if (v.Type == JavaScriptValueType.Object)
            {
                return ToBlittable(v, propertyKey, recursiveCall);
            }
            //if (v.IsRegExp())
            //    return null;

            throw new NotSupportedException(v.Type.ToString());
        }

        public void Dispose()
        {
            _executionContext?.Dispose();
        }

        public virtual JavaScriptValue LoadDocument(IEnumerable<JavaScriptValue> arguments)
        {
            var args = arguments.ToList();
            if (args.Count < 1)
                throw new InvalidOperationException("Number of supplied arguments to 'LoadDocument' is too small.");

            var keyJs = args[0];

            if (keyJs.Type != JavaScriptValueType.String)
                throw new InvalidOperationException("Supplied document key must be a string.");

            var key = keyJs.ToString();

            var document = _database.DocumentsStorage.Get(_context, key);

            if (DebugMode)
                DebugActions.LoadDocument.Add(key);

            if (document == null)
                return _engine.NullValue;

            return ToJsObject(document.Data);
        }

        public virtual JavaScriptValue PutDocument(IEnumerable<JavaScriptValue> arguments)
        {
            var args = arguments.ToList();
            if (args.Count < 2)
                throw new InvalidOperationException("Number of supplied arguments to 'PutDocument' is too small.");

            var keyJs = args[0];
            var documentJs = args[1];
            JavaScriptValue metadataJs = null;
            if (args.Count > 2)
                metadataJs = args[2];

            JavaScriptValue etagJs = null;
            if (args.Count > 3)
                etagJs = args[3];

            if (keyJs.Type != JavaScriptValueType.String)
            {
                if (keyJs.Type != JavaScriptValueType.Object)
                    throw new InvalidOperationException("Supplied document key must be a string.");

                if (keyJs.SimpleEquals(_engine.NullValue) == false)
                    throw new InvalidOperationException("Supplied document key must be a string.");

                keyJs = null;
            }

            var key = keyJs?.ToString();

            if (documentJs.Type != JavaScriptValueType.Object || documentJs.SimpleEquals(_engine.NullValue))
                throw new InvalidOperationException($"Supplied document data for document '{key}' must be an non-null object.");

            if (metadataJs != null && metadataJs.Type != JavaScriptValueType.Object)
                throw new InvalidOperationException($"Supplied document metadata for document '{key}' must be an object.");

            if (etagJs != null && etagJs.Type != JavaScriptValueType.Number)
                throw new InvalidOperationException($"Supplied etag for document '{key}' must be a number.");

            var data = ToBlittable(documentJs);
            if (metadataJs != null)
                data["@metadata"] = ToBlittable(metadataJs);

            long? etag = null;
            if (etagJs != null)
                etag = _engine.Converter.ToInt32(etagJs);

            if (DebugMode)
            {
                DebugActions.PutDocument.Add(new DynamicJsonValue
                {
                    ["Key"] = key,
                    ["Etag"] = etag,
                    ["Data"] = data,
                });
            }

            var json = _context.ReadObject(data, key, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            var putResult = _database.DocumentsStorage.Put(_context, key, etag, json);

            if (putResult.Key == key)
                return keyJs;

            return _engine.Converter.FromString(putResult.Key);
        }

        //public virtual void DeleteDocument(string documentKey)
        //{
        //    throw new NotSupportedException("Deleting documents is not supported.");
        //}
    }
}