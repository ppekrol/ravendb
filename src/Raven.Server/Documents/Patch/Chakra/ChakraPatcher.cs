using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Microsoft.Scripting.JavaScript;
using Raven.Abstractions.Data;
using Raven.Client.Extensions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Patch.Chakra
{
    public class ChakraPatcher : IDisposable
    {
        private static readonly IEnumerable<JavaScriptObject> EmptyArgs = Enumerable.Empty<JavaScriptObject>();

        private JavaScriptRuntime _runtime;
        private JavaScriptEngine _engine;
        private JavaScriptFunction _patchFn;
        private PatchRequest _patchRequest;

        private TimeSpan _timeout;

        public ChakraPatcher(JavaScriptRuntime runtime, JavaScriptEngine engine, JavaScriptFunction patchFn)
        {
            _runtime = runtime;
            _engine = engine;
            _patchFn = patchFn;
        }

        public ChakraPatcherOperationScope Prepare(DocumentDatabase database, DocumentsOperationContext context, PatchRequest patchRequest, bool debugMode)
        {
            var scope = new ChakraPatcherOperationScope(database, _engine, context, debugMode);

            _engine.SetGlobalFunction("PutDocument", (engine, constructor, thisValue, arguments) => scope.PutDocument(arguments));
            _engine.SetGlobalFunction("LoadDocument", (engine, constructor, thisValue, arguments) => scope.LoadDocument(arguments));
            _patchRequest = patchRequest;
            _timeout = Debugger.IsAttached
                ? TimeSpan.FromMinutes(15)
                : database.Configuration.Patching.Timeout.AsTimeSpan;

            if (_patchRequest.Values != null)
            {
                for (var i = 0; i < _patchRequest.Values.Count; i++)
                {
                    var property = _patchRequest.Values.GetPropertyByIndex(i);
                    var propertyValue = scope.ToJsValue(property.Item2, property.Item3);

                    _engine.GlobalObject.SetPropertyByName(property.Item1, propertyValue);
                }
            }

            return scope;
        }

        public BlittableJsonReaderObject Patch(Document document, JsonOperationContext context, ChakraPatcherOperationScope scope)
        {
            _engine.SetGlobalVariable(Constants.Indexing.Fields.DocumentIdFieldName, _engine.Converter.FromString(document.Key));

            var input = scope.ToJsObject(document.Data);

            Timer timer = null;
            var timeout = false;
            try
            {
                timer = new Timer(state =>
                {
                    _runtime.DisableExecution();
                }, null, _timeout, TimeSpan.FromDays(7));

                _patchFn.Call(input, EmptyArgs);
            }
            catch (Exception e)
            {
                if (e.Message != "A script was terminated.")
                    throw;

                timeout = true;
                throw new ChakraTimeoutException(_timeout, e);
            }
            finally
            {
                timer?.Dispose();

                if (timeout)
                    _engine.Runtime.EnableExecution();

                _engine.AssertNoExceptions();
            }

            var output = scope.ToBlittable(input);
            return context.ReadObject(output, document.Key, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
        }

        public void OutputLog(ChakraPatcherOperationScope scope)
        {
            var numberOfOutputsJs = _engine.GlobalObject.GetPropertyByName("number_of_outputs");
            var numberOfOutputs = _engine.Converter.ToInt32(numberOfOutputsJs);
            if (numberOfOutputs == 0)
                return;

            var arr = (JavaScriptArray)_engine.GlobalObject.GetPropertyByName("debug_outputs");

            foreach (var property in arr)
            {
                //if (property.Key == "length")
                //    continue;

                //var jsInstance = property.Value.Value;
                //if (!jsInstance.HasValue)
                //    continue;

                //var value = jsInstance.Value;
                string output = null;
                switch (property.Type)
                {
                    case JavaScriptValueType.Boolean:
                        output = _engine.Converter.ToBoolean(property).ToString();
                        break;
                    case JavaScriptValueType.Number:
                        output = _engine.Converter.ToInt32(property).ToInvariantString();
                        break;
                    case JavaScriptValueType.String:
                        output = property.ToString();
                        break;
                    case JavaScriptValueType.Undefined:
                        output = property.ToString();
                        break;
                    case JavaScriptValueType.Object:
                        if (property.IsTruthy)
                            output = property.ToString();
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }

                if (output != null)
                    scope.DebugInfo.Add(output);
            }

            _engine.CallGlobalFunction("clear_debug_outputs");
        }

        public void Dispose()
        {
            if (_patchRequest.Values != null)
            {
                for (var i = 0; i < _patchRequest.Values.Count; i++)
                {
                    var property = _patchRequest.Values.GetPropertyByIndex(i);

                    _engine.GlobalObject.DeletePropertyByName(property.Item1);
                }

                _patchRequest = null;
            }

            _patchFn?.Dispose();
            _patchFn = null;

            _engine?.Dispose();
            _engine = null;

            _runtime?.Dispose();
            _runtime = null;
        }

        public class Result
        {
            public BlittableJsonReaderObject Document;
            public PatchDebugActions DebugActions;
            public DynamicJsonArray DebugInfo;
        }
    }
}