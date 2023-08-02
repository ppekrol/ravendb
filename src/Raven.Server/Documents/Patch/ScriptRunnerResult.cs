using System;
using Jint;
using Jint.Native;
using Jint.Native.Object;
using Sparrow.Json;

namespace Raven.Server.Documents.Patch
{
    internal sealed class ScriptRunnerResult : IDisposable
    {
        private readonly ScriptRunner.SingleRun _parent;

        public ScriptRunnerResult(ScriptRunner.SingleRun parent, JsValue instance)
        {
            _parent = parent;
            Instance = instance;
        }

        public readonly JsValue Instance;

        public ObjectInstance GetOrCreate(string property)
        {
            if (Instance.AsObject() is BlittableObjectInstance b)
                return b.GetOrCreate(property);
            var parent = Instance.AsObject();
            var o = parent.Get(property);
            if (o == null || o.IsUndefined() || o.IsNull())
            {
                o = new JsObject(_parent.ScriptEngine);
                parent.Set(property, o, false);
            }
            return o.AsObject();
        }

        public bool? BooleanValue => Instance.IsBoolean() ? Instance.AsBoolean() : (bool?)null;

        public bool IsNull => Instance == null || Instance.IsNull() || Instance.IsUndefined();
        public string StringValue => Instance.IsString() ? Instance.AsString() : null;
        public JsValue RawJsValue => Instance;

        public BlittableJsonReaderObject TranslateToObject(JsonOperationContext context, JsBlittableBridge.IResultModifier modifier = null, BlittableJsonDocumentBuilder.UsageMode usageMode = BlittableJsonDocumentBuilder.UsageMode.None)
        {
            if (IsNull)
                return null;

            var obj = Instance.AsObject();
            return JsBlittableBridge.Translate(context, _parent.ScriptEngine, obj, modifier, usageMode);
        }

        public void Dispose()
        {
            if (Instance is BlittableObjectInstance boi)
                boi.Reset();

            _parent?.JavaScriptUtils.Clear();
        }
    }
}
