using System;
using System.Collections.Generic;
using Jint;
using Jint.Native;
using Jint.Native.Function;

namespace Raven.Server.Documents.Indexes.Static.JavaScript
{
    internal sealed class RecursiveJsFunction
    {
        private readonly List<JsValue> _result = new List<JsValue>();
        private readonly Engine _engine;
        private readonly JsValue _item;
        private readonly ScriptFunctionInstance _func;
        private readonly HashSet<JsValue> _results = new HashSet<JsValue>(JsValueComparer.Instance);
        private readonly Queue<object> _queue = new Queue<object>();

        public RecursiveJsFunction(Engine engine, JsValue item, ScriptFunctionInstance func)
        {
            _engine = engine ?? throw new ArgumentNullException(nameof(engine));
            _item = item;
            _func = func ?? throw new ArgumentNullException(nameof(func));
        }

        public JsValue Execute()
        {
            if (_item == null)
                return new JsArray(_engine);

            var current = NullIfEmptyEnumerable(_engine.Invoke(_func, _item));
            if (current == null)
            {
                _result.Add(_item);
                return new JsArray(_engine, _result.ToArray());
            }

            _queue.Enqueue(_item);
            while (_queue.Count > 0)
            {
                current = _queue.Dequeue();

                var list = current as IEnumerable<JsValue>;
                if (list != null)
                {
                    foreach (var o in list)
                        AddItem(o);
                }
                else if (current is JsValue currentJs)
                    AddItem(currentJs);
            }

            return new JsArray(_engine, _result.ToArray());
        }

        private void AddItem(JsValue current)
        {
            if (current.IsUndefined())
                return;

            if (_results.Add(current) == false)
                return;

            _result.Add(current);
            var result = NullIfEmptyEnumerable(_engine.Invoke(_func, current));
            if (result != null)
                _queue.Enqueue(result);
        }

        private static object NullIfEmptyEnumerable(JsValue item)
        {
            if (item.IsArray() == false)
                return item;

            var itemAsArray = item.AsArray();
            if (itemAsArray.Length == 0)
                return null;

            return Yield(itemAsArray);
        }

        private static IEnumerable<JsValue> Yield(JsArray array)
        {
            foreach (var item in array)
                yield return item;
        }

        private sealed class JsValueComparer : IEqualityComparer<JsValue>
        {
            public static readonly JsValueComparer Instance = new ();

            private JsValueComparer()
            {
            }

            public bool Equals(JsValue x, JsValue y)
            {
                if (ReferenceEquals(x, y)) return true;

                if (x is DynamicJsNull dx)
                    return dx.Equals(y);

                if (y is DynamicJsNull dy) 
                    return dy.Equals(x);

                if (x is null)
                    return y.Equals(null);

                if (y is null)
                    return x.Equals(null);

                return x.Equals(y);
            }

            public int GetHashCode(JsValue obj)
            {
                return obj.GetHashCode();
            }
        }
    }
}
