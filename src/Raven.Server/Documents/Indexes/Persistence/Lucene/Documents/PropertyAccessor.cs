﻿using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;
using Jint;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime.Interop;
using Microsoft.CSharp.RuntimeBinder;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents
{
    public delegate object DynamicGetter(object target);

    public class PropertyAccessor : IPropertyAccessor
    {
        private readonly Dictionary<string, Accessor> Properties = new Dictionary<string, Accessor>();

        private readonly List<KeyValuePair<string, Accessor>> _propertiesInOrder =
            new List<KeyValuePair<string, Accessor>>();

        public IEnumerable<(string Key, object Value, CompiledIndexField GroupByField, bool IsGroupByField)> GetProperties(object target)
        {
            foreach ((var key, var value) in _propertiesInOrder)
            {
                yield return (key, value.GetValue(target), value.GroupByField, value.IsGroupByField);
            }
        }

        public static IPropertyAccessor Create(Type type, object instance)
        {
            if (type == typeof(JsObject))
                return new JintPropertyAccessor(null);

            if (instance is Dictionary<string, object> dict)
                return DictionaryAccessor.Create(dict);

            return new PropertyAccessor(type);
        }

        public object GetValue(string name, object target)
        {
            if (Properties.TryGetValue(name, out Accessor accessor))
                return accessor.GetValue(target);

            throw new InvalidOperationException(string.Format("The {0} property was not found", name));
        }

        protected PropertyAccessor(Type type, Dictionary<string, CompiledIndexField> groupByFields = null)
        {
            var isValueType = type.IsValueType;
            foreach (var prop in type.GetProperties())
            {
                var getMethod = isValueType
                    ? (Accessor)CreateGetMethodForValueType(prop, type)
                    : CreateGetMethodForClass(prop, type);

                if (groupByFields != null)
                {
                    foreach (var groupByField in groupByFields.Values)
                    {
                        if (groupByField.IsMatch(prop.Name))
                        {
                            getMethod.GroupByField = groupByField;
                            getMethod.IsGroupByField = true;
                            break;
                        }
                    }
                }

                Properties.Add(prop.Name, getMethod);
                _propertiesInOrder.Add(new KeyValuePair<string, Accessor>(prop.Name, getMethod));
            }
        }

        private static ValueTypeAccessor CreateGetMethodForValueType(PropertyInfo prop, Type type)
        {
            var binder = Microsoft.CSharp.RuntimeBinder.Binder.GetMember(CSharpBinderFlags.None, prop.Name, type, new[] { CSharpArgumentInfo.Create(CSharpArgumentInfoFlags.None, null) });
            return new ValueTypeAccessor(CallSite<Func<CallSite, object, object>>.Create(binder));
        }

        private static ClassAccessor CreateGetMethodForClass(PropertyInfo propertyInfo, Type type)
        {
            var getMethod = propertyInfo.GetGetMethod();

            if (getMethod == null)
                throw new InvalidOperationException(string.Format("Could not retrieve GetMethod for the {0} property of {1} type", propertyInfo.Name, type.FullName));

            var arguments = new[]
            {
                typeof (object)
            };

            var getterMethod = new DynamicMethod(string.Concat("_Get", propertyInfo.Name, "_"), typeof(object), arguments, propertyInfo.DeclaringType);
            var generator = getterMethod.GetILGenerator();

            generator.DeclareLocal(typeof(object));
            generator.Emit(OpCodes.Ldarg_0);
            generator.Emit(OpCodes.Castclass, propertyInfo.DeclaringType);
            generator.EmitCall(OpCodes.Callvirt, getMethod, null);

            if (propertyInfo.PropertyType.IsClass == false)
                generator.Emit(OpCodes.Box, propertyInfo.PropertyType);

            generator.Emit(OpCodes.Ret);

            return new ClassAccessor((DynamicGetter)getterMethod.CreateDelegate(typeof(DynamicGetter)));
        }

        private class ValueTypeAccessor : Accessor
        {
            private readonly CallSite<Func<CallSite, object, object>> _callSite;

            public ValueTypeAccessor(CallSite<Func<CallSite, object, object>> callSite)
            {
                _callSite = callSite;
            }

            public override object GetValue(object target)
            {
                return _callSite.Target(_callSite, target);
            }
        }

        private class ClassAccessor : Accessor
        {
            private readonly DynamicGetter _dynamicGetter;

            public ClassAccessor(DynamicGetter dynamicGetter)
            {
                _dynamicGetter = dynamicGetter;
            }

            public override object GetValue(object target)
            {
                return _dynamicGetter(target);
            }
        }

        public abstract class Accessor
        {
            public abstract object GetValue(object target);

            public bool IsGroupByField;

            public CompiledIndexField GroupByField;
        }

        internal static IPropertyAccessor CreateMapReduceOutputAccessor(Type type, object instance, Dictionary<string, CompiledIndexField> groupByFields, bool isObjectInstance = false)
        {
            if (isObjectInstance || type == typeof(JsObject) || type.IsSubclassOf(typeof(ObjectInstance)))
                return new JintPropertyAccessor(groupByFields);

            if (instance is Dictionary<string, object> dict)
                return DictionaryAccessor.Create(dict, groupByFields);

            return new PropertyAccessor(type, groupByFields);
        }
    }

    internal class JintPropertyAccessor : IPropertyAccessor
    {
        private readonly Dictionary<string, CompiledIndexField> _groupByFields;

        public JintPropertyAccessor(Dictionary<string, CompiledIndexField> groupByFields)
        {
            _groupByFields = groupByFields;
        }

        public IEnumerable<(string Key, object Value, CompiledIndexField GroupByField, bool IsGroupByField)> GetProperties(object target)
        {
            if (!(target is ObjectInstance oi))
                throw new ArgumentException($"JintPropertyAccessor.GetPropertiesInOrder is expecting a target of type ObjectInstance but got one of type {target.GetType().Name}.");

            foreach (var property in oi.GetOwnProperties())
            {
                var propertyAsString = property.Key.AsString();

                CompiledIndexField field = null;
                var isGroupByField = _groupByFields?.TryGetValue(propertyAsString, out field) ?? false;

                yield return (propertyAsString, GetValue(property.Value.Value), field, isGroupByField);
            }
        }

        public object GetValue(string name, object target)
        {
            if (!(target is ObjectInstance oi))
                throw new ArgumentException($"JintPropertyAccessor.GetValue is expecting a target of type ObjectInstance but got one of type {target.GetType().Name}.");
            if (oi.HasOwnProperty(name) == false)
                throw new MissingFieldException($"The target for 'JintPropertyAccessor.GetValue' doesn't contain the property {name}.");
            return GetValue(oi.GetProperty(name).Value);
        }

        private static object GetValue(JsValue jsValue)
        {
            if (jsValue.IsNull())
                return null;
            if (jsValue.IsString())
                return jsValue.AsString();
            if (jsValue.IsBoolean())
                return jsValue.AsBoolean();
            if (jsValue.IsNumber())
                return jsValue.AsNumber();
            if (jsValue.IsDate())
                return jsValue.AsDate();
            if (jsValue is ObjectWrapper ow)
            {
                var target = ow.Target;
                switch (target)
                {
                    case LazyStringValue lsv:
                        return lsv;

                    case LazyCompressedStringValue lcsv:
                        return lcsv;

                    case LazyNumberValue lnv:
                        return lnv; //should be already blittable supported type.
                }
                ThrowInvalidObject(jsValue);
            }
            else if (jsValue.IsArray())
            {
                var arr = jsValue.AsArray();
                var array = new object[arr.Length];
                var i = 0;
                foreach (var val in arr)
                {
                    array[i++] = GetValue(val);
                }

                return array;
            }
            else if (jsValue.IsObject())
            {
                return jsValue.AsObject();
            }
            if (jsValue.IsUndefined())
            {
                return null;
            }

            ThrowInvalidObject(jsValue);
            return null;
        }

        [DoesNotReturn]
        private static void ThrowInvalidObject(JsValue jsValue)
        {
            throw new NotSupportedException($"Was requested to extract the value out of a JsValue object but could not figure its type, value={jsValue}");
        }
    }
}
