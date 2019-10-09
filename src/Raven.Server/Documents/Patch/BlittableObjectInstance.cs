﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using Jint;
using Jint.Native;
using Jint.Native.Array;
using Jint.Native.Object;
using Jint.Runtime.Descriptors;
using Jint.Runtime.Interop;
using Lucene.Net.Store;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries.Results;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Patch
{
    [DebuggerDisplay("Blittable JS object")]
    public class BlittableObjectInstance : ObjectInstance
    {
        public bool Changed;
        private readonly BlittableObjectInstance _parent;
        private readonly Document _doc;
        private bool _put;

        public readonly DateTime? LastModified;
        public readonly string ChangeVector;
        public readonly BlittableJsonReaderObject Blittable;
        public readonly string DocumentId;
        public HashSet<string> Deletes;
        public Dictionary<string, BlittableObjectProperty> OwnValues =
            new Dictionary<string, BlittableObjectProperty>();
        public Dictionary<string, BlittableJsonToken> OriginalPropertiesTypes;
        public Lucene.Net.Documents.Document LuceneDocument;
        public IState LuceneState;
        public Dictionary<string, IndexField> LuceneIndexFields;
        public bool LuceneAnyDynamicIndexFields;

        public double? Distance => _doc?.Distance;
        public float? Score => _doc?.IndexScore;

        private void MarkChanged()
        {
            Changed = true;
            _parent?.MarkChanged();
        }

        public ObjectInstance GetOrCreate(string key)
        {
            if (OwnValues.TryGetValue(key, out var property) == false)
            {
                property = GenerateProperty(key);

                OwnValues[key] = property;
                Deletes?.Remove(key);
            }

            return property.Value.AsObject();

            BlittableObjectProperty GenerateProperty(string propertyName)
            {
                var propertyIndex = Blittable.GetPropertyIndex(propertyName);

                var prop = new BlittableObjectProperty(this, propertyName);
                if (propertyIndex == -1)
                {
                    prop.Value = new ObjectInstance(Engine)
                    {
                        Extensible = true
                    };
                }

                return prop;
            }
        }

        public sealed class BlittableObjectProperty : PropertyDescriptor
        {
            private readonly BlittableObjectInstance _parent;
            private readonly string _property;
            private JsValue _value;
            public bool Changed;

            public override string ToString()
            {
                return _property;
            }

            public BlittableObjectProperty(BlittableObjectInstance parent, string property)
                : base(PropertyFlag.CustomJsValue | PropertyFlag.Writable | PropertyFlag.WritableSet | PropertyFlag.Enumerable | PropertyFlag.EnumerableSet)
            {
                _parent = parent;
                _property = property;

                if (TryGetValueFromLucene(_parent, _property, out _value) == false)
                {
                    var index = _parent.Blittable?.GetPropertyIndex(_property);
                    if (index == null || index == -1)
                    {
                        _value = JsValue.Undefined;
                    }
                    else
                    {
                        _value = GetPropertyValue(_property, index.Value);
                    }
                }
            }

            private bool TryGetValueFromLucene(BlittableObjectInstance parent, string property, out JsValue value)
            {
                value = null;

                if (parent.LuceneDocument == null || parent.LuceneIndexFields == null)
                    return false;

                if (parent.LuceneIndexFields.TryGetValue(_property, out var indexField) == false && parent.LuceneAnyDynamicIndexFields == false)
                    return false;

                if (indexField != null && indexField.Storage == FieldStorage.No)
                    return false;

                var fieldType = QueryResultRetrieverBase.GetFieldType(property, parent.LuceneDocument);
                if (fieldType.IsArray)
                {
                    // here we need to perform a manipulation in order to generate the object from the data
                    if (fieldType.IsJson)
                    {
                        Lucene.Net.Documents.Field[] propertyFields = parent.LuceneDocument.GetFields(property);

                        JsValue[] arrayItems =
                            new JsValue[propertyFields.Length];

                        for (int i = 0; i < propertyFields.Length; i++)
                        {
                            var field = propertyFields[i];
                            var stringValue = field.StringValue(parent.LuceneState);

                            var itemAsBlittable = parent.Blittable._context.ReadForMemory(stringValue, field.Name);

                            arrayItems[i] = TranslateToJs(parent, field.Name, BlittableJsonToken.StartObject, itemAsBlittable);
                        }

                        value = FromObject(parent.Engine, arrayItems);
                        return true;
                    }

                    var values = parent.LuceneDocument.GetValues(property, parent.LuceneState);
                    value = FromObject(parent.Engine, values);
                    return true;
                }

                var fieldable = _parent.LuceneDocument.GetFieldable(property);
                if (fieldable == null)
                    return false;

                var val = fieldable.StringValue(_parent.LuceneState);
                if (fieldType.IsJson)
                {
                    BlittableJsonReaderObject valueAsBlittable = parent.Blittable._context.ReadForMemory(val, property);
                    value = TranslateToJs(parent, property, BlittableJsonToken.StartObject, valueAsBlittable);
                    return true;
                }

                if (fieldable.IsTokenized == false)
                {
                    // NULL_VALUE and EMPTY_STRING fields aren't tokenized
                    // this will prevent converting fields with a "NULL_VALUE" string to null
                    switch (val)
                    {
                        case Client.Constants.Documents.Indexing.Fields.NullValue:
                            value = JsValue.Null;
                            return true;
                        case Client.Constants.Documents.Indexing.Fields.EmptyString:
                            value = string.Empty;
                            return true;
                    }
                }

                if (fieldType.IsNumeric)
                {
                    if (long.TryParse(val, out var valueAsLong))
                    {
                        value = valueAsLong;
                    }
                    else if (double.TryParse(val, out var valueAsDouble))
                    {
                        value = valueAsDouble;
                    }
                    else
                    {
                        throw new InvalidOperationException($"Recognized field '{property}' as numeric but was unable to parse its value to 'long' or 'double'. " +
                                                            $"documentId = '{parent.DocumentId}', value = {val}.");
                    }
                }
                else
                {
                    value = val;
                }

                return true;
            }

            protected override JsValue CustomValue
            {
                get => _value;
                set
                {
                    if (Equals(value, _value))
                        return;
                    _value = value;
                    _parent.MarkChanged();
                    Changed = true;
                }
            }

            private JsValue GetPropertyValue(string key, int propertyIndex)
            {
                var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();

                _parent.Blittable.GetPropertyByIndex(propertyIndex, ref propertyDetails, true);

                return TranslateToJs(_parent, key, propertyDetails.Token, propertyDetails.Value);
            }

            private ArrayInstance GetArrayInstanceFromBlittableArray(Engine e, BlittableJsonReaderArray bjra, BlittableObjectInstance parent)
            {
                bjra.NoCache = true;

                PropertyDescriptor[] items = new PropertyDescriptor[bjra.Length];
                for (var i = 0; i < bjra.Length; i++)
                {
                    var blit = bjra.GetValueTokenTupleByIndex(i);
                    BlittableJsonToken itemType = blit.Item2 & BlittableJsonReaderBase.TypesMask;
                    JsValue item;
                    if (itemType == BlittableJsonToken.Integer || itemType == BlittableJsonToken.LazyNumber)
                    {
                        item = TranslateToJs(null, null, blit.Item2, blit.Item1);
                    }
                    else
                    {
                        item = TranslateToJs(parent, null, blit.Item2, blit.Item1);
                    }
                    items[i] = new PropertyDescriptor(item, true, true, true);
                }

                var jsArray = new ArrayInstance(e, items);
                jsArray.Prototype = e.Array.PrototypeObject;
                jsArray.Extensible = true;

                return jsArray;
            }

            private JsValue TranslateToJs(BlittableObjectInstance owner, string key, BlittableJsonToken type, object value)
            {
                switch (type & BlittableJsonReaderBase.TypesMask)
                {
                    case BlittableJsonToken.Null:
                        return JsValue.Null;
                    case BlittableJsonToken.Boolean:
                        return (bool)value ? JsBoolean.True : JsBoolean.False;
                    case BlittableJsonToken.Integer:
                        // TODO: in the future, add [numeric type]TryFormat, when parsing numbers to strings
                        owner?.RecordNumericFieldType(key, BlittableJsonToken.Integer);
                        return (long)value;
                    case BlittableJsonToken.LazyNumber:
                        owner?.RecordNumericFieldType(key, BlittableJsonToken.LazyNumber);
                        return GetJSValueForLazyNumber(owner?.Engine, (LazyNumberValue)value);
                    case BlittableJsonToken.String:

                        return value.ToString();
                    case BlittableJsonToken.CompressedString:
                        return value.ToString();
                    case BlittableJsonToken.StartObject:
                        Changed = true;
                        _parent.MarkChanged();
                        BlittableJsonReaderObject blittable = (BlittableJsonReaderObject)value;
                        blittable.NoCache = true;
                        return new BlittableObjectInstance(owner.Engine,
                            owner,
                            blittable, null, null);
                    case BlittableJsonToken.StartArray:
                        Changed = true;
                        _parent.MarkChanged();
                        var bjra = (BlittableJsonReaderArray)value;
                        return GetArrayInstanceFromBlittableArray(owner.Engine, bjra, owner);
                    default:
                        throw new ArgumentOutOfRangeException(type.ToString());
                }
            }

            public static JsValue GetJSValueForLazyNumber(Engine engine, LazyNumberValue value)
            {
                // First, try and see if the number is withing double boundaries.
                // We use double's tryParse and it actually may round the number, 
                // But that are Jint's limitations
                if (value.TryParseDouble(out double doubleVal))
                {
                    return doubleVal;
                }

                // If number is not in double boundaries, we return the LazyNumberValue
                return new ObjectWrapper(engine, value);
            }

        }

        public BlittableObjectInstance(Engine engine,
            BlittableObjectInstance parent,
            BlittableJsonReaderObject blittable,
            string id, DateTime? lastModified, string changeVector = null) : base(engine)
        {
            _parent = parent;
            blittable.NoCache = true;
            LastModified = lastModified;
            ChangeVector = changeVector;
            Blittable = blittable;
            DocumentId = id;
            Prototype = engine.Object.PrototypeObject;
        }

        public BlittableObjectInstance(Engine engine,
            BlittableObjectInstance parent,
            BlittableJsonReaderObject blittable,
            Document doc) : this(engine,parent, blittable, doc.Id, doc.LastModified, doc.ChangeVector)
        {
            _doc = doc;
        }


        public override bool Delete(string propertyName, bool throwOnError)
        {
            if (Deletes == null)
                Deletes = new HashSet<string>();

            MarkChanged();
            Deletes.Add(propertyName);
            return OwnValues.Remove(propertyName);
        }

        public override PropertyDescriptor GetOwnProperty(string propertyName)
        {
            if (OwnValues.TryGetValue(propertyName, out var val))
                return val;

            Deletes?.Remove(propertyName);

            val = new BlittableObjectProperty(this, propertyName);

            if (val.Value.IsUndefined() &&
                DocumentId == null &&
                _put == false)
            {
                return PropertyDescriptor.Undefined;
            }

            OwnValues[propertyName] = val;

            return val;
        }

        public override void Put(string propertyName, JsValue value, bool throwOnError)
        {
            _put = true;
            try
            {
                base.Put(propertyName, value, throwOnError);
            }
            finally
            {
                _put = false;
            }
        }

        public override IEnumerable<KeyValuePair<string, PropertyDescriptor>> GetOwnProperties()
        {
            foreach (var value in OwnValues)
            {
                yield return new KeyValuePair<string, PropertyDescriptor>(value.Key, value.Value);
            }
            if (Blittable == null)
                yield break;
            foreach (var prop in Blittable.GetPropertyNames())
            {
                if (Deletes?.Contains(prop) == true)
                    continue;
                if (OwnValues.ContainsKey(prop))
                    continue;
                yield return new KeyValuePair<string, PropertyDescriptor>(
                    prop,
                    GetOwnProperty(prop)
                    );
            }
        }

        private void RecordNumericFieldType(string key, BlittableJsonToken type)
        {
            if (OriginalPropertiesTypes == null)
                OriginalPropertiesTypes = new Dictionary<string, BlittableJsonToken>();
            OriginalPropertiesTypes[key] = type;
        }
    }
}
