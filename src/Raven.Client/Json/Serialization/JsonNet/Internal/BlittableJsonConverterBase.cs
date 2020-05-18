﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Conventions;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Json.Serialization.JsonNet.Internal
{
    internal abstract class BlittableJsonConverterBase : IBlittableJsonConverterBase
    {
        protected readonly DocumentConventions Conventions;

        protected BlittableJsonConverterBase(DocumentConventions conventions)
        {
            Conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
        }

        public void PopulateEntity(object entity, BlittableJsonReaderObject json)
        {
            var jsonSerializer = Conventions.Serialization.CreateSerializer();
            PopulateEntity(entity, json, jsonSerializer);
        }

        public void PopulateEntity(object entity, BlittableJsonReaderObject json, IJsonSerializer jsonSerializer)
        {
            if (entity == null)
                throw new ArgumentNullException(nameof(entity));
            if (json == null)
                throw new ArgumentNullException(nameof(json));
            if (jsonSerializer == null)
                throw new ArgumentNullException(nameof(jsonSerializer));

            var serializer = (JsonNetJsonSerializer)jsonSerializer;
            var old = serializer.ObjectCreationHandling;
            serializer.ObjectCreationHandling = ObjectCreationHandling.Replace;

            try
            {
                using (var reader = new BlittableJsonReader())
                {
                    reader.Initialize(json);

                    serializer.Populate(reader, entity);
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Could not populate entity.", ex);
            }
            finally
            {
                serializer.ObjectCreationHandling = old;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected static BlittableJsonReaderObject ToBlittableInternal(
             object entity,
             DocumentConventions conventions,
             JsonOperationContext context,
             IJsonSerializer serializer,
             IJsonWriter writer,
             bool removeIdentityProperty = true)
        {
            serializer.Serialize(writer, entity);

            writer.FinalizeDocument();
            var reader = writer.CreateReader();

            var type = entity.GetType();
            var isDynamicObject = entity is IDynamicMetaObjectProvider;

            var changes = removeIdentityProperty && TryRemoveIdentityProperty(reader, type, conventions, isDynamicObject);
            changes |= TrySimplifyJson(reader, type);

            if (changes)
            {
                using (var old = reader)
                {
                    reader = context.ReadObject(reader, "convert/entityToBlittable");
                }
            }

            return reader;
        }

        private static bool TryRemoveIdentityProperty(BlittableJsonReaderObject document, Type entityType, DocumentConventions conventions, bool isDynamicObject)
        {
            var identityProperty = conventions.GetIdentityProperty(entityType);
            if (identityProperty == null)
            {
                if (conventions.AddIdFieldToDynamicObjects && isDynamicObject)
                {
                    if (document.Modifications == null)
                        document.Modifications = new DynamicJsonValue(document);

                    document.Modifications.Remove("Id");
                    return true;
                }

                return false;
            }

            if (document.Modifications == null)
                document.Modifications = new DynamicJsonValue(document);

            document.Modifications.Remove(identityProperty.Name);
            return true;
        }

        private static bool TrySimplifyJson(BlittableJsonReaderObject document, Type rootType)
        {
            var simplified = false;
            foreach (var propertyName in document.GetPropertyNames())
            {
                var propertyType = GetPropertyType(propertyName, rootType);
                if (propertyType == typeof(JObject) || propertyType == typeof(JArray) || propertyType == typeof(JValue))
                {
                    // don't simplify the property if it's a JObject
                    continue;
                }

                var propertyValue = document[propertyName];

                if (propertyValue is BlittableJsonReaderArray propertyArray)
                {
                    simplified |= TrySimplifyJson(propertyArray, propertyType);
                    continue;
                }

                var propertyObject = propertyValue as BlittableJsonReaderObject;
                if (propertyObject == null)
                    continue;

                if (propertyObject.TryGet(Constants.Json.Fields.Type, out string type) == false)
                {
                    simplified |= TrySimplifyJson(propertyObject, propertyType);
                    continue;
                }

                if (ShouldSimplifyJsonBasedOnType(type) == false)
                    continue;

                simplified = true;

                if (document.Modifications == null)
                    document.Modifications = new DynamicJsonValue(document);

                if (propertyObject.TryGet(Constants.Json.Fields.Values, out BlittableJsonReaderArray values) == false)
                {
                    if (propertyObject.Modifications == null)
                        propertyObject.Modifications = new DynamicJsonValue(propertyObject);

                    propertyObject.Modifications.Remove(Constants.Json.Fields.Type);
                    continue;
                }

                document.Modifications[propertyName] = values;

                simplified |= TrySimplifyJson(values, propertyType);
            }

            return simplified;
        }

        private static bool TrySimplifyJson(BlittableJsonReaderArray array, Type rootType)
        {
            var itemType = GetItemType();

            var simplified = false;
            foreach (var item in array)
            {
                var itemObject = item as BlittableJsonReaderObject;
                if (itemObject == null)
                    continue;

                simplified |= TrySimplifyJson(itemObject, itemType);
            }

            return simplified;

            Type GetItemType()
            {
                if (rootType == null)
                    return null;

                if (rootType.IsArray)
                    return rootType.GetElementType();

                var enumerableInterface = rootType.GetInterface(typeof(IEnumerable<>).Name);
                if (enumerableInterface == null)
                    return null;

                return enumerableInterface.GenericTypeArguments[0];
            }
        }

        private static bool ShouldSimplifyJsonBasedOnType(string typeValue)
        {
            var type = Type.GetType(typeValue);

            if (type == null)
                return false;

            if (type.IsArray)
                return true;

            if (type.GetGenericArguments().Length == 0)
                return type == typeof(Enumerable);

            return typeof(IEnumerable).IsAssignableFrom(type.GetGenericTypeDefinition());
        }

        internal static Type GetPropertyType(string propertyName, Type rootType)
        {
            if (rootType == null)
                return null;

            MemberInfo memberInfo = null;
            try
            {
                memberInfo = ReflectionUtil.GetPropertyOrFieldFor(rootType, BindingFlags.Public | BindingFlags.Instance | BindingFlags.NonPublic, propertyName);
            }
            catch (AmbiguousMatchException)
            {
                var memberInfos = ReflectionUtil.GetPropertiesAndFieldsFor(rootType, BindingFlags.Public | BindingFlags.Instance | BindingFlags.NonPublic)
                    .Where(x => x.Name == propertyName)
                    .ToList();

                while (typeof(object) != rootType)
                {
                    memberInfo = memberInfos.FirstOrDefault(x => x.DeclaringType == rootType);
                    if (memberInfo != null)
                        break;

                    if (rootType.BaseType == null)
                        break;

                    rootType = rootType.BaseType;
                }
            }

            switch (memberInfo)
            {
                case PropertyInfo pi:
                    return pi.PropertyType;
                case FieldInfo fi:
                    return fi.FieldType;
                default:
                    return null;
            }
        }
    }
}
