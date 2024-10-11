using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using Raven.Client.Documents.Session;
using Sparrow;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Sync;

namespace Raven.Client.Json
{
    internal static class BlittableOperation
    {
        private static readonly Lazy<JsonOperationContext> Context = new Lazy<JsonOperationContext>(JsonOperationContext.ShortTermSingleUse);

        private static readonly LazyStringValue LastModified;
        private static readonly LazyStringValue Collection;
        private static readonly LazyStringValue ChangeVector;
        private static readonly LazyStringValue Id;

        static BlittableOperation()
        {
            LastModified = Context.Value.GetLazyString(Constants.Documents.Metadata.LastModified);
            Collection = Context.Value.GetLazyString(Constants.Documents.Metadata.Collection);
            ChangeVector = Context.Value.GetLazyString(Constants.Documents.Metadata.ChangeVector);
            Id = Context.Value.GetLazyString(Constants.Documents.Metadata.Id);
        }

        public static bool EntityChanged(BlittableJsonReaderObject newObj, DocumentInfo documentInfo, IDictionary<string, DocumentsChanges[]> changes)
        {
            var docChanges = changes != null ? new List<DocumentsChanges>() : null;

            if (documentInfo.IsNewDocument == false && documentInfo.Document != null)
                return CompareBlittable(string.Empty, documentInfo.Id, documentInfo.Document, newObj, changes, docChanges);

            if (changes == null)
                return true;

            NewChange(null, null, null, null, docChanges, DocumentsChanges.ChangeType.DocumentAdded);
            changes[documentInfo.Id] = docChanges.ToArray();
            return true;
        }

        private static unsafe bool CompareBlittable(string fieldPath, string id, BlittableJsonReaderObject originalBlittable,
            BlittableJsonReaderObject newBlittable, IDictionary<string, DocumentsChanges[]> changes,
            List<DocumentsChanges> docChanges)
        {
            BlittableJsonReaderObject.AssertNoModifications(originalBlittable, id, assertChildren: false);
            BlittableJsonReaderObject.AssertNoModifications(newBlittable, id, assertChildren: false);

            var newBlittableProps = newBlittable.GetPropertyNames();
            var oldBlittableProps = originalBlittable.GetPropertyNames();
            var newFields = new HashSet<string>(newBlittableProps);
            newFields.ExceptWith(oldBlittableProps);
            var removedFields = new HashSet<string>(oldBlittableProps);
            removedFields.ExceptWith(newBlittableProps);

            using var orderedProperties = newBlittable.GetPropertiesByInsertionOrder();

            var newProp = new BlittableJsonReaderObject.PropertyDetails();
            var oldProp = new BlittableJsonReaderObject.PropertyDetails();

            foreach (var field in removedFields)
            {
                if (field.Equals(LastModified) ||
                    field.Equals(ChangeVector) ||
                    field.Equals(Id))
                    continue;
                if (changes == null)
                    return true;

                var oldPropId = originalBlittable.GetPropertyIndex(field);
                originalBlittable.GetPropertyByIndex(oldPropId, ref oldProp);
                NewChange(fieldPath, field, null, oldProp.Value, docChanges, DocumentsChanges.ChangeType.RemovedField);
            }

            for (int i = 0; i < orderedProperties.Size; i++)
            {
                newBlittable.GetPropertyByIndex(orderedProperties.Properties[i], ref newProp);

                if (newProp.Name.Equals(LastModified) ||
                    newProp.Name.Equals(Collection) ||
                    newProp.Name.Equals(ChangeVector) ||
                    newProp.Name.Equals(Id))
                    continue;

                if (newFields.Contains(newProp.Name))
                {
                    if (changes == null)
                        return true;
                    NewChange(fieldPath, newProp.Name, newProp.Value, null, docChanges, DocumentsChanges.ChangeType.NewField);
                    continue;
                }

                var oldPropId = originalBlittable.GetPropertyIndex(newProp.Name);
                originalBlittable.GetPropertyByIndex(oldPropId, ref oldProp);

                switch ((newProp.Token & BlittableJsonReaderBase.TypesMask))
                {
                    case BlittableJsonToken.Integer:
                    case BlittableJsonToken.Boolean:
                    case BlittableJsonToken.LazyNumber:
                    case BlittableJsonToken.CompressedString:
                    case BlittableJsonToken.String:
                        if (newProp.Value.Equals(oldProp.Value) || CompareValues(oldProp, newProp) ||
                            CompareStringsWithEscapePositions(newBlittable._context, oldProp, newProp))
                            break;
                        if (changes == null)
                            return true;
                        NewChange(fieldPath, newProp.Name, newProp.Value, oldProp.Value, docChanges,
                            DocumentsChanges.ChangeType.FieldChanged);
                        break;
                    case BlittableJsonToken.Null:
                        if (oldProp.Value == null)
                            break;
                        if (changes == null)
                            return true;
                        NewChange(fieldPath, newProp.Name, null, oldProp.Value, docChanges,
                            DocumentsChanges.ChangeType.FieldChanged);
                        break;
                    case BlittableJsonToken.StartArray:
                        var newArray = newProp.Value as BlittableJsonReaderArray;
                        var oldArray = oldProp.Value as BlittableJsonReaderArray;

                        if (newArray == null)
                            throw new InvalidDataException($"Invalid blittable, expected array but got {newProp.Value}");

                        if (oldArray == null)
                        {
                            if (changes == null)
                                return true;

                            NewChange(fieldPath, newProp.Name, newProp.Value, oldProp.Value, docChanges,
                                DocumentsChanges.ChangeType.FieldChanged);

                            break;
                        }

                        var changed = CompareBlittableArray(FieldPathCombine(fieldPath, newProp.Name), id, oldArray, newArray, changes, docChanges, newProp.Name);
                        if (changes == null && changed)
                            return true;

                        break;
                    case BlittableJsonToken.StartObject:
                        if (oldProp.Value == null ||
                            !(oldProp.Value is BlittableJsonReaderObject oldObj))
                        {
                            if (changes == null)
                                return true;

                            NewChange(fieldPath, newProp.Name, newProp.Value, oldProp.Value, docChanges,
                                DocumentsChanges.ChangeType.FieldChanged);
                            break;
                        }

                        if (!(newProp.Value is BlittableJsonReaderObject newObj))
                            throw new InvalidDataException($"Invalid blittable, expected object but got {newProp.Value}");

                        changed = CompareBlittable(FieldPathCombine(fieldPath, newProp.Name), id, oldObj, newObj, changes, docChanges);
                        if (changes == null && changed)
                            return true;

                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }

            if ((changes == null) || (docChanges.Count <= 0))
                return false;

            changes[id] = docChanges.ToArray();
            return true;
        }

        private static unsafe bool CompareStringsWithEscapePositions(JsonOperationContext context, BlittableJsonReaderObject.PropertyDetails oldProp,
            BlittableJsonReaderObject.PropertyDetails newProp)
        {
            // this is called if the values are NOT equal, but we need to check if the oldProp was read from network and already resolved 
            // the escape characters

            if (oldProp.Value is LazyStringValue lsv)
            {
                int pos = lsv.Size;
                int numOfEscapePositions = BlittableJsonReaderBase.ReadVariableSizeInt(lsv.Buffer, ref pos);
                if (numOfEscapePositions == 0)
                    return false;

                using (var memoryStream = RecyclableMemoryStreamFactory.GetRecyclableStream())
                {
                    using (var textWriter = new BlittableJsonTextWriter(context, memoryStream))
                    {
                        textWriter.WriteString(lsv);
                        textWriter.Flush();
                    }
                    memoryStream.TryGetBuffer(out var bytes);
                    fixed (byte* pBuff = bytes.Array)
                    {
                        // need to ignore the quote marks
                        using var str = context.AllocateStringValue(null, pBuff + bytes.Offset + 1, (int)memoryStream.Length - 2);

                        return newProp.Value.Equals(str);
                    }
                }
            }
            return false;
        }

        private static string FieldPathCombine(string path1, string path2)
            => string.IsNullOrEmpty(path1) ? path2 : path1 + "." + path2;

        private static bool CompareValues(BlittableJsonReaderObject.PropertyDetails oldProp, BlittableJsonReaderObject.PropertyDetails newProp)
        {
            if (newProp.Token == BlittableJsonToken.Integer && oldProp.Token == BlittableJsonToken.LazyNumber)
            {
                var @long = (long)newProp.Value;
                var @double = ((LazyNumberValue)oldProp.Value).ToDouble(CultureInfo.InvariantCulture);

                return @double % 1 == 0 && @long.Equals((long)@double);
            }

            if (oldProp.Token == BlittableJsonToken.Integer && newProp.Token == BlittableJsonToken.LazyNumber)
            {
                var @long = (long)oldProp.Value;
                var @double = ((LazyNumberValue)newProp.Value).ToDouble(CultureInfo.InvariantCulture);

                return @double % 1 == 0 && @long.Equals((long)@double);
            }

            if (oldProp.Token == BlittableJsonToken.LazyNumber && newProp.Token == BlittableJsonToken.LazyNumber)
            {
                var oldDouble = ((LazyNumberValue)oldProp.Value).ToDouble(CultureInfo.InvariantCulture);
                var newDouble = ((LazyNumberValue)newProp.Value).ToDouble(CultureInfo.InvariantCulture);

                return oldDouble.AlmostEquals(newDouble);
            }

            return false;
        }

        private static bool CompareBlittableArray(string fieldPath, string id, BlittableJsonReaderArray oldArray, BlittableJsonReaderArray newArray,
            IDictionary<string, DocumentsChanges[]> changes, List<DocumentsChanges> docChanges, LazyStringValue propName)
        {
            // if we don't care about the changes
            if (oldArray.Length != newArray.Length && changes == null)
                return true;

            var position = 0;
            var changed = false;
            while (position < oldArray.Length && position < newArray.Length && (changed == false || changes != null))
            {
                switch (oldArray[position])
                {
                    case BlittableJsonReaderObject bjro1:
                        if (newArray[position] is BlittableJsonReaderObject bjro2)
                        {
                            changed |= CompareBlittable(AddIndexFieldPath(fieldPath, position), id, bjro1, bjro2, changes, docChanges);
                        }
                        else
                        {
                            changed = true;
                            if (changes != null)
                            {
                                NewChange(AddIndexFieldPath(fieldPath, position), propName, newArray[position], oldArray[position], docChanges,
                                    DocumentsChanges.ChangeType.ArrayValueChanged);
                            }

                        }
                        break;
                    case BlittableJsonReaderArray bjra1:
                        if (newArray[position] is BlittableJsonReaderArray bjra2)
                        {
                            changed |= CompareBlittableArray(AddIndexFieldPath(fieldPath, position), id, bjra1, bjra2, changes, docChanges, propName);
                        }
                        else
                        {
                            changed = true;
                            if (changes != null)
                            {
                                NewChange(AddIndexFieldPath(fieldPath, position), propName, newArray[position], oldArray[position], docChanges,
                                    DocumentsChanges.ChangeType.ArrayValueChanged);
                            }
                        }
                        break;
                    case null:
                        if (newArray[position] != null)
                        {
                            changed = true;
                            if (changes != null)
                            {
                                NewChange(AddIndexFieldPath(fieldPath, position), propName, newArray[position], oldArray[position], docChanges,
                                    DocumentsChanges.ChangeType.ArrayValueChanged);
                            }
                        }
                        break;
                    default:
                        if (oldArray[position].Equals(newArray[position]) == false)
                        {
                            if (changes != null)
                            {
                                NewChange(AddIndexFieldPath(fieldPath, position), propName, newArray[position], oldArray[position], docChanges,
                                    DocumentsChanges.ChangeType.ArrayValueChanged);
                            }
                            changed = true;
                        }
                        break;
                }

                position++;
            }

            if (changes == null)
                return changed;

            // if one of the arrays is larger than the other
            while (position < oldArray.Length)
            {
                NewChange(fieldPath, propName, null, oldArray[position], docChanges,
                    DocumentsChanges.ChangeType.ArrayValueRemoved);
                position++;
            }

            while (position < newArray.Length)
            {
                NewChange(fieldPath, propName, newArray[position], null, docChanges,
                    DocumentsChanges.ChangeType.ArrayValueAdded);
                position++;
            }

            return changed;
        }

        private static string AddIndexFieldPath(string fieldPath, int position)
        {
            return fieldPath + $"[{position}]";
        }

        private static void NewChange(string fieldPath, string name, object newValue, object oldValue, List<DocumentsChanges> docChanges,
            DocumentsChanges.ChangeType change)
        {
            docChanges.Add(new DocumentsChanges
            {
                FieldName = name,
                FieldNewValue = newValue,
                FieldOldValue = oldValue,
                Change = change,
                FieldPath = fieldPath
            });
        }
    }
}
