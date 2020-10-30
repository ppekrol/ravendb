﻿using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Extensions;
using Raven.Server.Json;

using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents.Indexes.Static
{
    public class MapIndexDefinition : IndexDefinitionBase<IndexField>
    {
        private readonly bool _hasDynamicFields;
        private readonly bool _hasCompareExchange;

        public readonly IndexDefinition IndexDefinition;

        public MapIndexDefinition(IndexDefinition definition, IEnumerable<string> collections, string[] outputFields, bool hasDynamicFields, bool hasCompareExchange, long indexVersion)
            : base(definition.Name, collections, definition.LockMode ?? IndexLockMode.Unlock, definition.Priority ?? IndexPriority.Normal, GetFields(definition, outputFields), indexVersion)
        {
            _hasDynamicFields = hasDynamicFields;
            _hasCompareExchange = hasCompareExchange;
            IndexDefinition = definition;
        }

        public override bool HasDynamicFields => _hasDynamicFields;

        public override bool HasCompareExchange => _hasCompareExchange;

        private static IndexField[] GetFields(IndexDefinition definition, string[] outputFields)
        {
            definition.Fields.TryGetValue(Constants.Documents.Indexing.Fields.AllFields, out IndexFieldOptions allFields);

            var result = definition.Fields
                .Where(x => x.Key != Constants.Documents.Indexing.Fields.AllFields)
                .Select(x => IndexField.Create(x.Key, x.Value, allFields)).ToList();

            foreach (var outputField in outputFields)
            {
                if (definition.Fields.ContainsKey(outputField))
                    continue;

                result.Add(IndexField.Create(outputField, new IndexFieldOptions(), allFields));
            }

            return result.ToArray();
        }

        protected override void PersistFields(JsonOperationContext context, AsyncBlittableJsonTextWriter writer)
        {
            var builder = IndexDefinition.ToJson();
            using (var json = context.ReadObject(builder, nameof(IndexDefinition), BlittableJsonDocumentBuilder.UsageMode.ToDisk))
            {
                writer.WritePropertyNameAsync(nameof(IndexDefinition));
                writer.WriteObjectAsync(json);
            }
        }

        protected override void PersistMapFields(JsonOperationContext context, AsyncBlittableJsonTextWriter writer)
        {
            writer.WritePropertyNameAsync(nameof(MapFields));
            writer.WriteStartArrayAsync();
            var first = true;
            foreach (var field in MapFields.Values.Select(x => x.As<IndexField>()))
            {
                if (first == false)
                    writer.WriteCommaAsync();

                writer.WriteStartObjectAsync();

                writer.WritePropertyNameAsync(nameof(field.Name));
                writer.WriteStringAsync(field.Name);
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(field.Indexing));
                writer.WriteStringAsync(field.Indexing.ToString());

                writer.WriteEndObjectAsync();

                first = false;
            }
            writer.WriteEndArrayAsync();
        }

        protected internal override IndexDefinition GetOrCreateIndexDefinitionInternal()
        {
            var definition = IndexDefinition.Clone();
            definition.Name = Name;
            definition.Type = IndexDefinition.Type;
            definition.LockMode = LockMode;
            definition.Priority = Priority;
            return definition;
        }

        public override IndexDefinitionCompareDifferences Compare(IndexDefinitionBase indexDefinition)
        {
            return IndexDefinitionCompareDifferences.All;
        }

        public override IndexDefinitionCompareDifferences Compare(IndexDefinition indexDefinition)
        {
            return IndexDefinition.Compare(indexDefinition);
        }

        protected override int ComputeRestOfHash(int hashCode)
        {
            return hashCode * 397 ^ IndexDefinition.GetHashCode();
        }

        public static IndexDefinition Load(StorageEnvironment environment, out long version)
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var tx = environment.ReadTransaction())
            {
                using (var stream = GetIndexDefinitionStream(environment, tx))
                using (var reader = context.ReadForDisk(stream, "index/def"))
                {
                    var definition = ReadIndexDefinition(reader);
                    definition.Name = ReadName(reader);
                    definition.LockMode = ReadLockMode(reader);
                    definition.Priority = ReadPriority(reader);

                    version = ReadVersion(reader);

                    return definition;
                }
            }
        }

        private static IndexDefinition ReadIndexDefinition(BlittableJsonReaderObject reader)
        {
            if (reader.TryGet(nameof(IndexDefinition), out BlittableJsonReaderObject jsonObject) == false || jsonObject == null)
                throw new InvalidOperationException("No persisted definition");

            return JsonDeserializationServer.IndexDefinition(jsonObject);
        }
    }
}
