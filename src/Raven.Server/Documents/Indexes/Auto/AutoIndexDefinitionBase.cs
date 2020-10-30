using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Auto
{
    public abstract class AutoIndexDefinitionBase : IndexDefinitionBase<AutoIndexField>
    {
        public IndexState State { get; set; }

        protected AutoIndexDefinitionBase(string indexName, string collection, AutoIndexField[] fields, long? indexVersion = null)
            : base(indexName, new [] { collection }, IndexLockMode.Unlock, IndexPriority.Normal, fields, indexVersion ?? IndexVersion.CurrentVersion)
        {
            if (string.IsNullOrEmpty(collection))
                throw new ArgumentNullException(nameof(collection));
        }

        protected abstract override void PersistFields(JsonOperationContext context, AsyncBlittableJsonTextWriter writer);

        protected override void PersistMapFields(JsonOperationContext context, AsyncBlittableJsonTextWriter writer)
        {
            writer.WritePropertyNameAsync(nameof(MapFields));
            writer.WriteStartArrayAsync();
            var first = true;
            foreach (var field in MapFields.Values.Select(x => x.As<AutoIndexField>()))
            {
                if (first == false)
                    writer.WriteCommaAsync();

                writer.WriteStartObjectAsync();

                writer.WritePropertyNameAsync(nameof(field.Name));
                writer.WriteStringAsync(field.Name);
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(field.Indexing));
                writer.WriteStringAsync(field.Indexing.ToString());
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(field.Aggregation));
                writer.WriteIntegerAsync((int)field.Aggregation);
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(field.Spatial));
                if (field.Spatial == null)
                    writer.WriteNullAsync();
                else
                    writer.WriteObjectAsync(DocumentConventions.DefaultForServer.Serialization.DefaultConverter.ToBlittable(field.Spatial, context));
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(field.HasSuggestions));
                writer.WriteBool(field.HasSuggestions);

                writer.WriteEndObjectAsync();

                first = false;
            }
            writer.WriteEndArrayAsync();
        }

        protected internal abstract override IndexDefinition GetOrCreateIndexDefinitionInternal();

        public abstract override IndexDefinitionCompareDifferences Compare(IndexDefinitionBase indexDefinition);

        public abstract override IndexDefinitionCompareDifferences Compare(IndexDefinition indexDefinition);

        protected abstract override int ComputeRestOfHash(int hashCode);
    }
}
