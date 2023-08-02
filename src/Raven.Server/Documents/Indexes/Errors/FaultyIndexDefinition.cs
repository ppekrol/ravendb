using System;
using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.Documents.Indexes.Errors
{
    internal sealed class FaultyIndexDefinition : IndexDefinitionBaseServerSide<IndexField>
    {
        private readonly IndexDefinition _definition;

        public FaultyIndexDefinition(string name, IEnumerable<string> collections, IndexLockMode lockMode, IndexPriority priority, IndexState state, IndexField[] mapFields, IndexDefinition definition)
            : base(name, collections, lockMode, priority, state, mapFields, IndexVersion.CurrentVersion, definition.DeploymentMode, definition.ClusterState)
        {
            _definition = definition;
        }

        protected override void PersistMapFields(JsonOperationContext context, AbstractBlittableJsonTextWriter writer)
        {
            throw new NotSupportedException($"Definition of a faulty '{Name}' index does not support that");
        }

        protected override void PersistFields(JsonOperationContext context, AbstractBlittableJsonTextWriter writer)
        {
            throw new NotSupportedException($"Definition of a faulty '{Name}' index does not support that");
        }

        protected internal override IndexDefinition GetOrCreateIndexDefinitionInternal()
        {
            var definition = new IndexDefinition();
            _definition.CopyTo(definition);

            definition.Name = Name;
            definition.LockMode = LockMode;
            definition.Priority = Priority;
            definition.State = State;
            return definition;
        }

        protected override int ComputeRestOfHash(int hashCode)
        {
            return (hashCode * 397) ^ -1337;
        }

        public override IndexDefinitionCompareDifferences Compare(IndexDefinitionBaseServerSide indexDefinition)
        {
            return IndexDefinitionCompareDifferences.All;
        }

        public override IndexDefinitionCompareDifferences Compare(IndexDefinition indexDefinition)
        {
            return _definition.Compare(indexDefinition);
        }

        internal override void Reset()
        {
        }
    }
}
