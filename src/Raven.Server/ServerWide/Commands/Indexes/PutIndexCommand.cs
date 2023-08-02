using System;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Server.Extensions;
using Raven.Server.Rachis;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Indexes
{
    internal sealed class PutIndexCommand : UpdateDatabaseCommand
    {
        public IndexDefinition Definition;

        public IndexDeploymentMode? DefaultDeploymentMode;

        public PutIndexCommand()
        {
            // for deserialization
        }

        public PutIndexCommand(IndexDefinition definition, string databaseName, string source, DateTime createdAt, string uniqueRequestId, int revisionsToKeep, IndexDeploymentMode deploymentMode)
            : base(databaseName, uniqueRequestId)
        {
            Definition = definition;
            Definition.ClusterState ??= new IndexDefinitionClusterState();
            Source = source;
            CreatedAt = createdAt;
            RevisionsToKeep = revisionsToKeep;
            DefaultDeploymentMode = deploymentMode;
        }

        public DateTime CreatedAt { get; set; }

        public string Source { get; set; }
        
        public int RevisionsToKeep { get; set; }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            try
            {
                var indexNames = record.Indexes.Select(x => x.Value.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);

                if (indexNames.Add(Definition.Name) == false && record.Indexes.TryGetValue(Definition.Name, out var definition) == false)
                {
                    throw new InvalidOperationException($"Can not add index: {Definition.Name} because an index with the same name but different casing already exist");
                }

                record.AddIndex(Definition, Source, CreatedAt, etag, RevisionsToKeep, DefaultDeploymentMode ?? IndexDeploymentMode.Parallel);

            }
            catch (Exception e)
            {
                throw new RachisApplyException("Failed to update index", e);
            }
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Definition)] = Definition.ToJson();
            json[nameof(Source)] = Source;
            json[nameof(CreatedAt)] = CreatedAt;
            json[nameof(RevisionsToKeep)] = RevisionsToKeep;
            json[nameof(DefaultDeploymentMode)] = DefaultDeploymentMode;
        }

        public override string AdditionalDebugInformation(Exception exception)
        {
            var msg = $"Index name: '{Definition.Name}' for database '{DatabaseName}'";
            if (exception != null)
            {
                msg += $" Exception: {exception}.";
            }

            return msg;
        }
    }
}
