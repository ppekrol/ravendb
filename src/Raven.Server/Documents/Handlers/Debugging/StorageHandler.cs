using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data;
using Voron.Data.Fixed;
using Voron.Debugging;
using Voron.Impl;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class StorageHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/storage/trees", "GET", AuthorizationStatus.ValidUser, IsDebugInformationEndpoint = false)]
        public async Task Trees()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var tx = context.OpenReadTransaction())
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    await writer.WriteStartObjectAsync();

                    await writer.WritePropertyNameAsync("Results");
                    await writer.WriteStartArrayAsync();
                    var first = true;

                    foreach (var treeType in new[] { RootObjectType.VariableSizeTree, RootObjectType.FixedSizeTree, RootObjectType.EmbeddedFixedSizeTree })
                    {
                        foreach (var name in GetTreeNames(tx.InnerTransaction, treeType))
                        {
                            if (first == false)
                                await writer.WriteCommaAsync();

                            first = false;

                            await writer.WriteStartObjectAsync();

                            await writer.WritePropertyNameAsync("Name");
                            await writer.WriteStringAsync(name);
                            await writer.WriteCommaAsync();

                            await writer.WritePropertyNameAsync("Type");
                            await writer.WriteStringAsync(treeType.ToString());

                            await writer.WriteEndObjectAsync();
                        }
                    }

                    await writer.WriteEndArrayAsync();

                    await writer.WriteEndObjectAsync();
                }
            }
        }

        [RavenAction("/databases/*/debug/storage/btree-structure", "GET", AuthorizationStatus.ValidUser, IsDebugInformationEndpoint = false)]
        public Task BTreeStructure()
        {
            var treeName = GetStringQueryString("name", required: true);

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var tx = context.OpenReadTransaction())
            {
                var tree = tx.InnerTransaction.ReadTree(treeName)
                    ?? throw new InvalidOperationException("Tree name '" + treeName + "' was not found. Existing trees: " +
                        string.Join(", ", GetTreeNames(tx.InnerTransaction, RootObjectType.VariableSizeTree))
                    );

                HttpContext.Response.ContentType = "text/html";
                DebugStuff.DumpTreeToStream(tree, ResponseBodyStream());
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/debug/storage/fst-structure", "GET", AuthorizationStatus.ValidUser, IsDebugInformationEndpoint = false)]
        public Task FixedSizeTreeStructure()
        {
            var treeName = GetStringQueryString("name", required: true);

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var tx = context.OpenReadTransaction())
            {
                FixedSizeTree tree;
                try
                {
                    tree = tx.InnerTransaction.FixedTreeFor(treeName);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Existing trees: " +
                            string.Join(", ", GetTreeNames(tx.InnerTransaction, RootObjectType.FixedSizeTree))
                        , e);
                }

                HttpContext.Response.ContentType = "text/html";
                DebugStuff.DumpFixedSizedTreeToStream(tx.InnerTransaction.LowLevelTransaction, tree, ResponseBodyStream());
            }

            return Task.CompletedTask;
        }

        private IEnumerable<string> GetTreeNames(Transaction tx, RootObjectType type)
        {
            using (var rootIterator = tx.LowLevelTransaction.RootObjects.Iterate(false))
            {
                if (rootIterator.Seek(Slices.BeforeAllKeys) == false)
                    yield break;

                do
                {
                    if (tx.GetRootObjectType(rootIterator.CurrentKey) != type)
                        continue;

                    yield return rootIterator.CurrentKey.ToString();
                } while (rootIterator.MoveNext());
            }
        }

        [RavenAction("/databases/*/debug/storage/report", "GET", AuthorizationStatus.ValidUser, IsDebugInformationEndpoint = true)]
        public async Task Report()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    await writer.WriteStartObjectAsync();

                    await writer.WritePropertyNameAsync("BasePath");
                    await writer.WriteStringAsync(Database.Configuration.Core.DataDirectory.FullPath);
                    await writer.WriteCommaAsync();

                    await writer.WritePropertyNameAsync("Results");
                    await writer.WriteStartArrayAsync();
                    var first = true;
                    foreach (var env in Database.GetAllStoragesEnvironment())
                    {
                        if (first == false)
                            await writer.WriteCommaAsync();

                        first = false;

                        await writer.WriteStartObjectAsync();

                        await writer.WritePropertyNameAsync("Name");
                        await writer.WriteStringAsync(env.Name);
                        await writer.WriteCommaAsync();

                        await writer.WritePropertyNameAsync("Type");
                        await writer.WriteStringAsync(env.Type.ToString());
                        await writer.WriteCommaAsync();

                        var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(GetReport(env));
                        await writer.WritePropertyNameAsync("Report");
                        await writer.WriteObjectAsync(context.ReadObject(djv, env.Name));

                        await writer.WriteEndObjectAsync();
                    }

                    await writer.WriteEndArrayAsync();

                    await writer.WriteEndObjectAsync();
                }
            }
        }

        [RavenAction("/databases/*/debug/storage/all-environments/report", "GET", AuthorizationStatus.ValidUser, IsDebugInformationEndpoint = true)]
        public async Task AllEnvironmentsReport()
        {
            var name = GetStringQueryString("database");

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    await writer.WriteStartObjectAsync();

                    await writer.WritePropertyNameAsync("DatabaseName");
                    await writer.WriteStringAsync(name);
                    await writer.WriteCommaAsync();

                    await writer.WritePropertyNameAsync("Environments");
                    await writer.WriteStartArrayAsync();
                    await WriteAllEnvs(writer, context);
                    await writer.WriteEndArrayAsync();

                    await writer.WriteEndObjectAsync();
                }
            }
        }

        private async Task WriteAllEnvs(AsyncBlittableJsonTextWriter writer, DocumentsOperationContext context)
        {
            var envs = Database.GetAllStoragesEnvironment();

            bool first = true;
            foreach (var env in envs)
            {
                if (env == null)
                    continue;

                if (!first)
                    await writer.WriteCommaAsync();
                first = false;

                await writer.WriteStartObjectAsync();
                await writer.WritePropertyNameAsync("Environment");
                await writer.WriteStringAsync(env.Name);
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync("Type");
                await writer.WriteStringAsync(env.Type.ToString());
                await writer.WriteCommaAsync();

                var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(GetDetailedReport(env, false));
                await writer.WritePropertyNameAsync("Report");
                await writer.WriteObjectAsync(context.ReadObject(djv, env.Name));

                await writer.WriteEndObjectAsync();
            }
        }

        [RavenAction("/databases/*/debug/storage/environment/report", "GET", AuthorizationStatus.ValidUser)]
        public async Task EnvironmentReport()
        {
            var name = GetStringQueryString("name");
            var typeAsString = GetStringQueryString("type");
            var details = GetBoolValueQueryString("details", required: false) ?? false;

            if (Enum.TryParse(typeAsString, out StorageEnvironmentWithType.StorageEnvironmentType type) == false)
                throw new InvalidOperationException("Query string value 'type' is not a valid environment type: " + typeAsString);

            var env = Database.GetAllStoragesEnvironment()
                .FirstOrDefault(x => string.Equals(x.Name, name, StringComparison.OrdinalIgnoreCase) && x.Type == type);

            if (env == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return;
            }

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    await writer.WriteStartObjectAsync();

                    await writer.WritePropertyNameAsync("Name");
                    await writer.WriteStringAsync(env.Name);
                    await writer.WriteCommaAsync();

                    await writer.WritePropertyNameAsync("Type");
                    await writer.WriteStringAsync(env.Type.ToString());
                    await writer.WriteCommaAsync();

                    var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(GetDetailedReport(env, details));
                    await writer.WritePropertyNameAsync("Report");
                    await writer.WriteObjectAsync(context.ReadObject(djv, env.Name));

                    await writer.WriteEndObjectAsync();
                }
            }
        }

        private static StorageReport GetReport(StorageEnvironmentWithType environment)
        {
            using (var tx = environment.Environment.ReadTransaction())
            {
                return environment.Environment.GenerateReport(tx);
            }
        }

        private DetailedStorageReport GetDetailedReport(StorageEnvironmentWithType environment, bool details)
        {
            if (environment.Type != StorageEnvironmentWithType.StorageEnvironmentType.Index)
            {
                using (var tx = environment.Environment.ReadTransaction())
                {
                    return environment.Environment.GenerateDetailedReport(tx, details);
                }
            }

            var index = Database.IndexStore.GetIndex(environment.Name);
            return index.GenerateStorageReport(details);
        }
    }
}
