using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Json.Serialization;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Client.Http
{
    internal static class DatabaseTopologyLocalCache
    {
        private static readonly Logger _logger = LoggingSource.Instance.GetLogger("Client", typeof(DatabaseTopologyLocalCache).FullName);

        private static void Clear(string path)
        {
            try
            {
                if (File.Exists(path) == false)
                    return;

                File.Delete(path);
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Could not clear the persisted database topology", e);
            }
        }

        private static string GetPath(string databaseName, string topologyHash, DocumentConventions conventions)
        {
            return Path.Combine(conventions.TopologyCacheLocation, $"{databaseName}.{topologyHash}.raven-database-topology");
        }

        public static Topology TryLoad(string databaseName, string topologyHash, DocumentConventions conventions, JsonOperationContext context)
        {
            var path = GetPath(databaseName, topologyHash, conventions);
            return TryLoad(path, context);
        }

        private static Topology TryLoad(string path, JsonOperationContext context)
        {
            try
            {
                if (File.Exists(path) == false)
                    return null;

                using (var stream = SafeFileStream.Create(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                using (var json = context.Read(stream, "raven-database-topology"))
                {
                    return JsonDeserializationClient.Topology(json);
                }
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Could not understand the persisted database topology", e);
                return null;
            }
        }

        public static async Task TrySavingAsync(string databaseName, string topologyHash, Topology topology, DocumentConventions conventions, JsonOperationContext context, CancellationToken token)
        {
            try
            {
                var path = GetPath(databaseName, topologyHash, conventions);
                if (topology == null)
                {
                    Clear(path);
                    return;
                }

                var existingTopology = TryLoad(path, context);
                if (existingTopology?.Etag >= topology.Etag)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Skipping save topology with etag {topology.Etag} to cache " +
                                     $"as the cache already have a topology with etag: {existingTopology.Etag}");
                    return;
                }

                using (var stream = SafeFileStream.Create(path, FileMode.Create, FileAccess.Write, FileShare.Read))
                await using (var writer = new AsyncBlittableJsonTextWriter(context, stream))
                {
                    await writer.WriteStartObjectAsync().ConfigureAwait(false);

                    await writer.WritePropertyNameAsync(context.GetLazyString(nameof(topology.Nodes))).ConfigureAwait(false);
                    await writer.WriteStartArrayAsync().ConfigureAwait(false);
                    for (var i = 0; i < topology.Nodes.Count; i++)
                    {
                        var node = topology.Nodes[i];
                        if (i != 0)
                            await writer.WriteCommaAsync().ConfigureAwait(false);
                        await WriteNodeAsync(writer, node, context).ConfigureAwait(false);
                    }
                    await writer.WriteEndArrayAsync().ConfigureAwait(false);

                    await writer.WriteCommaAsync().ConfigureAwait(false);
                    await writer.WritePropertyNameAsync(context.GetLazyString(nameof(topology.Etag))).ConfigureAwait(false);
                    await writer.WriteIntegerAsync(topology.Etag).ConfigureAwait(false);

                    await writer.WriteCommaAsync().ConfigureAwait(false);
                    await writer.WritePropertyNameAsync(context.GetLazyString("PersistedAt")).ConfigureAwait(false);
                    await writer.WriteStringAsync(DateTimeOffset.UtcNow.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite)).ConfigureAwait(false);

                    await writer.WriteEndObjectAsync().ConfigureAwait(false);
                }
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Could not persist the database topology", e);
            }
        }

        private static async ValueTask WriteNodeAsync(AsyncBlittableJsonTextWriter writer, ServerNode node, JsonOperationContext context)
        {
            await writer.WriteStartObjectAsync().ConfigureAwait(false);

            await writer.WritePropertyNameAsync(context.GetLazyString(nameof(ServerNode.Url))).ConfigureAwait(false);
            await writer.WriteStringAsync(context.GetLazyString(node.Url)).ConfigureAwait(false);

            await writer.WriteCommaAsync().ConfigureAwait(false);
            await writer.WritePropertyNameAsync(context.GetLazyString(nameof(ServerNode.Database))).ConfigureAwait(false);
            await writer.WriteStringAsync(context.GetLazyString(node.Database)).ConfigureAwait(false);

            // ClusterTag and ServerRole included for debugging purpose only
            await writer.WriteCommaAsync().ConfigureAwait(false);
            await writer.WritePropertyNameAsync(context.GetLazyString(nameof(ServerNode.ClusterTag))).ConfigureAwait(false);
            await writer.WriteStringAsync(context.GetLazyString(node.ClusterTag)).ConfigureAwait(false);

            await writer.WriteCommaAsync().ConfigureAwait(false);
            await writer.WritePropertyNameAsync(context.GetLazyString(nameof(ServerNode.ServerRole))).ConfigureAwait(false);
            await writer.WriteStringAsync(context.GetLazyString(node.ServerRole.ToString())).ConfigureAwait(false);

            await writer.WriteEndObjectAsync().ConfigureAwait(false);
        }
    }
}
