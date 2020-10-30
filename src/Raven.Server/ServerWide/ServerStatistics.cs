using System;
using System.IO;
using System.Threading.Tasks;
using Raven.Client.Util;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.ServerWide
{
    public class ServerStatistics
    {
        private static readonly TimeSpan PersistFrequency = TimeSpan.FromMinutes(15);

        private DateTime _lastPersist;

        public ServerStatistics()
        {
            StartUpTime = _lastPersist = SystemTime.UtcNow;
        }

        [JsonDeserializationIgnore]
        public TimeSpan UpTime => SystemTime.UtcNow - StartUpTime;

        [JsonDeserializationIgnore]
        public readonly DateTime StartUpTime;

        public DateTime? LastRequestTime;

        public DateTime? LastAuthorizedNonClusterAdminRequestTime;

        public async Task WriteTo(AsyncBlittableJsonTextWriter writer)
        {
            await  writer.WriteStartObjectAsync();

            await  writer.WritePropertyNameAsync(nameof(UpTime));
            await  writer.WriteStringAsync(UpTime.ToString("c"));
            await  writer.WriteCommaAsync();

            await  writer.WritePropertyNameAsync(nameof(StartUpTime));
            await  writer.WriteDateTimeAsync(StartUpTime, isUtc: true);
            await  writer.WriteCommaAsync();

            await  writer.WritePropertyNameAsync(nameof(LastRequestTime));
            if (LastRequestTime.HasValue)
                await  writer.WriteDateTimeAsync(LastRequestTime.Value, isUtc: true);
            else
                await  writer.WriteNullAsync();
            await  writer.WriteCommaAsync();

            await  writer.WritePropertyNameAsync(nameof(LastAuthorizedNonClusterAdminRequestTime));
            if (LastAuthorizedNonClusterAdminRequestTime.HasValue)
                await  writer.WriteDateTimeAsync(LastAuthorizedNonClusterAdminRequestTime.Value, isUtc: true);
            else
                await  writer.WriteNullAsync();

            await  writer.WriteEndObjectAsync();
        }

        internal unsafe void Load(TransactionContextPool contextPool, Logger logger)
        {
            try
            {
                using (contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var tree = tx.InnerTransaction.ReadTree(nameof(ServerStatistics));
                    if (tree == null)
                        return;

                    var result = tree.Read(nameof(ServerStatistics));
                    if (result == null)
                        return;

                    using (var json = context.ReadForMemory(result.Reader.AsStream(), nameof(ServerStatistics)))
                    {
                        var stats = JsonDeserializationServer.ServerStatistics(json);

                        LastRequestTime = stats.LastRequestTime;
                        LastAuthorizedNonClusterAdminRequestTime = stats.LastAuthorizedNonClusterAdminRequestTime;
                    }
                }
            }
            catch (Exception e)
            {
                if (logger.IsInfoEnabled)
                    logger.Info("Could not load server statistics.", e);
            }
        }

        internal void Persist(TransactionContextPool contextPool, Logger logger)
        {
            if (contextPool == null)
                return;

            lock (this)
            {
                try
                {
                    using (contextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (var tx = context.OpenWriteTransaction())
                    {
                        using (var ms = new MemoryStream())
                        {
                            var writer = new AsyncBlittableJsonTextWriter(context, ms);
                            try
                            {
                                WriteTo(writer).ConfigureAwait(false).GetAwaiter().GetResult();
                                writer.FlushAsync().AsTask().ConfigureAwait(false).GetAwaiter().GetResult();

                                ms.Position = 0;

                                var tree = tx.InnerTransaction.CreateTree(nameof(ServerStatistics));
                                tree.Add(nameof(ServerStatistics), ms);

                                tx.Commit();
                            }
                            finally
                            {
                                writer.DisposeAsync().AsTask().ConfigureAwait(false).GetAwaiter().GetResult();
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    if (logger.IsInfoEnabled)
                        logger.Info("Could not persist server statistics.", e);
                }
            }
        }

        internal void MaybePersist(TransactionContextPool contextPool, Logger logger)
        {
            var now = SystemTime.UtcNow;
            if (now - _lastPersist <= PersistFrequency)
                return;

            try
            {
                Persist(contextPool, logger);
            }
            finally
            {
                _lastPersist = now;
            }
        }
    }
}
