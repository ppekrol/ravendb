using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Processors
{
    internal class ShardedStatsHandlerProcessorForGetDatabaseStatistics : AbstractStatsHandlerProcessorForGetDatabaseStatistics<ShardedRequestHandler, TransactionOperationContext>
    {
        public ShardedStatsHandlerProcessorForGetDatabaseStatistics([NotNull] ShardedRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override async ValueTask<DatabaseStatistics> GetDatabaseStatisticsAsync(string nodeTag)
        {
            ValidateNodeTag(nodeTag);
            var shardNumber = RequestHandler.GetIntValueQueryString("shardNumber", required: true).Value;

            var command = new GetStatisticsOperation.GetStatisticsCommand(debugTag: null, nodeTag);

            return await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber);
        }

        internal static IndexInformation[] GetDatabaseIndexesFromRecord(DatabaseRecord record)
        {
            var indexes = record.Indexes;
            var indexInformation = new IndexInformation[indexes.Count];

            int i = 0;
            foreach (var key in indexes.Keys)
            {
                var index = indexes[key];

                indexInformation[i] = new IndexInformation
                {
                    Name = index.Name,
                    // IndexDefinition includes nullable fields, then in case of null we set to default values
                    State = index.State ?? IndexState.Normal,
                    LockMode = index.LockMode ?? IndexLockMode.Unlock,
                    Priority = index.Priority ?? IndexPriority.Normal,
                    Type = index.Type,
                    SourceType = index.SourceType,
                    IsStale = false // for sharding we can't determine 
                };

                i++;
            }

            return indexInformation;
        }
    }
}
