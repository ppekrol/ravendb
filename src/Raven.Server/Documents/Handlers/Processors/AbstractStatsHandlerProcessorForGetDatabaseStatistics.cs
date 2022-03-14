﻿using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Server.Json;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors
{
    internal abstract class AbstractStatsHandlerProcessorForGetDatabaseStatistics<TRequestHandler, TOperationContext> : AbstractHandlerReadProcessor<DatabaseStatistics, TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        protected AbstractStatsHandlerProcessorForGetDatabaseStatistics([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> operationContext) : base(requestHandler, operationContext)
        {
        }

        protected override RavenCommand<DatabaseStatistics> CreateCommandForNode(string nodeTag)
        {
            return new GetStatisticsOperation.GetStatisticsCommand(debugTag: null, nodeTag);
        }

        protected override async ValueTask WriteResultAsync(DatabaseStatistics result)
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                writer.WriteDatabaseStatistics(context, result);
        }
    }
}
