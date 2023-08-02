using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Exceptions.Cluster;
using Raven.Client.Exceptions.Database;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Raven.Server.Extensions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Utils;
using StringSegment = Sparrow.StringSegment;

namespace Raven.Server.Routing
{
    public delegate Task HandleRequest(RequestHandlerContext ctx);

    internal sealed class RouteInformation
    {
        public AuthorizationStatus AuthorizationStatus;
        public readonly EndpointType? EndpointType;

        public readonly string Method;
        public readonly string Path;

        public readonly bool SkipUsagesCount;
        public readonly bool SkipLastRequestTimeUpdate;
        public readonly CorsMode CorsMode;

        public bool DisableOnCpuCreditsExhaustion;
        public bool CheckForChanges = true;

        private HandleRequest _request;
        private HandleRequest _shardedRequest;
        private RouteType _typeOfRoute;

        public bool IsDebugInformationEndpoint;

        public enum RouteType
        {
            None,
            Databases
        }

        public RouteInformation(
            string method,
            string path,
            AuthorizationStatus authorizationStatus,
            EndpointType? endpointType,
            bool skipUsagesCount,
            bool skipLastRequestTimeUpdate,
            CorsMode corsMode,
            bool isDebugInformationEndpoint = false,
            bool disableOnCpuCreditsExhaustion = false,
            bool checkForChanges = true)
        {
            DisableOnCpuCreditsExhaustion = disableOnCpuCreditsExhaustion;
            AuthorizationStatus = authorizationStatus;
            IsDebugInformationEndpoint = isDebugInformationEndpoint;
            Method = method;
            EndpointType = endpointType;
            Path = path;
            SkipUsagesCount = skipUsagesCount;
            SkipLastRequestTimeUpdate = skipLastRequestTimeUpdate;
            CorsMode = corsMode;
            CheckForChanges = checkForChanges;
        }

        public RouteType TypeOfRoute => _typeOfRoute;
        
        public void BuildSharded(MethodInfo shardedAction)
        {
            _shardedRequest = BuildInternal(shardedAction);
        }
        
        public void Build(MethodInfo action)
        {
            if (typeof(DatabaseRequestHandler).IsAssignableFrom(action.DeclaringType))
            {
                _typeOfRoute = RouteType.Databases;
            }

            _request = BuildInternal(action);
        }

        
        private static HandleRequest BuildInternal(MethodInfo action)
        {
            if (action.ReturnType != typeof(Task))
                throw new InvalidOperationException(action.DeclaringType.FullName + "." + action.Name +
                                                    " must return Task");

            // CurrentRequestContext currentRequestContext
            var currentRequestContext = Expression.Parameter(typeof(RequestHandlerContext), "currentRequestContext");
            // new Handler(currentRequestContext)
            var constructorInfo = action.DeclaringType.GetConstructor(new Type[0]);
            var newExpression = Expression.New(constructorInfo);
            var handler = Expression.Parameter(action.DeclaringType, "handler");

            var block = Expression.Block(typeof(Task), new[] { handler },
                Expression.Assign(handler, newExpression),
                Expression.Call(handler, nameof(RequestHandler.Init), new Type[0], currentRequestContext),
                Expression.Call(handler, action.Name, new Type[0]));
            // .Handle();
            return Expression.Lambda<HandleRequest>(block, currentRequestContext).Compile();
        }

        public Task CreateDatabase(RequestHandlerContext context)
        {
            var databaseName = context.RouteMatch.GetCapture();

            // todo: think if we need to pass this check to the landlord
            if (context.RavenServer.ServerStore.IsPassive())
            {
                throw new NodeIsPassiveException($"Can't perform actions on the database '{databaseName}' while the node is passive.");
            }

            if (context.RavenServer.ServerStore.IdleDatabases.TryGetValue(databaseName.Value, out var replicationsDictionary))
            {
                if (context.HttpContext.Request.Query.TryGetValue("from-outgoing", out var dbId) && context.HttpContext.Request.Query.TryGetValue("etag", out var replicationEtag))
                {
                    var hasChanges = false;
                    var etag = Convert.ToInt64(replicationEtag);

                    if (replicationsDictionary.TryGetValue(dbId, out var storedEtag))
                    {
                        if (storedEtag < etag)
                            hasChanges = true;
                    }
                    else if (etag > 0)
                    {
                        hasChanges = true;
                    }

                    if (hasChanges == false &&
                        context.HttpContext.Request.Query.TryGetValue("nodeTag", out var nodeTag) &&
                        string.IsNullOrEmpty(nodeTag) == false)
                    {

                        DatabaseTopology topology = null;
                        using (context.RavenServer.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                        using (ctx.OpenReadTransaction())
                            topology = context.RavenServer.ServerStore.Cluster.ReadDatabaseTopology(ctx, databaseName.ToString());

                        if (topology != null && topology.Rehabs.Contains(nodeTag))
                        {
                            hasChanges = true;
                        }
                    }

                    if (hasChanges == false)
                        throw new DatabaseIdleException($"Replication attempt doesn't have changes to database {databaseName.Value}, which is currently idle.");
                }
            }

            var databasesLandlord = context.RavenServer.ServerStore.DatabasesLandlord;
            var result = databasesLandlord.TryGetOrCreateDatabase(databaseName);
            switch (result.DatabaseStatus)
            {
                case DatabasesLandlord.DatabaseSearchResult.Status.Sharded:
                    context.DatabaseContext = result.DatabaseContext;
                    if (context.DatabaseContext.DatabaseRecord.Sharding.Orchestrator.Topology.AllNodes.Contains(context.RavenServer.ServerStore.NodeTag) == false)
                        DatabaseDoesNotExistException.Throw(databaseName.Value);

                    return null;
                default:
                    var database = result.DatabaseTask;
                    if (database.IsCompletedSuccessfully)
                    {
                        context.Database = database.Result;

                        if (context.Database == null)
                            DatabaseDoesNotExistException.Throw(databaseName.Value);

                        // ReSharper disable once PossibleNullReferenceException
                        if (context.Database.DatabaseShutdownCompleted.IsSet)
                        {
                            using (context.RavenServer.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                            using (ctx.OpenReadTransaction())
                            {
                                if (context.RavenServer.ServerStore.Cluster.DatabaseExists(ctx, databaseName.Value))
                                {
                                    // db got disabled during loading
                                    throw new DatabaseDisabledException($"Cannot complete the request, because {databaseName.Value} has been disabled.");
                                }
                            }

                            // db got deleted during loading
                            DatabaseDoesNotExistException.ThrowWithMessage(databaseName.Value, "Cannot complete the request.");
                        }

                        return context.Database.DatabaseShutdown.IsCancellationRequested == false
                            ? Task.CompletedTask
                            : UnlikelyWaitForDatabaseToUnload(context, context.Database, databasesLandlord, databaseName);
                    }

                    return UnlikelyWaitForDatabaseToLoad(context, database, databasesLandlord, databaseName);
            }
        }

        private async Task UnlikelyWaitForDatabaseToUnload(RequestHandlerContext context, DocumentDatabase database,
            DatabasesLandlord databasesLandlord, StringSegment databaseName)
        {
            var time = databasesLandlord.DatabaseLoadTimeout;
            if (await database.DatabaseShutdownCompleted.WaitAsync(time) == false)
            {
                ThrowDatabaseUnloadTimeout(databaseName, databasesLandlord.DatabaseLoadTimeout);
            }
            await CreateDatabase(context);
        }

        private async Task UnlikelyWaitForDatabaseToLoad(RequestHandlerContext context, Task<DocumentDatabase> database,
            DatabasesLandlord databasesLandlord, StringSegment databaseName)
        {
            var time = databasesLandlord.DatabaseLoadTimeout;
            await Task.WhenAny(database, Task.Delay(time));
            if (database.IsCompleted == false)
            {
                if (databasesLandlord.InitLog.TryGetValue(databaseName.Value, out var initLogQueue))
                {
                    var sb = new StringBuilder();
                    foreach (var logline in initLogQueue)
                        sb.AppendLine(logline);

                    ThrowDatabaseLoadTimeoutWithLog(databaseName, databasesLandlord.DatabaseLoadTimeout, sb.ToString());
                }
                ThrowDatabaseLoadTimeout(databaseName, databasesLandlord.DatabaseLoadTimeout);
            }
            context.Database = await database;
            if (context.Database == null)
                DatabaseDoesNotExistException.Throw(databaseName.Value);
        }

        [DoesNotReturn]
        private static void ThrowDatabaseUnloadTimeout(StringSegment databaseName, TimeSpan timeout)
        {
            throw new DatabaseLoadTimeoutException($"Timeout when unloading database {databaseName} after {timeout}, try again later");
        }

        [DoesNotReturn]
        private static void ThrowDatabaseLoadTimeout(StringSegment databaseName, TimeSpan timeout)
        {
            throw new DatabaseLoadTimeoutException($"Timeout when loading database {databaseName} after {timeout}, try again later");
        }

        [DoesNotReturn]
        private static void ThrowDatabaseLoadTimeoutWithLog(StringSegment databaseName, TimeSpan timeout, string log)
        {
            throw new DatabaseLoadTimeoutException($"Database {databaseName} after {timeout} is still loading, try again later. Database initialization log: " + Environment.NewLine + log);
        }

        public Tuple<HandleRequest, Task<HandleRequest>> TryGetHandler(RequestHandlerContext context)
        {
            if (_typeOfRoute == RouteType.None)
            {
                return Tuple.Create<HandleRequest, Task<HandleRequest>>(_request, null);
            }
            var database = CreateDatabase(context);
            if (database == null)
            {
                if (_shardedRequest == null)
                {
                    DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Pawel, DevelopmentHelper.Severity.Critical, "Remove this!");
                    throw new InvalidOperationException($"Unable to run request {context.HttpContext.Request.GetFullUrl()}, the database is sharded, but no sharded route is defined for this operation!");
                }

                return Tuple.Create<HandleRequest, Task<HandleRequest>>(_shardedRequest, null);
            }

            if (database.Status == TaskStatus.RanToCompletion)
            {
                return Tuple.Create<HandleRequest, Task<HandleRequest>>(_request, null);
            }
            return Tuple.Create<HandleRequest, Task<HandleRequest>>(null, WaitForDb(database));
        }

        private async Task<HandleRequest> WaitForDb(Task databaseLoading)
        {
            await databaseLoading;

            return _request;
        }

        public HandleRequest GetRequestHandler()
        {
            return _request;
        }

        public HandleRequest GetShardedRequestHandler()
        {
            return _shardedRequest;
        }

        public override string ToString()
        {
            return $"{nameof(Method)}: {Method}, {nameof(Path)}: {Path}, {nameof(AuthorizationStatus)}: {AuthorizationStatus}";
        }
    }
}
