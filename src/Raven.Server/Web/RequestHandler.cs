﻿using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Microsoft.AspNetCore.WebUtilities;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Cluster;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.Util;
using Raven.Server.Extensions;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.TrafficWatch;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Web
{
    public abstract partial class RequestHandler
    {
        public const string StartParameter = "start";

        public const string PageSizeParameter = "pageSize";

        private RequestHandlerContext _context;

        internal HttpContext HttpContext
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _context.HttpContext; }
        }

        public RavenServer Server
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _context.RavenServer; }
        }
        public ServerStore ServerStore
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _context.RavenServer.ServerStore; }
        }
        public RouteMatch RouteMatch
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _context.RouteMatch; }
        }

        public X509Certificate2 GetCurrentCertificate()
        {
            var feature = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;
            return feature?.Certificate;
        }

        public CancellationToken AbortRequestToken
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _context.HttpContext.RequestAborted; }
        }

        public virtual void Init(RequestHandlerContext context)
        {
            _context = context;
            context.HttpContext.Response.OnStarting(() => CheckForChanges(context));
        }

        public abstract Task CheckForChanges(RequestHandlerContext context);

        internal Stream TryGetRequestFromStream(string itemName)
        {
            if (HttpContext.Request.HasFormContentType == false)
                return null;

            if (HttpContext.Request.Form.TryGetValue(itemName, out Microsoft.Extensions.Primitives.StringValues value) == false)
                return null;

            if (value.Count == 0)
                return null;

            return new MemoryStream(Encoding.UTF8.GetBytes(value[0]));
        }

        private Stream _requestBodyStream;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Stream RequestBodyStream()
        {
            if (_requestBodyStream != null)
                return _requestBodyStream;
            _requestBodyStream = new StreamWithTimeout(GetDecompressedStream(HttpContext.Request.Body, HttpContext.Request.Headers));

            if (TrafficWatchManager.HasRegisteredClients)
            {
                HttpContext.Items["RequestStream"] = _requestBodyStream;
            }

            _context.HttpContext.Response.RegisterForDispose(_requestBodyStream);

            return _requestBodyStream;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Stream GetBodyStream(MultipartSection section)
        {
            Stream stream = new StreamWithTimeout(GetDecompressedStream(section.Body, section.Headers));
            _context.HttpContext.Response.RegisterForDispose(stream);
            return stream;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static Stream GetDecompressedStream(Stream stream, IDictionary<string, Microsoft.Extensions.Primitives.StringValues> headers)
        {
            var httpCompressionAlgorithm = GetHttpCompressionAlgorithmFromHeaders(headers, Constants.Headers.ContentEncoding);

            switch (httpCompressionAlgorithm)
            {
                case HttpCompressionAlgorithm.Gzip:
                    return GetGzipStream(stream, CompressionMode.Decompress);
                case HttpCompressionAlgorithm.Brotli:
                    return new BrotliStream(stream, CompressionMode.Decompress);
                case null:
                    return stream;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected static GZipStream GetGzipStream(Stream stream, CompressionMode mode, CompressionLevel level = CompressionLevel.Fastest)
        {
            GZipStream gZipStream =
                mode == CompressionMode.Compress ?
                    new GZipStream(stream, level, true) :
                    new GZipStream(stream, mode, true);
            return gZipStream;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected bool ClientAcceptsGzipResponse()
        {

            return
                Server.Configuration.Http.UseResponseCompression &&
                (HttpContext.Request.IsHttps == false ||
                    (HttpContext.Request.IsHttps && Server.Configuration.Http.AllowResponseCompressionOverHttps)) &&
                GetHttpCompressionAlgorithmFromHeaders(HttpContext.Request.Headers, Constants.Headers.AcceptEncoding) == HttpCompressionAlgorithm.Gzip;
        }

        private static HttpCompressionAlgorithm? GetHttpCompressionAlgorithmFromHeaders(IDictionary<string, Microsoft.Extensions.Primitives.StringValues> headers, string encodingsHeader)
        {
            if (headers.TryGetValue(encodingsHeader, out Microsoft.Extensions.Primitives.StringValues acceptedContentEncodings) == false)
                return null;

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var encoding in acceptedContentEncodings)
            {
                if (encoding.Contains("br"))
                    return HttpCompressionAlgorithm.Brotli;

                if (encoding.Contains("gzip"))
                    return HttpCompressionAlgorithm.Gzip;
            }

            return null;
        }

        public static void ValidateNodeForAddingToDb(string databaseName, string node, DatabaseRecord databaseRecord, ClusterTopology clusterTopology, RavenServer server, string baseMessage = null)
        {
            baseMessage ??= "Can't execute the operation";

            var databaseIsBeenDeleted = databaseRecord.DeletionInProgress != null &&
                                        databaseRecord.DeletionInProgress.TryGetValue(node, out var deletionInProgress) &&
                                        deletionInProgress != DeletionInProgressStatus.No;
            if (databaseIsBeenDeleted)
                throw new InvalidOperationException($"{baseMessage}, because the database {databaseName} is currently being deleted from node {node} (which is in the new topology)");

            var url = clusterTopology.GetUrlFromTag(node);
            if (url == null)
                throw new InvalidOperationException($"{baseMessage}, because node {node} (which is in the new topology) is not part of the cluster");

            if (databaseRecord.Encrypted && url.StartsWith("https:", StringComparison.OrdinalIgnoreCase) == false && server.AllowEncryptedDatabasesOverHttp == false)
                throw new InvalidOperationException($"{baseMessage}, because database {databaseName} is encrypted but node {node} (which is in the new topology) doesn't have an SSL certificate.");
        }

        /// <summary>
        /// puts the given string in TrafficWatch property of HttpContext.Items
        /// puts the given type in TrafficWatchChangeType property of HttpContext.Items
        /// </summary>
        /// <param name="str"></param>
        /// <param name="type"></param>
        public void AddStringToHttpContext(string str, TrafficWatchChangeType type)
        {
            HttpContext.Items["TrafficWatch"] = (str, type);
        }

        protected async Task WaitForExecutionOnSpecificNode(JsonOperationContext context, ClusterTopology clusterTopology, string node, long index)
        {
            await ServerStore.Cluster.WaitForIndexNotification(index); // first let see if we commit this in the leader

            using (var requester = ClusterRequestExecutor.CreateForShortTermUse(clusterTopology.GetUrlFromTag(node), ServerStore.Server.Certificate.Certificate, DocumentConventions.DefaultForServer))
            {
                await requester.ExecuteAsync(new WaitForRaftIndexCommand(index), context, token: AbortRequestToken);
            }
        }

        protected internal async Task WaitForExecutionOnRelevantNodes(JsonOperationContext context, string database, ClusterTopology clusterTopology, List<string> members, long index)
        {
            await ServerStore.Cluster.WaitForIndexNotification(index); // first let see if we commit this in the leader
            if (members.Count == 0)
                throw new InvalidOperationException("Cannot wait for execution when there are no nodes to execute ON.");

            var executors = new List<ClusterRequestExecutor>();

            try
            {
                using (var cts = CancellationTokenSource.CreateLinkedTokenSource(ServerStore.ServerShutdown))
                {
                    cts.CancelAfter(ServerStore.Configuration.Cluster.OperationTimeout.AsTimeSpan);

                    var waitingTasks = new List<Task<Exception>>();
                    List<Exception> exceptions = null;

                    foreach (var member in members)
                    {
                        var url = clusterTopology.GetUrlFromTag(member);
                        var executor = ClusterRequestExecutor.CreateForSingleNode(url, ServerStore.Server.Certificate.Certificate, DocumentConventions.DefaultForServer);
                        executors.Add(executor);
                        waitingTasks.Add(ExecuteTask(executor, member, cts.Token));
                    }

                    while (waitingTasks.Count > 0)
                    {
                        var task = await Task.WhenAny(waitingTasks);
                        waitingTasks.Remove(task);

                        if (task.Result == null)
                            continue;

                        var exception = task.Result.ExtractSingleInnerException();

                        if (exceptions == null)
                            exceptions = new List<Exception>();

                        exceptions.Add(exception);
                    }

                    if (exceptions != null)
                    {
                        var allTimeouts = true;
                        foreach (var exception in exceptions)
                        {
                            if (exception is OperationCanceledException)
                                continue;

                            allTimeouts = false;
                        }

                        var aggregateException = new AggregateException(exceptions);

                        if (allTimeouts)
                            throw new TimeoutException($"Waited too long for the raft command (number {index}) to be executed on any of the relevant nodes to this command.", aggregateException);

                        throw new InvalidDataException($"The database '{database}' was created but is not accessible, because all of the nodes on which this database was supposed to reside on, threw an exception.", aggregateException);
                    }
                }
            }
            finally
            {
                foreach (var executor in executors)
                {
                    executor.Dispose();
                }
            }

            async Task<Exception> ExecuteTask(RequestExecutor executor, string nodeTag, CancellationToken token)
            {
                try
                {
                    await executor.ExecuteAsync(new WaitForRaftIndexCommand(index), context, token: token);
                    return null;
                }
                catch (RavenException re) when (re.InnerException is HttpRequestException)
                {
                    // we want to throw for self-checks
                    if (nodeTag == ServerStore.NodeTag)
                        return re;

                    // ignore - we are ok when connection with a node cannot be established (test: AddDatabaseOnDisconnectedNode)
                    return null;
                }
                catch (Exception e)
                {
                    return e;
                }
            }
        }

        private Stream _responseStream;

        internal Stream ResponseBodyStream()
        {
            if (_responseStream != null)
                return _responseStream;

            _responseStream = new StreamWithTimeout(HttpContext.Response.Body);

            _context.HttpContext.Response.RegisterForDispose(_responseStream);

            if (TrafficWatchManager.HasRegisteredClients)
            {
                HttpContext.Items["ResponseStream"] = _responseStream;
            }

            return _responseStream;
        }

        internal string GetRaftRequestIdFromQuery()
        {
            var guid = GetStringQueryString("raft-request-id", required: false);

            if (guid == null)
            {
#if DEBUG
                var fromStudio = HttpContext.Request.IsFromStudio();
                if (fromStudio)
                    guid = RaftIdGenerator.NewId();
#else
                guid = RaftIdGenerator.NewId();
#endif
            }

            return guid;
        }

        protected internal string GetStringFromHeaders(string name)
        {
            var headers = HttpContext.Request.Headers[name];
            if (headers.Count == 0)
                return null;

            if (headers[0].Length < 2)
                return headers[0];

            string raw = headers[0][0] == '\"'
                ? headers[0].Substring(1, headers[0].Length - 2)
                : headers[0];

            return raw;
        }

        public virtual long? GetLongFromHeaders(string name)
        {
            var headers = HttpContext.Request.Headers[name];
            if (headers.Count == 0)
                return null;

            var raw = headers[0][0] == '\"'
                ? headers[0].AsSpan().Slice(1, headers[0].Length - 2)
                : headers[0].AsSpan();

            var success = long.TryParse(raw, out var result);

            if (success)
                return result;

            return null;
        }

        [DoesNotReturn]
        public void ThrowInvalidInteger(string name, string etag, string type = "int")
        {
            throw new ArgumentException($"Could not parse header '{name}' header as {type}, value was: {etag}");
        }

        protected internal int GetStart(int defaultStart = 0)
        {
            return GetIntValueQueryString(StartParameter, required: false) ?? defaultStart;
        }

        protected internal int GetPageSize()
        {
            var pageSize = GetIntValueQueryString(PageSizeParameter, required: false);
            if (pageSize.HasValue == false)
                return int.MaxValue;

            return pageSize.Value;
        }

        protected internal int? GetIntValueQueryString(string name, bool required = true)
        {
            var intAsString = GetStringQueryString(name, required);
            if (intAsString == null)
                return null;

            if (int.TryParse(intAsString, out int result) == false)
                ThrowInvalidInteger(name, intAsString);

            return result;
        }

        internal long GetLongQueryString(string name)
        {
            return GetLongQueryString(name, true).Value;
        }

        internal long? GetLongQueryString(string name, bool required)
        {
            var longAsString = GetStringQueryString(name, required);
            if (longAsString == null)
                return null;

            if (long.TryParse(longAsString, out long result) == false)
                ThrowInvalidInteger(name, longAsString, "long");

            return result;
        }

        internal string GetStringQueryString(string name, bool required = true)
        {
            var val = HttpContext.Request.Query[name];
            if (val.Count == 0 || string.IsNullOrWhiteSpace(val[0]))
            {
                if (required)
                    ThrowRequiredMember(name);

                return null;
            }

            return val[0];
        }

        internal char? GetCharQueryString(string name, bool required = true)
        {
            var val = HttpContext.Request.Query[name];
            if (val.Count == 0 || string.IsNullOrWhiteSpace(val[0]))
            {
                if (required)
                    ThrowRequiredMember(name);

                return null;
            }

            var value = val[0];
            if (value.Length > 1)
                ThrowSingleCharacterRequired(name, value);

            return value[0];
        }

        [DoesNotReturn]
        private static void ThrowSingleCharacterRequired(string name, string value)
        {
            throw new InvalidOperationException($"Query string {name} is expecting single character, but got '{value}'.");
        }

        [DoesNotReturn]
        private static void ThrowRequiredMember(string name)
        {
            throw new ArgumentException($"Query string {name} is mandatory, but wasn't specified.");
        }

        [DoesNotReturn]
        public static void ThrowRequiredPropertyNameInRequest(string name)
        {
            throw new ArgumentException($"Request should have a property name '{name}' which is mandatory.");
        }

        internal Microsoft.Extensions.Primitives.StringValues GetStringValuesQueryString(string name, bool required = true)
        {
            var val = HttpContext.Request.Query[name];
            if (val.Count == 0)
            {
                if (required)
                    ThrowRequiredMember(name);

                return default;
            }

            return val;
        }

        internal bool? GetBoolValueQueryString(string name, bool required = true)
        {
            var boolAsString = GetStringQueryString(name, required);
            if (boolAsString == null)
                return null;

            if (bool.TryParse(boolAsString, out bool result) == false)
                ThrowInvalidBoolean(name, boolAsString);

            return result;
        }

        [DoesNotReturn]
        private static void ThrowInvalidBoolean(string name, string val)
        {
            throw new ArgumentException($"Could not parse query string '{name}' as bool, val {val}");
        }

        internal DateTime? GetDateTimeQueryString(string name, bool required = true)
        {
            var dataAsString = GetStringQueryString(name, required);
            if (dataAsString == null)
                return null;

            dataAsString = Uri.UnescapeDataString(dataAsString);

            if (DateTime.TryParseExact(dataAsString, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out DateTime result))
                return result;

            ThrowInvalidDateTime(name, dataAsString);
            return null; //unreachable
        }

        [DoesNotReturn]
        public static void ThrowInvalidDateTime(string name, string dataAsString)
        {
            throw new ArgumentException($"Could not parse query string '{name}' as date, val '{dataAsString}'");
        }

        protected internal TimeSpan? GetTimeSpanQueryString(string name, bool required = true)
        {
            var timeSpanAsString = GetStringQueryString(name, required);
            if (timeSpanAsString == null)
                return null;

            timeSpanAsString = Uri.UnescapeDataString(timeSpanAsString);

            if (TimeSpan.TryParse(timeSpanAsString, out TimeSpan result))
                return result;

            ThrowInvalidTimeSpan(name, timeSpanAsString);
            return null;// unreachable
        }

        [DoesNotReturn]
        private static void ThrowInvalidTimeSpan(string name, string timeSpanAsString)
        {
            throw new ArgumentException($"Could not parse query string '{name}' as timespan val {timeSpanAsString}");
        }

        internal string GetQueryStringValueAndAssertIfSingleAndNotEmpty(string name)
        {
            var values = HttpContext.Request.Query[name];
            if (values.Count == 0 || string.IsNullOrWhiteSpace(values[0]))
                InvalidEmptyValue(name);
            if (values.Count > 1)
                InvalidCountOfValues(name);
            return values[0];
        }

        private static void InvalidEmptyValue(string name)
        {
            throw new ArgumentException($"Query string value '{name}' must have a non empty value");
        }

        private static void InvalidCountOfValues(string name)
        {
            throw new ArgumentException($"Query string value '{name}' must appear exactly once");
        }

        internal Task NoContent(HttpStatusCode statusCode = HttpStatusCode.NoContent)
        {
            NoContentStatus(statusCode);

            return Task.CompletedTask;
        }

        internal void NoContentStatus(HttpStatusCode statusCode = HttpStatusCode.NoContent)
        {
            HttpContext.Response.Headers.Remove(Constants.Headers.ContentType);
            HttpContext.Response.StatusCode = (int)statusCode;
        }

        protected bool IsClusterAdmin()
        {
            var feature = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;
            var status = feature?.Status;
            switch (status)
            {
                case null:
                case RavenServer.AuthenticationStatus.None:
                case RavenServer.AuthenticationStatus.NoCertificateProvided:
                case RavenServer.AuthenticationStatus.UnfamiliarIssuer:
                case RavenServer.AuthenticationStatus.UnfamiliarCertificate:
                case RavenServer.AuthenticationStatus.Expired:
                case RavenServer.AuthenticationStatus.Allowed:
                case RavenServer.AuthenticationStatus.NotYetValid:
                case RavenServer.AuthenticationStatus.Operator:
                    if (Server.Configuration.Security.AuthenticationEnabled == false)
                        return true;

                    return false;
                case RavenServer.AuthenticationStatus.ClusterAdmin:
                    return true;
                default:
                    ThrowInvalidAuthStatus(status);
                    return false;
            }
        }

        public async Task<bool> IsOperatorAsync()
        {
            var feature = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;
            var status = feature?.Status;
            switch (status)
            {
                case null:
                case RavenServer.AuthenticationStatus.None:
                case RavenServer.AuthenticationStatus.NoCertificateProvided:
                case RavenServer.AuthenticationStatus.UnfamiliarIssuer:
                case RavenServer.AuthenticationStatus.UnfamiliarCertificate:
                case RavenServer.AuthenticationStatus.Expired:
                case RavenServer.AuthenticationStatus.Allowed:
                case RavenServer.AuthenticationStatus.NotYetValid:
                    if (Server.Configuration.Security.AuthenticationEnabled == false)
                        return true;

                    await RequestRouter.UnlikelyFailAuthorizationAsync(HttpContext, null, feature,
                        AuthorizationStatus.Operator);
                    return false;
                case RavenServer.AuthenticationStatus.Operator:
                case RavenServer.AuthenticationStatus.ClusterAdmin:
                    return true;
                default:
                    ThrowInvalidAuthStatus(status);
                    return false;
            }
        }

        public sealed class AllowedDbs
        {
            public bool HasAccess { get; set; }

            public Dictionary<string, DatabaseAccess> AuthorizedDatabases { get; set; }
        }

        internal async Task<bool> CanAccessDatabaseAsync(string dbName, bool requireAdmin, bool requireWrite)
        {
            var result = await GetAllowedDbsAsync(dbName, requireAdmin, requireWrite);

            return result.HasAccess;
        }

        protected internal async Task<AllowedDbs> GetAllowedDbsAsync(string dbName, bool requireAdmin, bool requireWrite)
        {
            var feature = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;

            var status = feature?.Status;
            switch (status)
            {
                case null:
                case RavenServer.AuthenticationStatus.None:
                case RavenServer.AuthenticationStatus.NoCertificateProvided:
                case RavenServer.AuthenticationStatus.UnfamiliarIssuer:
                case RavenServer.AuthenticationStatus.UnfamiliarCertificate:
                case RavenServer.AuthenticationStatus.Expired:
                case RavenServer.AuthenticationStatus.NotYetValid:
                    if (Server.Configuration.Security.AuthenticationEnabled == false)
                        return new AllowedDbs { HasAccess = true };

                    await RequestRouter.UnlikelyFailAuthorizationAsync(HttpContext, dbName, null, requireAdmin ? AuthorizationStatus.DatabaseAdmin : AuthorizationStatus.ValidUser);
                    return new AllowedDbs { HasAccess = false };
                case RavenServer.AuthenticationStatus.ClusterAdmin:
                case RavenServer.AuthenticationStatus.Operator:
                    return new AllowedDbs { HasAccess = true };
                case RavenServer.AuthenticationStatus.Allowed:
                    if (dbName != null && feature.CanAccess(dbName, requireAdmin, requireWrite) == false)
                    {
                        await RequestRouter.UnlikelyFailAuthorizationAsync(HttpContext, dbName, feature, requireAdmin ? AuthorizationStatus.DatabaseAdmin : AuthorizationStatus.ValidUser);
                        return new AllowedDbs { HasAccess = false };
                    }

                    return new AllowedDbs { HasAccess = true, AuthorizedDatabases = feature.AuthorizedDatabases };
                default:
                    ThrowInvalidAuthStatus(status);
                    return new AllowedDbs { HasAccess = false };
            }
        }

        [DoesNotReturn]
        private static void ThrowInvalidAuthStatus(RavenServer.AuthenticationStatus? status)
        {
            throw new ArgumentOutOfRangeException("Unknown authentication status: " + status);
        }

        public static void SetupCORSHeaders(HttpContext httpContext, ServerStore serverStore, CorsMode corsMode)
        {
            httpContext.Response.Headers.Add("Vary", "Origin");

            var requestedOrigin = httpContext.Request.Headers["Origin"];

            if (requestedOrigin.Count == 0)
            {
                // no CORS headers needed
                return;
            }

            string allowedOrigin = null; // prevent access by default

            switch (corsMode)
            {
                case CorsMode.Public:
                    allowedOrigin = requestedOrigin;
                    break;
                case CorsMode.Cluster:
                    if (IsOriginAllowed(requestedOrigin, serverStore))
                        allowedOrigin = requestedOrigin;
                    break;
            }

            httpContext.Response.Headers.Add("Access-Control-Allow-Origin", allowedOrigin);
            httpContext.Response.Headers.Add("Access-Control-Allow-Methods", "PUT, POST, GET, OPTIONS, DELETE");
            httpContext.Response.Headers.Add("Access-Control-Allow-Headers", httpContext.Request.Headers["Access-Control-Request-Headers"]);
            httpContext.Response.Headers.Add("Access-Control-Max-Age", "86400");
        }

        private static bool IsOriginAllowed(string origin, ServerStore serverStore)
        {
            if (serverStore.Server.Certificate.Certificate == null)
            {
                // running in unsafe mode - since server can be access via multiple urls/aliases accept them 
                return true;
            }

            var topology = serverStore.GetClusterTopology();

            // check explicitly each topology type to avoid allocations in topology.AllNodes
            foreach (var kvp in topology.Members)
            {
                if (kvp.Value.Equals(origin, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }

            foreach (var kvp in topology.Watchers)
            {
                if (kvp.Value.Equals(origin, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }

            foreach (var kvp in topology.Promotables)
            {
                if (kvp.Value.Equals(origin, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }

            return false;
        }
        protected void RedirectToLeader()
        {
            if (ServerStore.LeaderTag == null)
                throw new NoLeaderException();

            if (ServerStore.Engine.CurrentState == RachisState.LeaderElect)
                throw new NoLeaderException("This node is elected to be the leader, but didn't took office yet.");

            ClusterTopology topology;
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                topology = ServerStore.GetClusterTopology(context);
            }
            var url = topology.GetUrlFromTag(ServerStore.LeaderTag);
            if (string.Equals(url, ServerStore.GetNodeHttpServerUrl(), StringComparison.OrdinalIgnoreCase))
            {
                throw new NoLeaderException($"This node is not the leader, but the current topology does mark it as the leader. Such confusion is usually an indication of a network or configuration problem.");
            }
            var leaderLocation = url + HttpContext.Request.Path + HttpContext.Request.QueryString;
            HttpContext.Response.StatusCode = (int)HttpStatusCode.TemporaryRedirect;
            HttpContext.Response.Headers.Remove(Constants.Headers.ContentType);
            HttpContext.Response.Headers.Add("Location", leaderLocation);
        }

        public virtual bool IsShutdownRequested() => ServerStore.ServerShutdown.IsCancellationRequested;

        [DoesNotReturn]
        public virtual void ThrowShutdownException(Exception inner = null) => throw new OperationCanceledException($"Server on node {ServerStore.NodeTag} is shutting down", inner);

        public virtual OperationCancelToken CreateHttpRequestBoundOperationToken()
        {
            return new OperationCancelToken(ServerStore.ServerShutdown, HttpContext.RequestAborted);
        }

        public virtual OperationCancelToken CreateHttpRequestBoundOperationToken(CancellationToken token)
        {
            return new OperationCancelToken(ServerStore.ServerShutdown, HttpContext.RequestAborted, token);
        }

        public virtual OperationCancelToken CreateHttpRequestBoundTimeLimitedOperationToken(TimeSpan cancelAfter)
        {
            return new OperationCancelToken(cancelAfter, ServerStore.ServerShutdown, HttpContext.RequestAborted);
        }

        public virtual OperationCancelToken CreateBackgroundOperationToken()
        {
            return new OperationCancelToken(ServerStore.ServerShutdown);
        }

        public virtual Task WaitForIndexToBeAppliedAsync(TransactionOperationContext context, long index)
        {
            return Task.CompletedTask;
        }

        public const string BackupDatabaseOnceTag = "one-time-database-backup";
        public const string DefineHubDebugTag = "update-hub-pull-replication";
        public const string UpdatePullReplicationOnSinkNodeDebugTag = "update-sink-pull-replication";
        public const string UpdatePeriodicBackupDebugTag = "update-periodic-backup";
        public const string PutConnectionStringDebugTag = "put-connection-string";
        public const string AddEtlDebugTag = "etl-add";
        public const string AddQueueSinkDebugTag = "queue-sink-add";
        public const string UpdateExternalReplicationDebugTag = "update_external_replication";

        private DynamicJsonValue GetCustomConfigurationAuditJson(string name, BlittableJsonReaderObject configuration)
        {
            switch (name)
            {
                case BackupDatabaseOnceTag:
                    return JsonDeserializationServer.BackupConfiguration(configuration).ToAuditJson();

                case UpdatePeriodicBackupDebugTag:
                    return JsonDeserializationClient.PeriodicBackupConfiguration(configuration).ToAuditJson();

                case UpdateExternalReplicationDebugTag:
                    return JsonDeserializationClient.ExternalReplication(configuration).ToAuditJson();

                case DefineHubDebugTag:
                    return JsonDeserializationClient.PullReplicationDefinition(configuration).ToAuditJson();

                case UpdatePullReplicationOnSinkNodeDebugTag:
                    return JsonDeserializationClient.PullReplicationAsSink(configuration).ToAuditJson();

                case AddEtlDebugTag:
                    return GetEtlConfigurationAuditJson(configuration);

                case PutConnectionStringDebugTag:
                    return GetConnectionStringConfigurationAuditJson(configuration);
            }
            return null;
        }

        private DynamicJsonValue GetEtlConfigurationAuditJson(BlittableJsonReaderObject configuration)
        {
            var etlType = EtlConfiguration<ConnectionString>.GetEtlType(configuration);

            switch (etlType)
            {
                case EtlType.Raven:
                    return JsonDeserializationClient.RavenEtlConfiguration(configuration).ToAuditJson();

                case EtlType.ElasticSearch:
                    return JsonDeserializationClient.ElasticSearchEtlConfiguration(configuration).ToAuditJson();

                case EtlType.Queue:
                    return JsonDeserializationClient.QueueEtlConfiguration(configuration).ToAuditJson();

                case EtlType.Sql:
                    return JsonDeserializationClient.SqlEtlConfiguration(configuration).ToAuditJson();

                case EtlType.Olap:
                    return JsonDeserializationClient.OlapEtlConfiguration(configuration).ToAuditJson();
            }

            return null;
        }

        private DynamicJsonValue GetConnectionStringConfigurationAuditJson(BlittableJsonReaderObject configuration)
        {
            var connectionStringType = ConnectionString.GetConnectionStringType(configuration);
            switch (connectionStringType)
            {
                case ConnectionStringType.Raven:
                    return JsonDeserializationClient.RavenConnectionString(configuration).ToAuditJson();

                case ConnectionStringType.ElasticSearch:
                    return JsonDeserializationClient.ElasticSearchConnectionString(configuration).ToAuditJson();

                case ConnectionStringType.Queue:
                    return JsonDeserializationClient.QueueConnectionString(configuration).ToAuditJson();

                case ConnectionStringType.Sql:
                    return JsonDeserializationClient.SqlConnectionString(configuration).ToAuditJson();

                case ConnectionStringType.Olap:
                    return JsonDeserializationClient.OlapConnectionString(configuration).ToAuditJson();
            }

            return null;
        }

        public void LogTaskToAudit(string description, long id, BlittableJsonReaderObject configuration)
        {
            if (LoggingSource.AuditLog.IsInfoEnabled)
            {
                DynamicJsonValue conf = GetCustomConfigurationAuditJson(description, configuration);
                var clientCert = GetCurrentCertificate();
                var auditLog = LoggingSource.AuditLog.GetLogger(_context.DatabaseName ?? "Server", "Audit");
                var line = $"Task: '{description}' with taskId: '{id}'";

                if (clientCert != null)
                    line += $" executed by '{clientCert.Subject}' '{clientCert.Thumbprint}'";

                if (conf != null)
                {
                    var confString = string.Empty;
                    using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
                    {
                        confString = ctx.ReadObject(conf, "conf").ToString();
                    }

                    line += ($" Configuration: {confString}");
                }

                auditLog.Info(line);
            }
        }
    }
}
