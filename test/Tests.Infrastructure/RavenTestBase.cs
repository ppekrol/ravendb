﻿using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Cluster;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Json.Sync;
using Sparrow.Platform;
using Sparrow.Utils;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace FastTests
{
    public abstract partial class RavenTestBase : TestBase
    {
        public static BackupTestBase Backup => BackupTestBase.Instance.Value;

        public static TimeSeriesTestBase TimeSeries => TimeSeriesTestBase.Instance.Value;

        protected readonly ConcurrentSet<DocumentStore> CreatedStores = new ConcurrentSet<DocumentStore>();

        protected RavenTestBase(ITestOutputHelper output) : base(output)
        {
            Sharding = new ShardingTestBase(this);
        }

        protected virtual Task<DocumentDatabase> GetDocumentDatabaseInstanceFor(IDocumentStore store, string database = null)
        {
            return Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database ?? store.Database);
        }

        protected bool WaitForDocument<T>(IDocumentStore store,
            string docId,
            Func<T, bool> predicate,
            int timeout = 10000,
            string database = null)
        {
            if (DebuggerAttachedTimeout.DisableLongTimespan == false &&
                Debugger.IsAttached)
                timeout *= 1000;

            var sw = Stopwatch.StartNew();
            Exception ex = null;
            while (sw.ElapsedMilliseconds < timeout)
            {
                using (var session = store.OpenSession(database ?? store.Database))
                {
                    try
                    {
                        var doc = session.Load<T>(docId);
                        if (doc != null)
                        {
                            if (predicate == null || predicate(doc))
                                return true;
                        }
                    }
                    catch (Exception e)
                    {
                        ex = e;
                        // expected that we might get conflict, ignore and wait
                    }
                }

                Thread.Sleep(100);
            }

            using (var session = store.OpenSession(database ?? store.Database))
            {
                //one last try, and throw if there is still a conflict
                var doc = session.Load<T>(docId);
                if (doc != null)
                {
                    if (predicate == null || predicate(doc))
                        return true;
                }
            }
            if (ex != null)
            {
                throw ex;
            }
            return false;
        }

        protected static void CreateNorthwindDatabase(DocumentStore store, DatabaseItemType operateOnTypes = DatabaseItemType.Documents)
        {
            store.Maintenance.Send(new CreateSampleDataOperation(operateOnTypes));
        }

        protected static async Task CreateLegacyNorthwindDatabase(DocumentStore store)
        {
            using (var stream = Assembly.GetExecutingAssembly().GetManifestResourceStream("Tests.Infrastructure.Data.Northwind.4.2.ravendbdump"))
            {
                Assert.NotNull(stream);

                var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), stream);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
            }
        }

        protected async Task SetDatabaseId(DocumentStore store, Guid dbId)
        {
            var database = await GetDocumentDatabaseInstanceFor(store);
            var type = database.GetAllStoragesEnvironment().Single(t => t.Type == StorageEnvironmentWithType.StorageEnvironmentType.Documents);
            type.Environment.FillBase64Id(dbId);
        }

        private readonly object _getDocumentStoreSync = new object();

        protected string EncryptedServer(out TestCertificatesHolder certificates, out string databaseName)
        {
            certificates = SetupServerAuthentication();
            databaseName = GetDatabaseName();
            RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            var buffer = new byte[32];
            using (var rand = RandomNumberGenerator.Create())
            {
                rand.GetBytes(buffer);
            }

            var base64Key = Convert.ToBase64String(buffer);

            var canUseProtect = PlatformDetails.RunningOnPosix == false;

            if (canUseProtect)
            {
                // sometimes when using `dotnet xunit` we get platform not supported from ProtectedData
                try
                {
#pragma warning disable CA1416 // Validate platform compatibility
                    ProtectedData.Protect(Encoding.UTF8.GetBytes("Is supported?"), null, DataProtectionScope.CurrentUser);
#pragma warning restore CA1416 // Validate platform compatibility
                }
                catch (PlatformNotSupportedException)
                {
                    canUseProtect = false;
                }
            }

            if (canUseProtect == false) // fall back to a file
                Server.ServerStore.Configuration.Security.MasterKeyPath = GetTempFileName();

            Assert.True(Server.ServerStore.EnsureNotPassiveAsync().Wait(TimeSpan.FromSeconds(30))); // activate license so we can insert the secret key
            Assert.True(Server.ServerStore.LicenseManager.TryActivateLicenseAsync(Server.ThrowOnLicenseActivationFailure).Wait(TimeSpan.FromSeconds(30))); // activate license so we can insert the secret key
            Server.ServerStore.PutSecretKey(base64Key, databaseName, overwrite: true);

            return Convert.ToBase64String(buffer);
        }

        protected void EncryptedCluster(List<RavenServer> nodes, TestCertificatesHolder certificates, out string databaseName)
        {
            databaseName = GetDatabaseName();

            foreach (var node in nodes)
            {
                RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, node);

                var base64Key = CreateMasterKey(out _);

                EnsureServerMasterKeyIsSetup(node);

                Assert.True(node.ServerStore.EnsureNotPassiveAsync().Wait(TimeSpan.FromSeconds(30))); // activate license so we can insert the secret key
                Assert.True(node.ServerStore.LicenseManager.TryActivateLicenseAsync(Server.ThrowOnLicenseActivationFailure).Wait(TimeSpan.FromSeconds(30))); // activate license so we can insert the secret key

                node.ServerStore.PutSecretKey(base64Key, databaseName, overwrite: true);
            }
        }

        protected async Task WaitForRaftCommandToBeAppliedInCluster(RavenServer leader, string commandType)
        {
            var updateIndex = LastRaftIndexForCommand(leader, commandType);
            await WaitForRaftIndexToBeAppliedInCluster(updateIndex, TimeSpan.FromSeconds(10));
        }

        protected async Task WaitForRaftCommandToBeAppliedInLocalServer(string commandType)
        {
            var updateIndex = LastRaftIndexForCommand(Server, commandType);
            await Server.ServerStore.Cluster.WaitForIndexNotification(updateIndex, TimeSpan.FromSeconds(10));
        }

        protected static long LastRaftIndexForCommand(RavenServer server, string commandType)
        {
            var updateIndex = 0L;
            var commandFound = false;
            using (server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var entry in server.ServerStore.Engine.LogHistory.GetHistoryLogs(context))
                {
                    var type = entry[nameof(RachisLogHistory.LogHistoryColumn.Type)].ToString();
                    if (type == commandType)
                    {
                        commandFound = true;
                        Assert.True(long.TryParse(entry[nameof(RachisLogHistory.LogHistoryColumn.Index)].ToString(), out updateIndex));
                    }
                }
            }

            Assert.True(commandFound, $"{commandType} wasn't found in the log.");
            return updateIndex;
        }

        protected static IEnumerable<DynamicJsonValue> GetRaftCommands(RavenServer server, string commandType = null)
        {
            using (server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var entry in server.ServerStore.Engine.LogHistory.GetHistoryLogs(context))
                {
                    var type = entry[nameof(RachisLogHistory.LogHistoryColumn.Type)].ToString();
                    if (commandType == null || commandType == type)
                        yield return entry;
                }
            }
        }

        protected string GetRaftHistory(RavenServer server)
        {
            var sb = new StringBuilder();

            using (server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var entry in server.ServerStore.Engine.LogHistory.GetHistoryLogs(context))
                {
                    sb.AppendLine(context.ReadObject(entry, "raft-command-history").ToString());
                }
            }

            return sb.ToString();
        }

        protected async Task WaitForRaftIndexToBeAppliedInClusterWithNodesValidation(long index, TimeSpan? timeout = null)
        {
            var notDisposed = Servers.Count(s => s.ServerStore.Disposed == false);
            var notPassive = Servers.Count(s => s.ServerStore.Engine.CurrentState != RachisState.Passive);

            Assert.True(Servers.Count == notDisposed, $"Unequal not disposed nodes {Servers.Count} != {notDisposed}");
            Assert.True(Servers.Count == notPassive, $"Unequal not passive nodes {Servers.Count} != {notPassive}");

            await WaitForRaftIndexToBeAppliedInCluster(index, timeout);
        }

        protected async Task WaitForRaftIndexToBeAppliedInCluster(long index, TimeSpan? timeout = null)
        {
            await WaitForRaftIndexToBeAppliedOnClusterNodes(index, Servers, timeout);
        }

        protected static async Task WaitForRaftIndexToBeAppliedOnClusterNodes(long index, List<RavenServer> nodes, TimeSpan? timeout = null)
        {
            if (nodes.Count == 0)
                throw new InvalidOperationException("Cannot wait for raft index to be applied when the cluster is empty. Make sure you are using the right server.");

            if (timeout.HasValue == false)
                timeout = Debugger.IsAttached ? TimeSpan.FromSeconds(300) : TimeSpan.FromSeconds(60);

            var tasks = nodes.Where(s => s.ServerStore.Disposed == false &&
                                          s.ServerStore.Engine.CurrentState != RachisState.Passive)
                .Select(server => server.ServerStore.Cluster.WaitForIndexNotification(index))
                .ToList();

            if (await Task.WhenAll(tasks).WaitWithoutExceptionAsync(timeout.Value))
                return;

            ThrowTimeoutException(nodes, tasks, index, timeout.Value);
        }

        private static void ThrowTimeoutException(List<RavenServer> nodes, List<Task> tasks, long index, TimeSpan timeout)
        {
            var message = $"Timed out after {timeout} waiting for index {index} because out of {nodes.Count} servers" +
                          " we got confirmations that it was applied only on the following servers: ";

            for (var i = 0; i < tasks.Count; i++)
            {
                message += $"{Environment.NewLine}Url: {nodes[i].WebUrl}. Applied: {tasks[i].IsCompleted}.";
                if (tasks[i].IsCompleted == false)
                {
                    using (nodes[i].ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                    {
                        context.OpenReadTransaction();
                        message += $"{Environment.NewLine}Log state for non responsing server:{Environment.NewLine}{nodes[i].ServerStore.Engine.LogHistory.GetHistoryLogsAsString(context)}";
                    }
                }
            }

            throw new TimeoutException(message);
        }

        public static string CollectLogsFromNodes(List<RavenServer> nodes)
        {
            var message = "";
            for (var i = 0; i < nodes.Count; i++)
            {
                message += $"{Environment.NewLine}Url: {nodes[i].WebUrl}.";
                using (nodes[i].ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                using (context.OpenReadTransaction())
                {
                    message += CollectLogs(context, nodes[i]);
                }
            }

            return message;
        }

        protected static string CollectLogs(ClusterOperationContext context, RavenServer server)
        {
            return
                $"{Environment.NewLine}Log for server '{server.ServerStore.NodeTag}':" +
                $"{Environment.NewLine}Last notified Index '{server.ServerStore.Cluster.LastNotifiedIndex}':" +
                $"{Environment.NewLine}{context.ReadObject(server.ServerStore.GetLogDetails(context, max: int.MaxValue), "LogSummary/" + server.ServerStore.NodeTag)}" +
                $"{Environment.NewLine}{server.ServerStore.Engine.LogHistory.GetHistoryLogsAsString(context)}";
        }

        protected virtual DocumentStore GetDocumentStore(Options options = null, [CallerMemberName] string caller = null)
        {
            try
            {
                lock (_getDocumentStoreSync)
                {
                    options ??= Options.Default;
                    var serverToUse = options.Server ?? Server;

                    var name = GetDatabaseName(caller);

                    if (options.ModifyDatabaseName != null)
                        name = options.ModifyDatabaseName(name) ?? name;

                    var hardDelete = true;
                    var runInMemory = options.RunInMemory;

                    var pathToUse = options.Path;
                    if (runInMemory == false && options.ReplicationFactor > 1)
                    {
                        if (pathToUse == null)
                        {
                            // the folders will be assigned automatically
                        }
                        else
                        {
                            throw new InvalidOperationException($"You cannot set {nameof(Options)}.{nameof(Options.Path)} when, {nameof(Options)}.{nameof(Options.ReplicationFactor)} > 1 and {nameof(Options)}.{nameof(Options.RunInMemory)} == false.");
                        }
                    }
                    else if (pathToUse == null)
                    {
                        if (options.ReplicationFactor > 1)
                        {
                            // the folders will be assigned automatically - when running in cluster it's better to put files in directories under dedicated server / node dir
                        }
                        else
                        {
                            pathToUse = NewDataPath(name);
                        }
                    }
                    else
                    {
                        hardDelete = false;
                        runInMemory = false;
                    }

                    var doc = new DatabaseRecord(name)
                    {
                        Settings =
                        {
                            [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = runInMemory.ToString(),
                            [RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexCannotBeOpened)] = "true",
                            [RavenConfiguration.GetKey(x => x.Indexing.MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory)] = int.MaxValue.ToString(),
                        }
                    };

                    if (options.Encrypted)
                        doc.Encrypted = true;

                    if (pathToUse != null)
                    {
                        doc.Settings.Add(RavenConfiguration.GetKey(x => x.Core.DataDirectory), pathToUse);
                    }

                    options.ModifyDatabaseRecord?.Invoke(doc);

                    var store = new DocumentStore
                    {
                        Urls = UseFiddler(serverToUse.WebUrl),
                        Database = name,
                        Certificate = options.ClientCertificate,
                        Conventions =
                        {
                            DisableTopologyCache = true
                        }
                    };

                    options.ModifyDocumentStore?.Invoke(store);

                    //This gives too much error details in most cases, we don't need this now
                    store.RequestExecutorCreated += (sender, executor) =>
                    {
                        executor.AdditionalErrorInformation += sb => sb.AppendLine().Append(GetLastStatesFromAllServersOrderedByTime());
                    };

                    store.Initialize();

                    if (options.CreateDatabase)
                    {
                        if (Servers.Contains(serverToUse))
                        {
                            Servers.ForEach(server => CheckIfDatabaseExists(server, name));
                        }
                        else
                        {
                            CheckIfDatabaseExists(serverToUse, name);
                        }

                        long raftCommand;
                        try
                        {
                            if (options.AdminCertificate != null)
                            {
                                using (var adminStore = new DocumentStore
                                {
                                    Urls = UseFiddler(serverToUse.WebUrl),
                                    Database = name,
                                    Certificate = options.AdminCertificate
                                }.Initialize())
                                {
                                    raftCommand = adminStore.Maintenance.Server.Send(new CreateDatabaseOperation(doc, options.ReplicationFactor)).RaftCommandIndex;
                                }
                            }
                            else
                            {
                                raftCommand = store.Maintenance.Server.Send(new CreateDatabaseOperation(doc, options.ReplicationFactor)).RaftCommandIndex;
                            }
                        }
                        catch (ConcurrencyException)
                        {
                            var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(name));
                            Assert.Equal(options.ReplicationFactor, record.IsSharded ? record.Shards[0].Count : record.Topology.ReplicationFactor);
                            raftCommand = record.Etag;
                        }

                        Assert.True(raftCommand > 0); //sanity check

                        if (Servers.Contains(serverToUse))
                        {
                            var timeout = TimeSpan.FromMinutes(Debugger.IsAttached ? 5 : 1);
                            AsyncHelpers.RunSync(async () => await WaitForRaftIndexToBeAppliedInClusterWithNodesValidation(raftCommand, timeout));

                            // skip 'wait for requests' on DocumentDatabase dispose
                            Servers.ForEach(server => ApplySkipDrainAllRequestsToDatabase(server, name));
                        }
                        else
                        {
                            ApplySkipDrainAllRequestsToDatabase(serverToUse, name);
                        }
                    }

                    store.BeforeDispose += (sender, args) =>
                    {
                        var realException = Context.GetException();
                        try
                        {
                            if (CreatedStores.TryRemove(store) == false)
                                return; // can happen if we are wrapping the store inside sharded one

                            DeleteDatabaseResult result = null;
                            if (options.DeleteDatabaseOnDispose)
                            {
                                result = DeleteDatabase(options, serverToUse, name, hardDelete, store);
                            }

                            if (Servers.Contains(serverToUse) && result != null)
                            {
                                var timeout = options.DeleteTimeout ?? TimeSpan.FromSeconds(Debugger.IsAttached ? 150 : 15);
                                AsyncHelpers.RunSync(async () => await WaitForRaftIndexToBeAppliedInCluster(result.RaftCommandIndex, timeout));
                            }
                        }
                        catch (Exception e)
                        {
                            if (realException != null)
                                throw new AggregateException(realException, e);

                            throw;
                        }
                    };
                    CreatedStores.Add(store);

                    return store;
                }
            }
            catch (TimeoutException te)
            {
                throw new TimeoutException($"{te.Message} {Environment.NewLine} {te.StackTrace}{Environment.NewLine}Servers states:{Environment.NewLine}{GetLastStatesFromAllServersOrderedByTime()}");
            }
        }

        private static void CheckIfDatabaseExists(RavenServer server, string name)
        {
            using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                context.OpenReadTransaction();
                if (server.ServerStore.Cluster.Read(context, Constants.Documents.Prefix + name) != null)
                    throw new InvalidOperationException($"Database '{name}' already exists");
            }
        }

        private void ApplySkipDrainAllRequestsToDatabase(RavenServer serverToUse, string name)
        {
            try
            {
                var documentDatabase = AsyncHelpers.RunSync(async () => await serverToUse.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(name));
                Assert.True(documentDatabase != null, $"(RavenDB-16924) documentDatabase is null on '{serverToUse.ServerStore.NodeTag}' {Environment.NewLine}{CollectLogsFromNodes(Servers)}");
                documentDatabase.ForTestingPurposesOnly().SkipDrainAllRequests = true;
            }
            catch (InvalidOperationException)
            {
                //TODO expected if sharded - need to fix that
            }
            catch (DatabaseNotRelevantException)
            {
            }
        }

        private DeleteDatabaseResult DeleteDatabase(Options options, RavenServer serverToUse, string name, bool hardDelete, DocumentStore store)
        {
            try
            {
                if (options.AdminCertificate != null)
                {
                    using (var adminStore =
                        new DocumentStore { Urls = UseFiddler(serverToUse.WebUrl), Database = name, Certificate = options.AdminCertificate }.Initialize())
                    {
                        return adminStore.Maintenance.Server.Send(new DeleteDatabasesOperation(name, hardDelete));
                    }
                }

                return store.Maintenance.Server.Send(new DeleteDatabasesOperation(name, hardDelete));
            }
            catch (OperationCanceledException)
            {
                //failed to delete in time
            }
            catch (TimeoutException)
            {
                //failed to delete in time
            }
            catch (DatabaseDoesNotExistException)
            {
            }
            catch (NoLeaderException)
            {
            }
            catch (AllTopologyNodesDownException)
            {

            }
            catch (Exception e)
            {
                if (e is RavenException && (e.InnerException is TimeoutException || e.InnerException is OperationCanceledException))
                    return null;

                if (Servers.Contains(serverToUse))
                {
                    if (Servers.All(s => s.Disposed))
                        return null;
                }

                if (serverToUse.Disposed)
                    return null;

                throw;
            }
            return null;
        }

        protected string GetLastStatesFromAllServersOrderedByTime()
        {
            List<(string tag, RachisConsensus.StateTransition transition)> states = new List<(string tag, RachisConsensus.StateTransition transition)>();
            foreach (var s in Servers)
            {
                foreach (var state in s.ServerStore.Engine.PrevStates)
                {
                    states.Add((s.ServerStore.NodeTag, state));
                }
            }
            return string.Join(Environment.NewLine, states.OrderBy(x => x.transition.When).Select(x => $"State for {x.tag}-term{x.Item2.CurrentTerm}:{Environment.NewLine}{x.Item2.From}=>{x.Item2.To} at {x.Item2.When:o} {Environment.NewLine}because {x.Item2.Reason}"));
        }

        public static void WaitForIndexing(IDocumentStore store, string databaseName = null, TimeSpan? timeout = null, bool allowErrors = false, string nodeTag = null)
        {
            databaseName ??= store.Database;
            var admin = store.Maintenance.ForDatabase(databaseName);
            var databaseRecord = admin.Server.Send(new GetDatabaseRecordOperation(databaseName));

            timeout ??= (Debugger.IsAttached
                ? TimeSpan.FromMinutes(15)
                : TimeSpan.FromMinutes(1));

            var sp = Stopwatch.StartNew();
            var nonStaleShards = new HashSet<int>();

            while (sp.Elapsed < timeout.Value)
            {
                var staleStatus = RavenTestBase.StaleStatus.NonStale;

                if (databaseRecord.IsSharded)
                {
                    for (var i = 0; i < databaseRecord.Shards.Length; i++)
                    {
                        if (nonStaleShards.Contains(i))
                            continue;

                        var shardStatus = StaleStatus(shardId: i);
                        if (shardStatus == RavenTestBase.StaleStatus.NonStale)
                            nonStaleShards.Add(i);

                        staleStatus |= shardStatus;
                    }
                }
                else
                {
                    staleStatus = StaleStatus();
                }

                if (staleStatus.HasFlag(RavenTestBase.StaleStatus.Error))
                    break;

                if (staleStatus == RavenTestBase.StaleStatus.NonStale)
                    return;
            }

            if (allowErrors)
            {
                return;
            }

            var files = new List<string>();
            if (databaseRecord.IsSharded)
            {
                for (var i = 0; i < databaseRecord.Shards.Length; i++)
                {
                    files.Add(OutputIndexInfo(i));
                }
            }
            else
            {
                files.Add(OutputIndexInfo(null));
            }

            string OutputIndexInfo(int? shard)
            {
                IndexPerformanceStats[] perf;
                IndexErrors[] errors;
                IndexStats[] stats;
                if (shard.HasValue == false)
                {
                    perf = admin.Send(new GetIndexPerformanceStatisticsOperation());
                    errors = admin.Send(new GetIndexErrorsOperation());
                    stats = admin.Send(new GetIndexesStatisticsOperation());
                }
                else
                {
                    perf = admin.ForShard(shard.Value).Send(new GetIndexPerformanceStatisticsOperation());
                    errors = admin.ForShard(shard.Value).Send(new GetIndexErrorsOperation());
                    stats = admin.ForShard(shard.Value).Send(new GetIndexesStatisticsOperation());
                }

                var total = new
                {
                    Errors = errors,
                    Stats = stats,
                    Performance = perf,
                    NodeTag = nodeTag
                };

                var file = $"{Path.GetTempFileName()}{(shard != null ? $"_shard{shard}" : "")}.json";

                using (var stream = File.Open(file, FileMode.OpenOrCreate))
                using (var context = JsonOperationContext.ShortTermSingleUse())
                using (var writer = new BlittableJsonTextWriter(context, stream))
                {
                    var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(total);
                    var json = context.ReadObject(djv, "errors");
                    writer.WriteObject(json);
                    writer.Flush();
                }

                return file;
            }

            List<IndexInformation> allIndexes = new();

            if (databaseRecord.IsSharded)
            {
                for (var i = 0; i < databaseRecord.Shards.Length; i++)
                {
                    var statistics = admin.ForShard(i).Send(new GetStatisticsOperation("wait-for-indexing", nodeTag));
                    allIndexes.AddRange(statistics.Indexes);
                }
            }
            else
            {
                allIndexes.AddRange(admin.Send(new GetStatisticsOperation("wait-for-indexing", nodeTag)).Indexes);
            }

            var corrupted = allIndexes.Where(x => x.State == IndexState.Error).ToList();
            if (corrupted.Count > 0)
            {
                throw new InvalidOperationException(
                    $"The following indexes are with error state: {string.Join(",", corrupted.Select(x => x.Name))} - details at " + string.Join(", ", files));
            }

            throw new TimeoutException("The indexes stayed stale for more than " + timeout.Value + ", stats at " + string.Join(", ", files));

            StaleStatus StaleStatus(int? shardId = null)
            {
                var executor = shardId.HasValue ? admin.ForShard(shardId.Value) : admin;
                var databaseStatistics = executor.Send(new GetStatisticsOperation("wait-for-indexing", nodeTag));
                var indexes = databaseStatistics.Indexes
                    .Where(x => x.State != IndexState.Disabled);

                var staleIndexesCount = indexes.Count(x => x.IsStale || x.Name.StartsWith("ReplacementOf/"));
                if (staleIndexesCount == 0)
                    return RavenTestBase.StaleStatus.NonStale;

                var erroredIndexesCount = databaseStatistics.Indexes.Count(x => x.State == IndexState.Error);
                if (allowErrors)
                {
                    // wait for all indexes to become non stale
                }
                else if (erroredIndexesCount > 0)
                {
                    // have at least some errors
                    return RavenTestBase.StaleStatus.Error;
                }

                Thread.Sleep(32);
                return RavenTestBase.StaleStatus.Stale;
            }
        }

        [Flags]
        public enum StaleStatus
        {
            NonStale = 0x1,
            Stale = 0x2,
            Error = 0x4
        }
        public static IndexErrors[] WaitForIndexingErrors(IDocumentStore store, string[] indexNames = null, TimeSpan? timeout = null, string nodeTag = null, bool? errorsShouldExists = null)
        {
            if (errorsShouldExists is null)
            {
                timeout ??= Debugger.IsAttached ? TimeSpan.FromMinutes(15) : TimeSpan.FromMinutes(1);
            }
            else
            {
                timeout ??= errorsShouldExists is true
                    ? TimeSpan.FromSeconds(5)
                    : TimeSpan.FromSeconds(1);
            }

            var toWait = new HashSet<string>(indexNames ?? Array.Empty<string>(), StringComparer.OrdinalIgnoreCase);

            var sp = Stopwatch.StartNew();
            while (sp.Elapsed < timeout.Value)
            {
                try
                {
                    var indexes = store.Maintenance.Send(new GetIndexErrorsOperation(indexNames, nodeTag));
                    foreach (var index in indexes)
                    {
                        if (index.Errors.Length > 0)
                        {
                            toWait.Remove(index.Name);

                            if (toWait.Count == 0)
                                return indexes;
                        }
                    }
                }
                catch (IndexDoesNotExistException)
                {

                }

                Thread.Sleep(32);
            }

            var msg = $"Got no index error for more than {timeout.Value}.";
            if (toWait.Count != 0)
                msg += $" Still waiting for following indexes: {string.Join(",", toWait)}";

            if (errorsShouldExists is null)
                throw new TimeoutException(msg);

            return null;
        }

        public static int WaitForEntriesCount(IDocumentStore store, string indexName, int minEntriesCount, string databaseName = null, TimeSpan? timeout = null, bool throwOnTimeout = true)
        {
            timeout ??= (Debugger.IsAttached
                ? TimeSpan.FromMinutes(15)
                : TimeSpan.FromMinutes(1));

            var sp = Stopwatch.StartNew();
            var entriesCount = -1;

            while (sp.Elapsed < timeout.Value)
            {
                MaintenanceOperationExecutor operations = string.IsNullOrEmpty(databaseName) == false ? store.Maintenance.ForDatabase(databaseName) : store.Maintenance;

                entriesCount = operations.Send(new GetIndexStatisticsOperation(indexName)).EntriesCount;

                if (entriesCount >= minEntriesCount)
                    return entriesCount;

                Thread.Sleep(32);
            }

            if (throwOnTimeout)
                throw new TimeoutException($"It didn't get min entries count {minEntriesCount} for index {indexName}. The index has {entriesCount} entries.");

            return entriesCount;
        }

        protected static async Task<TC> AssertWaitForSingleAsync<TC>(Func<Task<TC>> act, int timeout = 15000, int interval = 100) where TC : ICollection
        {
            var ret = await WaitForSingleAsync(act, timeout, interval);
            Assert.Single(ret);
            return ret;
        }
        protected static async Task<TC> AssertWaitForCountAsync<TC>(Func<Task<TC>> act, int count, int timeout = 15000, int interval = 100) where TC : ICollection
        {
            var ret = await WaitForCountAsync(act, count, timeout, interval);
            Assert.True(ret.Count == count, $"Expected {count}, Actual {ret.Count}");
            return ret;
        }

        protected static async Task<TC> WaitForSingleAsync<TC>(Func<Task<TC>> act, int timeout = 15000, int interval = 100) where TC : ICollection =>
            await WaitForCountAsync(act, 1, timeout, interval);
        protected static async Task<TC> WaitForCountAsync<TC>(Func<Task<TC>> act, int count, int timeout = 15000, int interval = 100) where TC : ICollection =>
            await WaitForPredicateAsync(a => a != null && a.Count == count, act, timeout, interval);

        protected static async Task<T> AssertWaitForGreaterThanAsync<T>(Func<Task<T>> act, T val, int timeout = 15000, int interval = 100) where T : IComparable
        {
            var ret = await WaitForGreaterThanAsync(act, val, timeout, interval);
            if (ret.CompareTo(val) > 0 == false)
                throw new TimeoutException($"Timeout {TimeSpan.FromMilliseconds(timeout):g}. Value should be greater then {val}. Current value {ret}");
            return ret;
        }

        protected static async Task<T> WaitForGreaterThanAsync<T>(Func<Task<T>> act, T val, int timeout = 15000, int interval = 100) where T : IComparable =>
            await WaitForPredicateAsync(a => a.CompareTo(val) > 0, act, timeout, interval);

        protected static async Task AssertWaitForTrueAsync(Func<Task<bool>> act, int timeout = 15000, int interval = 100)
        {
            Assert.True(await WaitForValueAsync(act, true, timeout, interval));
        }

        protected static async Task<T> AssertWaitForValueAsync<T>(Func<Task<T>> act, T expectedVal, int timeout = 15000, int interval = 100)
        {
            var ret = await WaitForValueAsync(act, expectedVal, timeout, interval);
            Assert.Equal(expectedVal, ret);
            return ret;
        }

        protected static async Task<T> WaitForValueAsync<T>(Func<Task<T>> act, T expectedVal, int timeout = 15000, int interval = 100) =>
             await WaitForPredicateAsync(a => (a == null && expectedVal == null) || (a != null && a.Equals(expectedVal)), act, timeout, interval);

        protected static async Task AssertWaitForExceptionAsync<T>(Func<Task> act, int timeout = 15000, int interval = 100)
            where T : class
        {
            await WaitAndAssertForValueAsync(async () =>
                await act().ContinueWith(t =>
                    t.Exception?.InnerException?.GetType()), typeof(T), timeout, interval);
        }

        protected static async Task<T> AssertWaitForNotNullAsync<T>(Func<Task<T>> act, int timeout = 15000, int interval = 100) where T : class
        {
            var ret = await WaitForNotNullAsync(act, timeout, interval);
            Assert.NotNull(ret);
            return ret;
        }

        protected static async Task<T> AssertWaitForNotDefaultAsync<T>(Func<Task<T>> act, int timeout = 15000, int interval = 100)
        {
            var ret = await WaitForNotDefaultAsync(act, timeout, interval);
            Assert.NotEqual(ret, default);
            return ret;
        }

        protected static async Task AssertWaitForNullAsync<T>(Func<Task<T>> act, int timeout = 15000, int interval = 100) where T : class
        {
            var result = await WaitForNullAsync(act, timeout, interval);
            Assert.Null(result);
        }

        protected static async Task WaitAndAssertForValueAsync<T>(Func<Task<T>> act, T expectedVal, int timeout = 15000, int interval = 100)
        {
            var val = await WaitForPredicateAsync(t => t.Equals(expectedVal), act, timeout, interval);
            Assert.Equal(expectedVal, val);
        }

        protected static async Task<T> AssertWaitFoGreaterAsync<T>(Func<T> act, T value, int timeout = 15000, int interval = 100) where T : IComparable
        {
            return await AssertWaitFoGreaterAsync(() => Task.FromResult(act()), value, timeout, interval);
        }

        protected static async Task<T> AssertWaitFoGreaterAsync<T>(Func<Task<T>> act, T value, int timeout = 15000, int interval = 100) where T : IComparable
        {
            var ret = await WaitForPredicateAsync(r => r.CompareTo(value) > 0, act, timeout, interval);
            Assert.NotNull(ret);
            return ret;
        }

        protected static async Task<T> WaitForNotNullAsync<T>(Func<Task<T>> act, int timeout = 15000, int interval = 100) where T : class =>
            await WaitForPredicateAsync(a => a != null, act, timeout, interval);

        protected static async Task<T> WaitForNotDefaultAsync<T>(Func<Task<T>> act, int timeout = 15000, int interval = 100) =>
            await WaitForPredicateAsync(a => !EqualityComparer<T>.Default.Equals(a, default), act, timeout, interval);

        protected static async Task<T> WaitForNullAsync<T>(Func<Task<T>> act, int timeout = 15000, int interval = 100) where T : class =>
            await WaitForPredicateAsync(a => a == null, act, timeout, interval);

        protected static async Task<T> WaitAndAssertForGreaterThanAsync<T>(Func<Task<T>> act, T expectedVal, int timeout = 15000, int interval = 100) where T : IComparable
        {
            var actualValue = await WaitForGreaterThanAsync(act, expectedVal, timeout, interval);
            Assert.True(actualValue.CompareTo(expectedVal) > 0, $"expectedVal:{expectedVal}, actualValue: {actualValue}");
            return actualValue;
        }

        protected async Task WaitAndAssertForValueAsync<T>(Func<T> act, T expectedVal, int timeout = 15000, int interval = 100)
        {
            var val = await WaitForPredicateAsync(t => t.Equals(expectedVal), () => Task.FromResult(act.Invoke()), timeout, interval);
            Assert.Equal(expectedVal, val);
        }

        private static async Task<T> WaitForPredicateAsync<T>(Predicate<T> predicate, Func<Task<T>> act, int timeout = 15000, int interval = 100)
        {
            if (Debugger.IsAttached)
                timeout *= 100;

            var sw = Stopwatch.StartNew();
            while (true)
            {
                try
                {
                    var currentVal = await act();
                    if (predicate(currentVal) || sw.ElapsedMilliseconds > timeout)
                        return currentVal;
                }
                catch
                {
                    if (sw.ElapsedMilliseconds > timeout)
                    {
                        throw;
                    }
                }
                await Task.Delay(interval);
            }
        }

        protected static async Task<T> WaitForValueAsync<T>(Func<T> act, T expectedVal, int timeout = 15000)
        {
            if (Debugger.IsAttached)
                timeout *= 100;

            var sw = Stopwatch.StartNew();
            do
            {
                try
                {
                    var currentVal = act();
                    if (expectedVal.Equals(currentVal))
                    {
                        return currentVal;
                    }
                    if (sw.ElapsedMilliseconds > timeout)
                    {
                        return currentVal;
                    }
                }
                catch
                {
                    if (sw.ElapsedMilliseconds > timeout)
                    {
                        throw;
                    }
                }
                await Task.Delay(100);
            } while (true);
        }

        protected static T WaitForValue<T>(Func<T> act, T expectedVal, int timeout = 15000, int interval = 16)
        {
            if (Debugger.IsAttached)
                timeout *= 100;

            var sw = Stopwatch.StartNew();
            do
            {
                try
                {
                    var currentVal = act();
                    if (expectedVal.Equals(currentVal))
                    {
                        return currentVal;
                    }
                    if (sw.ElapsedMilliseconds > timeout)
                    {
                        return currentVal;
                    }
                }
                catch
                {
                    if (sw.ElapsedMilliseconds > timeout)
                    {
                        throw;
                    }
                }

                Thread.Sleep(interval);
            } while (true);
        }

        public static void WaitForUserToContinueTheTest(string url, bool debug = true, X509Certificate2 clientCert = null)
        {
            if (debug && Debugger.IsAttached == false)
                return;

            if (clientCert != null && RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                using (var userPersonalStore = new X509Store(StoreName.My, StoreLocation.CurrentUser))
                {
                    userPersonalStore.Open(OpenFlags.ReadWrite);
                    userPersonalStore.Add(clientCert);
                }
            }

            try
            {
                var documentsPage = url + "/studio/index.html";

                OpenBrowser(documentsPage);// start the server

                do
                {
                    Thread.Sleep(500);
                } while (debug == false || Debugger.IsAttached);
            }
            finally
            {
                if (clientCert != null && RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    using (var userPersonalStore = new X509Store(StoreName.My, StoreLocation.CurrentUser))
                    {
                        userPersonalStore.Open(OpenFlags.ReadWrite);
                        userPersonalStore.Remove(clientCert);
                    }
                }
            }
        }

        public static void WaitForUserToContinueTheTest(IDocumentStore documentStore, bool debug = true, string database = null, X509Certificate2 clientCert = null)
        {
            if (clientCert != null && RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                using (var userPersonalStore = new X509Store(StoreName.My, StoreLocation.CurrentUser))
                {
                    userPersonalStore.Open(OpenFlags.ReadWrite);
                    userPersonalStore.Add(clientCert);
                }
            }

            try
            {
                if (debug && Debugger.IsAttached == false)
                    return;

                var urls = documentStore.Urls;
                if (clientCert != null)
                    Console.WriteLine($"Using certificate with serial: {clientCert.SerialNumber}");

                var databaseNameEncoded = Uri.EscapeDataString(database ?? documentStore.Database);
                var documentsPage = urls.First() + "/studio/index.html#databases/documents?&database=" + databaseNameEncoded + "&withStop=true&disableAnalytics=true";

                OpenBrowser(documentsPage);// start the server

                do
                {
                    Thread.Sleep(500);
                } while (documentStore.Commands(database).Head("Debug/Done") == null && (debug == false || Debugger.IsAttached));

                documentStore.Commands(database).Delete("Debug/Done", null);
            }
            finally
            {
                if (clientCert != null && RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    using (var userPersonalStore = new X509Store(StoreName.My, StoreLocation.CurrentUser))
                    {
                        userPersonalStore.Open(OpenFlags.ReadWrite);
                        userPersonalStore.Remove(clientCert);
                    }
                }
            }
        }

        protected ManualResetEventSlim WaitForIndexBatchCompleted(IDocumentStore store, Func<(string IndexName, bool DidWork), bool> predicate)
        {
            var database = GetDatabase(store.Database).Result;

            var mre = new ManualResetEventSlim();

            database.IndexStore.IndexBatchCompleted += x =>
            {
                if (predicate(x))
                    mre.Set();
            };

            return mre;
        }

        protected static async Task WaitForConflict(IDocumentStore slave, string id, int timeout = 15_000)
        {
            var timeoutAsTimeSpan = TimeSpan.FromMilliseconds(timeout);
            var sw = Stopwatch.StartNew();

            while (sw.Elapsed < timeoutAsTimeSpan)
            {
                using (var session = slave.OpenAsyncSession())
                {
                    try
                    {
                        await session.LoadAsync<dynamic>(id);
                        await Task.Delay(100);
                    }
                    catch (ConflictException)
                    {
                        return;
                    }
                }
            }

            throw new InvalidOperationException($"Waited '{sw.Elapsed}' for conflict on '{id}' but it did not happen.");
        }

        protected static bool WaitForCounterReplication(IEnumerable<IDocumentStore> stores, string docId, string counterName, long expected, TimeSpan timeout)
        {
            long? val = null;
            var sw = Stopwatch.StartNew();

            foreach (var store in stores)
            {
                val = null;
                while (sw.Elapsed < timeout)
                {
                    val = store.Operations
                        .Send(new GetCountersOperation(docId, new[] { counterName }))
                        .Counters[0]?.TotalValue;

                    if (val == expected)
                        break;

                    Thread.Sleep(100);
                }
            }

            return val == expected;
        }

        protected override void Dispose(ExceptionAggregator exceptionAggregator)
        {
            foreach (var store in CreatedStores)
            {
                if (store.WasDisposed)
                    continue;

                exceptionAggregator.Execute(store.Dispose);
            }
            CreatedStores.Clear();
        }

        protected X509Certificate2 RegisterClientCertificate(TestCertificatesHolder certificates, Dictionary<string, DatabaseAccess> permissions, SecurityClearance clearance = SecurityClearance.ValidUser, RavenServer server = null)
        {
            return RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, permissions, clearance, server);
        }

        protected X509Certificate2 RegisterClientCertificate(
            X509Certificate2 serverCertificate,
            X509Certificate2 clientCertificate,
            Dictionary<string, DatabaseAccess> permissions,
            SecurityClearance clearance = SecurityClearance.ValidUser,
            RavenServer server = null,
            string certificateName = "client certificate")
        {
            using var store = GetDocumentStore(new Options
            {
                CreateDatabase = false,
                Server = server,
                ClientCertificate = serverCertificate,
                AdminCertificate = serverCertificate,
                ModifyDocumentStore = s => s.Conventions = new DocumentConventions { DisableTopologyUpdates = true }
            });
            store.Maintenance.Server.Send(new PutClientCertificateOperation(certificateName, clientCertificate, permissions, clearance));
            return clientCertificate;
        }

        protected static IDisposable EnsureDatabaseDeletion(string databaseToDelete, IDocumentStore store)
        {
            return new DisposableAction(() =>
            {
                try
                {
                    store.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseToDelete, hardDelete: true));
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Failed to delete '{databaseToDelete}' database. Exception: " + e);

                    // do not throw to not hide an exception that could be thrown in a test
                }
            });
        }

        protected TestCertificatesHolder SetupServerAuthentication(IDictionary<string, string> customSettings = null, string serverUrl = null, TestCertificatesHolder certificates = null, [CallerMemberName] string caller = null)
        {
            if (customSettings == null)
                customSettings = new ConcurrentDictionary<string, string>();

            if (certificates == null)
                certificates = GenerateAndSaveSelfSignedCertificate(caller: caller);

            if (customSettings.TryGetValue(RavenConfiguration.GetKey(x => x.Security.CertificateLoadExec), out var _) == false)
                customSettings[RavenConfiguration.GetKey(x => x.Security.CertificatePath)] = certificates.ServerCertificatePath;

            customSettings[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = serverUrl ?? "https://" + Environment.MachineName + ":0";

            DoNotReuseServer(customSettings);

            return certificates;
        }

        private readonly Dictionary<(RavenServer Server, string Database), string> _serverDatabaseToMasterKey = new Dictionary<(RavenServer Server, string Database), string>();

        protected void PutSecretKeyForDatabaseInServerStore(string databaseName, RavenServer server)
        {
            var base64key = CreateMasterKey(out _);
            var base64KeyClone = new string(base64key.ToCharArray());

            EnsureServerMasterKeyIsSetup(server);

            Assert.True(server.ServerStore.EnsureNotPassiveAsync().Wait(TimeSpan.FromSeconds(30))); // activate license so we can insert the secret key
            Assert.True(server.ServerStore.LicenseManager.TryActivateLicenseAsync(Server.ThrowOnLicenseActivationFailure).Wait(TimeSpan.FromSeconds(30))); // activate license so we can insert the secret key

            server.ServerStore.PutSecretKey(base64key, databaseName, overwrite: true);

            _serverDatabaseToMasterKey.Add((server, databaseName), base64KeyClone);
        }

        protected void DeleteSecretKeyForDatabaseFromServerStore(string databaseName, RavenServer server)
        {
            server.ServerStore.DeleteSecretKey(databaseName);
        }

        protected string SetupEncryptedDatabase(out TestCertificatesHolder certificates, out byte[] masterKey, [CallerMemberName] string caller = null)
        {
            certificates = SetupServerAuthentication();
            var dbName = GetDatabaseName(caller);
            RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            string base64Key = CreateMasterKey(out masterKey);

            EnsureServerMasterKeyIsSetup(Server);

            Server.ServerStore.PutSecretKey(base64Key, dbName, true);
            return dbName;
        }

        private void EnsureServerMasterKeyIsSetup(RavenServer server)
        {
            var canUseProtect = PlatformDetails.RunningOnPosix == false;

            if (canUseProtect)
            {
                // sometimes when using `dotnet xunit` we get platform not supported from ProtectedData
                try
                {
#pragma warning disable CA1416 // Validate platform compatibility
                    ProtectedData.Protect(Encoding.UTF8.GetBytes("Is supported?"), null, DataProtectionScope.CurrentUser);
#pragma warning restore CA1416 // Validate platform compatibility
                }
                catch (PlatformNotSupportedException)
                {
                    canUseProtect = false;
                }
            }

            if (canUseProtect == false)
            {
                // so we fall back to a file
                if (File.Exists(server.ServerStore.Configuration.Security.MasterKeyPath) == false)
                {
                    server.ServerStore.Configuration.Security.MasterKeyPath = GetTempFileName();
                }
            }
        }

        public static string CreateMasterKey(out byte[] masterKey)
        {
            var buffer = new byte[32];
            using (var rand = RandomNumberGenerator.Create())
            {
                rand.GetBytes(buffer);
            }

            masterKey = buffer;

            var base64Key = Convert.ToBase64String(buffer);
            return base64Key;
        }

        public class Options
        {
            private readonly bool _frozen;

            private X509Certificate2 _clientCertificate;
            private X509Certificate2 _adminCertificate;
            private bool _createDatabase;
            private bool _deleteDatabaseOnDispose;
            private TimeSpan? _deleteTimeout;
            private RavenServer _server;
            private int _replicationFactor;
            private bool _ignoreDisabledDatabase;
            private Action<DocumentStore> _modifyDocumentStore;
            private Action<DatabaseRecord> _modifyDatabaseRecord;
            private Func<string, string> _modifyDatabaseName;
            private string _path;
            private bool _runInMemory = true;
            private bool _encrypted;
            private StringBuilder _descriptionBuilder;

            public static readonly Options Default = new Options(true);

            public Options() : this(false)
            {
            }

            private Options(bool frozen)
            {
                DeleteDatabaseOnDispose = true;
                CreateDatabase = true;
                ReplicationFactor = 1;

                _frozen = frozen;
            }

            public static Options ForMode(RavenDatabaseMode mode)
            {
                var options = new Options();
                switch (mode)
                {
                    case RavenDatabaseMode.Single:
                        options.DatabaseMode = RavenDatabaseMode.Single;
                        options.AddToDescription($"{nameof(RavenDataAttribute.DatabaseMode)} = {nameof(RavenDatabaseMode.Single)}");

                        return options;
                    case RavenDatabaseMode.Sharded:

                        options.ModifyDatabaseRecord = record =>
                        {
                            record.Shards = new[]
                            {
                                    new DatabaseTopology(),
                                    new DatabaseTopology(),
                                    new DatabaseTopology(),
                            };
                        };

                        options.ModifyDocumentStore = s => s.Conventions.OperationStatusFetchMode = OperationStatusFetchMode.Polling;
                        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "remove above after changes api is working");

                        options.DatabaseMode = RavenDatabaseMode.Sharded;
                        options.AddToDescription($"{nameof(RavenDataAttribute.DatabaseMode)} = {nameof(RavenDatabaseMode.Sharded)}");

                        return options;
                    case RavenDatabaseMode.All:
                    default:
                        throw new ArgumentOutOfRangeException(nameof(mode), mode, null);
                }
            }

            private void AddToDescription(string descriptionToAdd)
            {
                _descriptionBuilder ??= new StringBuilder();

                _descriptionBuilder
                    .Append(" ")
                    .Append(descriptionToAdd);
            }

            public string Path
            {
                get => _path;
                set
                {
                    AssertNotFrozen();
                    _path = value;
                }
            }

            public Func<string, string> ModifyDatabaseName
            {
                get => _modifyDatabaseName;
                set
                {
                    AssertNotFrozen();
                    _modifyDatabaseName = value;
                }
            }

            public Action<DatabaseRecord> ModifyDatabaseRecord
            {
                get => _modifyDatabaseRecord;
                set
                {
                    AssertNotFrozen();
                    _modifyDatabaseRecord = value;
                }
            }

            public Action<DocumentStore> ModifyDocumentStore
            {
                get => _modifyDocumentStore;
                set
                {
                    AssertNotFrozen();
                    _modifyDocumentStore = value;
                }
            }

            public bool IgnoreDisabledDatabase
            {
                get => _ignoreDisabledDatabase;
                set
                {
                    AssertNotFrozen();
                    _ignoreDisabledDatabase = value;
                }
            }

            public int ReplicationFactor
            {
                get => _replicationFactor;
                set
                {
                    AssertNotFrozen();
                    _replicationFactor = value;
                }
            }

            public RavenServer Server
            {
                get => _server;
                set
                {
                    AssertNotFrozen();
                    _server = value;
                }
            }

            public bool DeleteDatabaseOnDispose
            {
                get => _deleteDatabaseOnDispose;
                set
                {
                    AssertNotFrozen();
                    _deleteDatabaseOnDispose = value;
                }
            }

            public TimeSpan? DeleteTimeout
            {
                get => _deleteTimeout;
                set
                {
                    AssertNotFrozen();
                    _deleteTimeout = value;
                }
            }

            public bool CreateDatabase
            {
                get => _createDatabase;
                set
                {
                    AssertNotFrozen();
                    _createDatabase = value;
                    if (value == false)
                    {
                        ModifyDocumentStore = s => s.Conventions.DisableTopologyUpdates = true;
                    }
                }
            }

            public bool RunInMemory
            {
                get => _runInMemory;
                set
                {
                    AssertNotFrozen();
                    _runInMemory = value;
                }
            }

            public X509Certificate2 AdminCertificate
            {
                get => _adminCertificate;
                set
                {
                    AssertNotFrozen();
                    _adminCertificate = value;
                }
            }

            public X509Certificate2 ClientCertificate
            {
                get => _clientCertificate;
                set
                {
                    AssertNotFrozen();
                    _clientCertificate = value;
                }
            }

            public bool Encrypted
            {
                get => _encrypted;
                set
                {
                    AssertNotFrozen();
                    _encrypted = value;
                }
            }

            public RavenDatabaseMode DatabaseMode { get; private set; }

            public RavenSearchEngineMode SearchEngineMode { get; internal set; }

            private void AssertNotFrozen()
            {
                if (_frozen)
                    throw new InvalidOperationException("Options are frozen and cannot be changed.");
            }

            public override string ToString()
            {
                return _descriptionBuilder == null
                    ? base.ToString()
                    : _descriptionBuilder.ToString();
            }

            public Options Clone()
            {
                return new Options
                {
                    AdminCertificate = AdminCertificate,
                    ClientCertificate = ClientCertificate,
                    CreateDatabase = CreateDatabase,
                    DeleteDatabaseOnDispose = DeleteDatabaseOnDispose,
                    DeleteTimeout = DeleteTimeout,
                    Encrypted = Encrypted,
                    IgnoreDisabledDatabase = IgnoreDisabledDatabase,
                    ModifyDatabaseName = ModifyDatabaseName,
                    ModifyDatabaseRecord = ModifyDatabaseRecord,
                    ModifyDocumentStore = ModifyDocumentStore,
                    Path = Path,
                    ReplicationFactor = ReplicationFactor,
                    RunInMemory = RunInMemory,
                    Server = Server,
                    DatabaseMode = DatabaseMode,
                    _descriptionBuilder = new StringBuilder(_descriptionBuilder.ToString())
                };
            }
        }

        public static async Task WaitForPolicyRunner(DocumentDatabase database)
        {
            var loops = 10;
            await database.TimeSeriesPolicyRunner.HandleChanges();
            for (int i = 0; i < loops; i++)
            {
                var rolled = await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();
                if (rolled == 0)
                    return;
            }

            Assert.True(false, $"We still have pending rollups left.");
        }

        protected static void SaveChangesWithTryCatch<T>(IDocumentSession session, T loaded) where T : class
        {
            //This try catch is only to investigate RavenDB-15366 issue
            try
            {
                session.SaveChanges();
            }
            catch (Exception e)
            {
                if (!(session is InMemoryDocumentSessionOperations inMemoryDocumentSessionOperations))
                    throw;

                foreach (var entity in inMemoryDocumentSessionOperations.DocumentsByEntity)
                {
                    if (!(entity.Key is T t) || t != loaded)
                        continue;

                    var blittable = inMemoryDocumentSessionOperations.JsonConverter.ToBlittable(entity.Key, entity.Value);
                    throw new InvalidOperationException($"blittable: {blittable}\n documentInfo {entity.Value.Document}", e);
                }
            }
        }

        protected static async Task SaveChangesWithTryCatchAsync<T>(IAsyncDocumentSession session, T loaded) where T : class
        {
            //This try catch is only to investigate RavenDB-15366 issue
            try
            {
                await session.SaveChangesAsync();
            }
            catch (Exception e)
            {
                if (!(session is InMemoryDocumentSessionOperations inMemoryDocumentSessionOperations))
                    throw;

                foreach (var entity in inMemoryDocumentSessionOperations.DocumentsByEntity)
                {
                    if (!(entity.Key is T u) || u != loaded)
                        continue;

                    var blittable = inMemoryDocumentSessionOperations.JsonConverter.ToBlittable(entity.Key, entity.Value);
                    throw new InvalidOperationException($"blittable: {blittable}\n documentInfo {entity.Value.Document}", e);
                }
            }
        }

        protected void WriteDocDirectlyFromStorageToTestOutput(string storeDatabase, string docId, [CallerMemberName] string caller = null)
        {
            AsyncHelpers.RunSync(() => WriteDocDirectlyFromStorageToTestOutputAsync(storeDatabase, docId));
        }

        protected async Task WriteDocDirectlyFromStorageToTestOutputAsync(string storeDatabase, string docId, [CallerMemberName] string caller = null)
        {
            //This function is only to investigate RavenDB-15366 issue

            var db = await GetDatabase(storeDatabase);
            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var doc = db.DocumentsStorage.Get(context, docId);

                var sb = new StringBuilder();
                sb.AppendLine($"Test: '{caller}'. Document: '{docId}'. Data:");
                sb.AppendLine(doc.Data.ToString());

                Output?.WriteLine(sb.ToString());
                Console.WriteLine(sb.ToString());
            }
        }
    }
}
