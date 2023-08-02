using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using Raven.Client.Documents;
using Raven.Client.Exceptions.Database;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using Index = Raven.Server.Documents.Indexes.Index;

// ReSharper disable ExplicitCallerInfoArgument

namespace FastTests
{
    public abstract class RavenLowLevelTestBase : TestBase
    {
        private readonly List<string> _databases = new List<string>();

        protected RavenLowLevelTestBase(ITestOutputHelper output) : base(output)
        {
        }

        internal static void WaitForIndexMap(Index index, long etag)
        {
            var timeout = Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(15);
            Assert.True(SpinWait.SpinUntil(() => index.GetLastMappedEtagsForDebug().Values.Min() == etag, timeout));
        }

        internal IDisposable CreatePersistentDocumentDatabase(string dataDirectory, out DocumentDatabase db, Action<Dictionary<string, string>> modifyConfiguration = null, [CallerMemberName]string caller = null)
        {
            var database = CreateDocumentDatabase(runInMemory: false, dataDirectory: dataDirectory, caller: caller, modifyConfiguration: modifyConfiguration);
            db = database;
            Debug.Assert(database != null);
            return new DisposableAction(() =>
            {
                DeleteDatabase(database.Name);
            });
        }

        internal DocumentDatabase CreateDocumentDatabaseForSearchEngine(RavenTestParameters config)
        {
            return CreateDocumentDatabase(modifyConfiguration: dictionary =>
                dictionary[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = config.SearchEngine.ToString());
        }
        
        internal DocumentDatabase CreateDocumentDatabase([CallerMemberName] string caller = null, bool runInMemory = true, string dataDirectory = null, Action<Dictionary<string, string>> modifyConfiguration = null)
        {
            var name = GetDatabaseName(caller);

            return CreateDatabaseWithName(runInMemory, dataDirectory, modifyConfiguration, name);
        }

        internal DocumentDatabase CreateDatabaseWithName(bool runInMemory, string dataDirectory, Action<Dictionary<string, string>> modifyConfiguration, string name)
        {
            _databases.Add(name);

            if (string.IsNullOrEmpty(dataDirectory))
                dataDirectory = NewDataPath(name);

            var configuration = new Dictionary<string, string>();
            configuration.Add(RavenConfiguration.GetKey(x => x.Indexing.MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory), int.MaxValue.ToString());
            configuration.Add(RavenConfiguration.GetKey(x => x.Core.DataDirectory), dataDirectory);
            configuration.Add(RavenConfiguration.GetKey(x => x.Core.RunInMemory), runInMemory.ToString());
            configuration.Add(RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexCannotBeOpened), "true");

            modifyConfiguration?.Invoke(configuration);

            using (var store = new DocumentStore
            {
                Urls = UseFiddler(Server.WebUrl),
                Database = name
            })
            {
                store.Initialize();

                var doc = new DatabaseRecord(name)
                {
                    Settings = configuration
                };

                var result = store.Maintenance.Server.Send(new CreateDatabaseOperation(doc, replicationFactor: 1));

                try
                {
                    return AsyncHelpers.RunSync(() => GetDatabase(name));
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Database {result.Name} was created with '{result.RaftCommandIndex}' index.", e);
                }
            }
        }

        protected void DeleteDatabase(string dbName)
        {
            using (var store = new DocumentStore
            {
                Urls = UseFiddler(Server.WebUrl),
                Database = dbName
            })
            {
                store.Initialize();

                store.Maintenance.Server.Send(new DeleteDatabasesOperation(dbName, true, timeToWaitForConfirmation: TimeSpan.FromSeconds(30)));
            }
        }

        internal override void Dispose(ExceptionAggregator exceptionAggregator)
        {
            if (_databases.Count == 0)
                return;

            using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                foreach (var database in _databases)
                {
                    exceptionAggregator.Execute(() =>
                    {
                        try
                        {
                            Server.ServerStore.DatabasesLandlord.UnloadDirectly(database);
                        }
                        catch (DatabaseDisabledException)
                        {
                        }
                        catch (AggregateException ae) when (ae.InnerException is DatabaseDisabledException)
                        {
                        }
                    });

                    exceptionAggregator.Execute(() =>
                    {
                        AsyncHelpers.RunSync(async () =>
                        {
                            try
                            {
                                var (index, _) = await Server.ServerStore.DeleteDatabaseAsync(database, hardDelete: true, fromNodes: new[] { Server.ServerStore.NodeTag }, Guid.NewGuid().ToString());
                                await Server.ServerStore.Cluster.WaitForIndexNotification(index);
                            }
                            catch (DatabaseDoesNotExistException)
                            {
                            }
                            catch (Exception e) when (e.InnerException is DatabaseDoesNotExistException)
                            {
                            }
                        });
                    });
                }
            }
        }

        protected static BlittableJsonReaderObject CreateDocument(JsonOperationContext context, string key, DynamicJsonValue value)
        {
            return context.ReadObject(value, key, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
        }
    }
}
