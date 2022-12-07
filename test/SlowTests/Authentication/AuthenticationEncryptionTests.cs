﻿using Tests.Infrastructure;
using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Config;
using SlowTests.Voron.Compaction;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Authentication
{
    public class AuthenticationEncryptionTests : RavenTestBase
    {
        public AuthenticationEncryptionTests(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenExplicitData(searchEngine: RavenSearchEngineMode.Lucene)]
        public async Task CanUseEncryption(RavenTestParameters configuration)
        {
            string dbName = Encryption.SetupEncryptedDatabase(out var certificates, out var _);

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = certificates.ServerCertificate.Value,
                ClientCertificate = certificates.ServerCertificate.Value,
                ModifyDatabaseName = s => dbName,
                ModifyDatabaseRecord = record => {
                    record.Settings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = configuration.SearchEngine.ToString();
                    record.Settings[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)] = configuration.SearchEngine.ToString();
                    record.Encrypted = true;
                },
                Path = NewDataPath()
            }))
            {
                store.Maintenance.Send(new CreateSampleDataOperation(Raven.Client.Documents.Smuggler.DatabaseItemType.Documents | Raven.Client.Documents.Smuggler.DatabaseItemType.Indexes));

                Indexes.WaitForIndexing(store);

                var file = GetTempFileName();
                var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                using (var commands = store.Commands())
                {
                    var result = commands.Query(new IndexQuery
                    {
                        Query = "FROM @all_docs",
                        WaitForNonStaleResults = true
                    });
                    Indexes.WaitForIndexing(store);

                    Assert.True(result.Results.Length > 1000);

                    QueryResult queryResult = store.Commands().Query(new IndexQuery
                    {
                        Query = "FROM INDEX 'Orders/ByCompany'"
                    });
                    QueryResult queryResult2 = store.Commands().Query(new IndexQuery
                    {
                        Query = "FROM INDEX 'Orders/Totals'"
                    });
                    QueryResult queryResult3 = store.Commands().Query(new IndexQuery
                    {
                        Query = "FROM INDEX 'Product/Search'"
                    });

                    Assert.True(queryResult.Results.Length > 0);
                    Assert.True(queryResult2.Results.Length > 0);
                    Assert.True(queryResult3.Results.Length > 0);
                }
            }
        }

        [Theory]
        [RavenExplicitData(searchEngine: RavenSearchEngineMode.Lucene)]
        public async Task CanRestartEncryptedDbWithIndexes(RavenTestParameters configuration)
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            var buffer = new byte[32];
            using (var rand = RandomNumberGenerator.Create())
            {
                rand.GetBytes(buffer);
            }
            var base64Key = Convert.ToBase64String(buffer);

            // sometimes when using `dotnet xunit` we get platform not supported from ProtectedData
            try
            {
#pragma warning disable CA1416 // Validate platform compatibility
                ProtectedData.Protect(Encoding.UTF8.GetBytes("Is supported?"), null, DataProtectionScope.CurrentUser);
#pragma warning restore CA1416 // Validate platform compatibility
            }
            catch (PlatformNotSupportedException)
            {
                // so we fall back to a file
                Server.ServerStore.Configuration.Security.MasterKeyPath = GetTempFileName();
            }

            Server.ServerStore.PutSecretKey(base64Key, dbName, true);

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbName,
                ModifyDatabaseRecord = record => {
                    record.Settings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = configuration.SearchEngine.ToString();
                    record.Settings[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)] = configuration.SearchEngine.ToString();
                    record.Encrypted = true;
                },
                Path = NewDataPath()
            }))
            {
                store.Maintenance.Send(new CreateSampleDataOperation(operateOnTypes: DatabaseSmugglerOptions.DefaultOperateOnTypes));

                using (var commands = store.Commands())
                {
                    // create auto map index
                    var command = new QueryCommand(commands.Session, new IndexQuery
                    {
                        Query = "FROM Orders WHERE Lines.Count > 2",
                        WaitForNonStaleResults = true
                    });

                    commands.RequestExecutor.Execute(command, commands.Context);

                    // create auto map reduce index
                    command = new QueryCommand(commands.Session, new IndexQuery
                    {
                        Query = "FROM Orders GROUP BY Company WHERE count() > 5 SELECT count() as TotalCount",
                        WaitForNonStaleResults = true
                    });

                    commands.RequestExecutor.Execute(command, commands.Context);

                    var indexDefinitions = store.Maintenance.Send(new GetIndexesOperation(0, 10));

                    Assert.Equal(9, indexDefinitions.Length); // 6 sample data indexes + 2 new dynamic indexes

                    Indexes.WaitForIndexing(store);

                    // perform a query per index
                    foreach (var indexDef in indexDefinitions)
                    {
                        QueryResult queryResult = store.Commands().Query(new IndexQuery
                        {
                            Query = $"FROM INDEX '{indexDef.Name}'"
                        });

                        Assert.True(queryResult.Results.Length > 0, $"queryResult.Results.Length > 0 for '{indexDef.Name}' index.");
                    }

                    // restart database
                    Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);
                    await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

                    // perform a query per index
                    foreach (var indexDef in indexDefinitions)
                    {
                        QueryResult queryResult = store.Commands().Query(new IndexQuery
                        {
                            Query = $"FROM INDEX '{indexDef.Name}'"
                        });

                        Assert.True(queryResult.Results.Length > 0, $"queryResult.Results.Length > 0 for '{indexDef.Name}' index.");
                    }
                }
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanCompactEncryptedDb(Options options)
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            var buffer = new byte[32];
            using (var rand = RandomNumberGenerator.Create())
            {
                rand.GetBytes(buffer);
            }
            var base64Key = Convert.ToBase64String(buffer);

            // sometimes when using `dotnet xunit` we get platform not supported from ProtectedData
            try
            {
#pragma warning disable CA1416 // Validate platform compatibility
                ProtectedData.Protect(Encoding.UTF8.GetBytes("Is supported?"), null, DataProtectionScope.CurrentUser);
#pragma warning restore CA1416 // Validate platform compatibility
            }
            catch (PlatformNotSupportedException)
            {
                // so we fall back to a file
                Server.ServerStore.Configuration.Security.MasterKeyPath = GetTempFileName();
            }

            Server.ServerStore.PutSecretKey(base64Key, dbName, true);
            options.AdminCertificate = adminCert;
            options.ClientCertificate = adminCert;
            options.ModifyDatabaseName = s => dbName;
            var path = NewDataPath();
            options.Path = path;
            options.ModifyDatabaseRecord = record =>
            {
                record.Settings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = options.SearchEngineMode.ToString();
                record.Settings[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)] = options.SearchEngineMode.ToString();
                record.Encrypted = true;
            };

            using (var store = GetDocumentStore(options))
            {
                store.Maintenance.Send(new CreateSampleDataOperation(operateOnTypes: DatabaseSmugglerOptions.DefaultOperateOnTypes));

                for (int i = 0; i < 3; i++)
                {
                    await store.Operations.Send(new PatchByQueryOperation(new IndexQuery
                    {
                        Query = @"FROM Orders UPDATE { put(""orders/"", this); } "
                    })).WaitForCompletionAsync(TimeSpan.FromSeconds(300));
                }

                Indexes.WaitForIndexing(store);

                var deleteOperation = store.Operations.Send(new DeleteByQueryOperation(new IndexQuery() { Query = "FROM orders" }));
                await deleteOperation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                var oldSize = StorageCompactionTestsSlow.GetDirSize(new DirectoryInfo(path));

                var compactOperation = store.Maintenance.Server.Send(new CompactDatabaseOperation(new CompactSettings
                {
                    DatabaseName = store.Database,
                    Documents = true,
                    Indexes = new[] { "Orders/ByCompany", "Orders/Totals" }
                }));
                await compactOperation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                var newSize = StorageCompactionTestsSlow.GetDirSize(new DirectoryInfo(path));

                Assert.True(oldSize > newSize);
            }
        }
    }
}
