using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Utils;
using Sparrow.LowMemory;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16105 : RavenTestBase
    {
        public RavenDB_16105(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task T1()
        {
            string dbName = SetupEncryptedDatabase(out var certificates, out var _);

            var path = NewDataPath();

            IOExtensions.DeleteDirectory(path);

            using (var store = GetDocumentStore(new Options()
            {
                AdminCertificate = certificates.ServerCertificate.Value,
                ClientCertificate = certificates.ServerCertificate.Value,
                ModifyDatabaseName = s => dbName,
                Path = path,
                RunInMemory = false,
                ModifyDatabaseRecord = r =>
                {
                    r.Encrypted = true;
                }
            }))
            {
                var databaseRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));

                Console.WriteLine($"Database: {dbName}. Encrypted: {databaseRecord.Encrypted}. Path: {path}");

                using (var process = Process.GetCurrentProcess())
                {
                    AffinityHelper.SetProcessAffinity(process, 2, processAffinityMask: null, out var currentCores);

                    Console.WriteLine($"Affinity: {currentCores}");
                }

                await store.Maintenance.SendAsync(new CreateSampleDataOperation(DatabaseSmugglerOptions.DefaultOperateOnTypes));

                await PatchAsync(store);

                var doWork = true;
                _ = Task.Factory.StartNew(() =>
                {
                    while (doWork)
                    {
                        var random = new Random();
                        Thread.Sleep(random.Next(1000, 5000));
                        LowMemoryNotification.Instance.SimulateLowMemoryNotification();
                    }
                });

                var tasks = new List<Task>();

                tasks.Add(PatchAsync(store));
                tasks.Add(PatchAsync(store));
                tasks.Add(PatchAsync(store));
                tasks.Add(PatchAsync(store));
                tasks.Add(PatchAsync(store));

                await Task.WhenAll(tasks);

                doWork = false;

                var stats = await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation());

                Console.WriteLine($"Total: {stats.CountOfDocuments}");
                Console.WriteLine($"Orders: {stats.Collections["Orders"]}");

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(10));

                RavenTestHelper.AssertNoIndexErrors(store);
            }
        }

        private async Task PatchAsync(IDocumentStore store)
        {
            var operation = await store.Operations.SendAsync(new PatchByQueryOperation(@"
from Orders update {
 put('Orders/', this)
  put('Orders/', this)
   put('Orders/', this)
    put('Orders/', this)
     put('Orders/', this)
      put('Orders/', this)
       put('Orders/', this)
        put('Orders/', this)
}
"));

            await operation.WaitForCompletionAsync();
        }
    }
}
