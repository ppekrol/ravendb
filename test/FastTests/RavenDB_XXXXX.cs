using System;
using System.Diagnostics;
using System.IO.Compression;
using System.Threading.Tasks;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client;

public class RavenDB_XXXXX : RavenTestBase
{
    [Theory]
    [InlineData(HttpCompressionAlgorithm.Brotli, 1)]
    [InlineData(HttpCompressionAlgorithm.Zstd, 1)]
    [InlineData(HttpCompressionAlgorithm.Gzip, 1)]
    public async Task BulkInsert(HttpCompressionAlgorithm compressionAlgorithm, int count)
    {
        var path = NewDataPath();
        IOExtensions.DeleteDirectory(path);

        using (var store = GetDocumentStore(new Options
        {
            RunInMemory = false,
            Path = path,
            ModifyDocumentStore = s => s.Conventions.HttpCompressionAlgorithm = compressionAlgorithm
        }))
        {
            //WaitForUserToContinueTheTest(store, debug: false);

            BlittableJsonContent.CountingStream._wrote = 0;

            var sw = Stopwatch.StartNew();

            try
            {
                await using (var bulkInsert = store.BulkInsert(new BulkInsertOptions
                {
                    CompressionLevel = CompressionLevel.Fastest
                }))
                {
                    for (int i = 0; i < count; i++)
                    {
                        await bulkInsert.StoreAsync(new Company { Name = $"HR_{i}" });

                        if (i % 10_000 == 0)
                            Output.WriteLine(i.ToString());
                    }
                }
            }
            finally
            {
                sw.Stop();

                Output.WriteLine($"Items: {count}. Took: {sw.Elapsed}. Items/s: {count / sw.Elapsed.TotalSeconds}. Wrote: {new Size(BlittableJsonContent.CountingStream._wrote, SizeUnit.Bytes)}");
            }
        }
    }

    [Theory]
    [InlineData(HttpCompressionAlgorithm.Brotli)]
    [InlineData(HttpCompressionAlgorithm.Zstd)]
    [InlineData(HttpCompressionAlgorithm.Gzip)]
    public async Task Session(HttpCompressionAlgorithm compressionAlgorithm, int batches, int batchSize)
    {
        var path = NewDataPath();
        IOExtensions.DeleteDirectory(path);

        using (var store = GetDocumentStore(new Options
        {
            RunInMemory = false,
            Path = path,
            ModifyDocumentStore = s => s.Conventions.HttpCompressionAlgorithm = compressionAlgorithm
        }))
        {
            //WaitForUserToContinueTheTest(store, debug: false);

            BlittableJsonContent._wrote = 0;

            var sw = Stopwatch.StartNew();

            for (var i = 0; i < batches; i++)
            {
                using (var session = store.OpenAsyncSession())
                {
                    for (int j = 0; j < batchSize; j++)
                    {
                        await session.StoreAsync(new Order
                        {
                            Company = $"companies/{j}",
                            Employee = $"employees/{j}",
                            OrderedAt = DateTime.Now,
                            RequireAt = DateTime.Now.AddDays(j),
                            ShipVia = "shippers/{j}",
                            Freight = j,
                            ShipTo = new Address
                            {
                                City = "City",
                                Country = "Country",
                                Street = $"Street_{i}",
                                ZipCode = i * j
                            }
                        });
                    }

                    await session.SaveChangesAsync();
                }

                if (i % 1_000 == 0)
                    Output.WriteLine(i.ToString());
            }

            sw.Stop();

            Output.WriteLine($"Batches: {batches}. Batch Size: {batchSize} Took: {sw.Elapsed}. Items/s: {(batches * batchSize) / sw.Elapsed.TotalSeconds}. Wrote: {new Size(BlittableJsonContent._wrote, SizeUnit.Bytes)}");
        }
    }

    public RavenDB_XXXXX(ITestOutputHelper output) : base(output)
    {
    }
}
