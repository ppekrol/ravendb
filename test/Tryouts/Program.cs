using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using FastTests.Client;
using Raven.Client.Http;
using Tests.Infrastructure;
using Raven.Server.Utils;
using SlowTests.Corax;
using SlowTests.Sharding.Cluster;
using Xunit;

namespace Tryouts;

public static class Program
{
    static Program()
    {
        XunitLogging.RedirectStreams = false;
    }

    public static async Task Main(string[] args)
    {
        Console.WriteLine(Process.GetCurrentProcess().Id);

        for (int i = 0; i < 1000; i++)
        {
            Console.WriteLine($"Starting to run {i}");

            try
            {
                TryRemoveDatabasesFolder();
                using (var testOutputHelper = new ConsoleTestOutputHelper())
                using (var test = new RavenDB_XXXXX(testOutputHelper))
                {
                    DebuggerAttachedTimeout.DisableLongTimespan = true;
                    await test.Session(HttpCompressionAlgorithm.Zstd, 1, 1);
                    //await test.BulkInsert(HttpCompressionAlgorithm.Brotli, 1_000_000);
                }
            }
            catch (Exception e)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(e);
                Console.ForegroundColor = ConsoleColor.White;
            }

            return;
        }
    }

    private static void TryRemoveDatabasesFolder()
    {
        var p = System.AppDomain.CurrentDomain.BaseDirectory;
        var dbPath = Path.Combine(p, "Databases");
        if (Directory.Exists(dbPath))
        {
            try
            {
                Directory.Delete(dbPath, true);
                Assert.False(Directory.Exists(dbPath), "Directory.Exists(dbPath)");
            }
            catch
            {
                Console.WriteLine($"Could not remove Databases folder on path '{dbPath}'");
            }
        }
    }
}
