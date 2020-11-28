using System;
using System.Diagnostics;
using System.Threading.Tasks;
using SlowTests.Cluster;
using Sparrow.Server.LowMemory;
using Tests.Infrastructure;

namespace Tryouts
{
    public static class Program
    {
        static Program()
        {
            XunitLogging.RedirectStreams = false;
        }

        public static async Task Main(string[] args)
        {
            var result = CheckPageFileOnHdd.PosixIsSwappingOnHddInsteadOfSsd();

            Console.WriteLine($"Result: {result}");

            return;
            Console.WriteLine(Process.GetCurrentProcess().Id);
            for (int i = 0; i < 1230; i++)
            {
                var sp = Stopwatch.StartNew();
                Console.WriteLine($"Starting to run {i}");

                try
                {
                    using (var testOutputHelper = new ConsoleTestOutputHelper())
                    using (var test = new ClusterTransactionTests(testOutputHelper))
                    {
                        await test.ClusterTransactionWaitForIndexes(10);
                    }
                }
                catch (Exception e)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine(e);
                    Console.ForegroundColor = ConsoleColor.White;
                    // Console.ReadLine();
                    throw;
                }
                finally
                {
                    Console.WriteLine($"test ran for {sp.ElapsedMilliseconds:#,#}ms");
                }
            }
        }
    }
}
