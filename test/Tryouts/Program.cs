using System;
using Tests.Infrastructure;
using SlowTests.Issues;

namespace Tryouts;

public static class Program
{
    static Program()
    {
        XunitLogging.RedirectStreams = false;
    }

    public static void Main(string[] args)
    {
        try
        {
            using (var testOutputHelper = new ConsoleTestOutputHelper())
            using (var x = new RavenDB_13291(testOutputHelper))
                x.CanMigrateTablesWithCounterWord();
        }
        catch (Exception e)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(e);
            Console.ForegroundColor = ConsoleColor.White;
            return;
        }
    }
}
