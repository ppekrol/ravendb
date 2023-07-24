using System;
using System.Threading;
using System.Threading.Tasks;
using NLog;

namespace Raven.Server.Utils
{
    public class UnhandledExceptions
    {
        public static void Track(Logger logger)
        {
            AppDomain.CurrentDomain.UnhandledException += (sender, args) =>
            {
                if (logger.IsFatalEnabled == false)
                    return;

                if (args.ExceptionObject is Exception ex)
                {
                    logger.Fatal(ex, "UnhandledException occurred.");
                }
                else
                {
                    var exceptionString = $"UnhandledException: {args.ExceptionObject.ToString() ?? "null"}.";
                    logger.Fatal(exceptionString);
                }

                Console.Error.WriteLine("UnhandledException occurred");
                Console.Error.WriteLine(args.ExceptionObject);

                LogManager.Shutdown();
            };

            TaskScheduler.UnobservedTaskException += (sender, args) =>
            {
                if (logger.IsInfoEnabled == false)
                    return;

                if (args.Observed)
                    return;

                logger.Info(args.Exception, "UnobservedTaskException occurred.");
            };
        }
    }
}
