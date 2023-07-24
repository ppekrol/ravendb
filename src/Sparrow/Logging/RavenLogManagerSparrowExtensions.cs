using System;
using NLog;

namespace Sparrow.Logging;

internal static class RavenLogManagerSparrowExtensions
{
    public static Logger GetLoggerForSparrow<T>(this RavenLogManager logManager) => GetLoggerForSparrow(logManager, typeof(T));

    public static Logger GetLoggerForSparrow(this RavenLogManager logManager, Type type)
    {
        if (type == null)
            throw new ArgumentNullException(nameof(type));

        return LogManager.GetLogger(type.FullName)
            .WithProperty(Global.Constants.Logging.Properties.Resource, "Sparrow");
    }
}
