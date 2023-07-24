using System;
using NLog;
using Sparrow.Logging;

namespace Raven.Embedded.Logging;

internal static class RavenLogManagerEmbeddedExtensions
{
    public static Logger GetLoggerForEmbedded<T>(this RavenLogManager logManager) => GetLoggerForEmbedded(logManager, typeof(T));

    public static Logger GetLoggerForEmbedded(this RavenLogManager logManager, Type type)
    {
        if (type == null)
            throw new ArgumentNullException(nameof(type));

        return LogManager.GetLogger(type.FullName)
            .WithProperty(Sparrow.Global.Constants.Logging.Properties.Resource, "Embedded");
    }
}
