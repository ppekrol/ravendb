using System;
using NLog;
using Sparrow.Logging;

namespace Raven.Client.Logging;

internal static class RavenLogManagerClientExtensions
{
    public static Logger GetLoggerForClient<T>(this RavenLogManager logManager) => GetLoggerForClient(logManager, typeof(T));

    public static Logger GetLoggerForClient(this RavenLogManager logManager, Type type)
    {
        if (type == null)
            throw new ArgumentNullException(nameof(type));

        return LogManager.GetLogger(type.FullName)
            .WithProperty(Sparrow.Global.Constants.Logging.Properties.Resource, "Client");
    }
}
