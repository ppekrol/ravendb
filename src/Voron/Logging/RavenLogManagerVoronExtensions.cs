using System;
using NLog;
using Sparrow.Global;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Voron.Logging;

internal static class RavenLogManagerVoronExtensions
{
    public static Logger GetLoggerForGlobalVoron<T>(this RavenLogManager logManager) => GetLoggerForGlobalVoron(logManager, typeof(T));

    public static Logger GetLoggerForGlobalVoron(this RavenLogManager logManager, Type type)
    {
        return LogManager.GetLogger(type.FullName)
            .WithProperty(Constants.Logging.Properties.Resource, LoggingResource.Voron);
    }

    public static Logger GetLoggerForVoron<T>(this RavenLogManager logManager, StorageEnvironmentOptions options, string filePath) => GetLoggerForVoron(logManager, typeof(T), options, filePath);

    public static Logger GetLoggerForVoron(this RavenLogManager logManager, Type type, StorageEnvironmentOptions options, string filePath)
    {
        if (options == null) 
            throw new ArgumentNullException(nameof(options));

        return LogManager.GetLogger(type.FullName)
            .WithProperty(Constants.Logging.Properties.Resource, options.LoggingResource)
            .WithProperty(Constants.Logging.Properties.Component, options.LoggingComponent)
            .WithProperty(Constants.Logging.Properties.Other, filePath);
    }
}
