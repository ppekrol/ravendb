﻿using NLog;

namespace Sparrow.Logging;

internal class RavenLogManager
{
    public static readonly RavenLogManager Instance = new();

    public bool IsAuditEnabled;

    private RavenLogManager()
    {
        var innerLogger = LogManager.GetLogger("Audit");
        IsAuditEnabled = innerLogger.IsInfoEnabled;

        LogManager.ConfigurationChanged += (_, _) => IsAuditEnabled = innerLogger.IsInfoEnabled;
    }
}
