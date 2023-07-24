using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using JetBrains.Annotations;
using NLog;
using NLog.Config;
using NLog.Targets;
using NLog.Targets.Wrappers;
using Raven.Client.ServerWide.Operations.Logs;
using Raven.Server.Config;
#if !RVN
using Raven.Server.Documents;
using Raven.Server.Documents.Sharding;
#endif
using Sparrow;
using Sparrow.Global;
using Sparrow.Logging;
using Sparrow.Server.Logging;
using LogLevel = NLog.LogLevel;

namespace Raven.Server.Logging;

internal static class RavenLogManagerServerExtensions
{
    private static readonly NullTarget NullTarget = new(nameof(NullTarget));

    internal static LoggingRule DefaultRule;

    private static LoggingRule DefaultAuditRule;

    private static readonly LoggingRule SystemRule = new()
    {
        RuleName = "Raven_System",
        FinalMinLevel = LogLevel.Warn,
        LoggerNamePattern = "System.*",
        Targets = { NullTarget }
    };

    private static readonly LoggingRule MicrosoftRule = new()
    {
        RuleName = "Raven_Microsoft",
        FinalMinLevel = LogLevel.Warn,
        LoggerNamePattern = "Microsoft.*",
        Targets = { NullTarget }
    };

#if !RVN
    internal static readonly LoggingRule AdminLogsRule = new()
    {
        RuleName = "Raven_WebSocket",
        LoggerNamePattern = "*",
        Targets = { new AsyncTargetWrapper(AdminLogsTarget.Instance) { QueueLimit = 128, OverflowAction = AsyncTargetWrapperOverflowAction.Discard } }
    };

    internal static readonly LoggingRule PipeRule = new()
    {
        RuleName = "Raven_Pipe",
        LoggerNamePattern = "*",
        Targets = { StreamTarget.Instance }
    };
#endif

    internal static readonly LoggingRule ConsoleRule = new()
    {
        RuleName = "Raven_Console",
        LoggerNamePattern = "*",
        Targets = {
            new ConsoleTarget
            {
                DetectConsoleAvailable = true,
                Layout = Constants.Logging.DefaultLayout,
            }
        }
    };

#if !RVN
    private static readonly ConcurrentDictionary<string, RavenAuditLogger> AuditLoggers = new(StringComparer.OrdinalIgnoreCase);
#endif

    public static Logger GetLoggerForCluster<T>(this RavenLogManager logManager, LoggingComponent component = null) => GetLoggerForCluster(logManager, typeof(T), component);

    public static Logger GetLoggerForCluster(this RavenLogManager logManager, [NotNull] Type type, LoggingComponent component = null)
    {
        if (type == null)
            throw new ArgumentNullException(nameof(type));

        return GetLoggerForResourceInternal(logManager, type.FullName, LoggingResource.Cluster, component);
    }

    public static Logger GetLoggerForServer<T>(this RavenLogManager logManager, LoggingComponent component = null) => GetLoggerForServer(logManager, typeof(T), component);

    public static Logger GetLoggerForServer(this RavenLogManager logManager, [NotNull] Type type, LoggingComponent component = null)
    {
        if (type == null)
            throw new ArgumentNullException(nameof(type));

        return GetLoggerForResourceInternal(logManager, type.FullName, LoggingResource.Server, component);
    }

#if !RVN
    public static Logger GetLoggerForDatabase<T>(this RavenLogManager logManager, [NotNull] DocumentDatabase database)
    {
        if (database == null)
            throw new ArgumentNullException(nameof(database));

        return GetLoggerForDatabase<T>(logManager, database.Name);
    }

    public static Logger GetLoggerForDatabase(this RavenLogManager logManager, Type type, DocumentDatabase database)
    {
        if (database == null)
            throw new ArgumentNullException(nameof(database));

        return GetLoggerForDatabase(logManager, type, database.Name);
    }

    public static Logger GetLoggerForDatabase<T>(this RavenLogManager logManager, [NotNull] ShardedDatabaseContext databaseContext)
    {
        if (databaseContext == null)
            throw new ArgumentNullException(nameof(databaseContext));

        return GetLoggerForDatabase<T>(logManager, databaseContext.DatabaseName);
    }

    public static Logger GetLoggerForDatabase(this RavenLogManager logManager, Type type, ShardedDatabaseContext databaseContext)
    {
        if (databaseContext == null)
            throw new ArgumentNullException(nameof(databaseContext));

        return GetLoggerForDatabase(logManager, type, databaseContext.DatabaseName);
    }
#endif

    public static Logger GetLoggerForDatabase<T>(this RavenLogManager logManager, [NotNull] string databaseName)
    {
        if (databaseName == null)
            throw new ArgumentNullException(nameof(databaseName));

        return GetLoggerForDatabase(logManager, typeof(T), databaseName);
    }

    public static Logger GetLoggerForDatabase(this RavenLogManager logManager, [NotNull] Type type, [NotNull] string databaseName)
    {
        if (type == null)
            throw new ArgumentNullException(nameof(type));
        if (databaseName == null)
            throw new ArgumentNullException(nameof(databaseName));

        return GetLoggerForDatabaseInternal(logManager, type.FullName, databaseName);
    }

#if !RVN
    public static Logger GetLoggerForIndex<T>(this RavenLogManager logManager, [NotNull] Raven.Server.Documents.Indexes.Index index)
    {
        if (index == null)
            throw new ArgumentNullException(nameof(index));

        return GetLoggerForIndex(logManager, typeof(T), index);
    }

    public static Logger GetLoggerForIndex(this RavenLogManager logManager, [NotNull] Type type, [NotNull] Raven.Server.Documents.Indexes.Index index)
    {
        if (type == null)
            throw new ArgumentNullException(nameof(type));
        if (index == null)
            throw new ArgumentNullException(nameof(index));

        return GetLoggerForIndexInternal(logManager, type.FullName, index.DocumentDatabase.Name, index.Name);
    }

    public static RavenAuditLogger GetAuditLoggerForServer(this RavenLogManager logManager)
    {
        return AuditLoggers.GetOrAdd(LoggingResource.Server.ToString(), r =>
        {
            var logger = LogManager.GetLogger("Audit")
                .WithProperty(Constants.Logging.Properties.Resource, r);

            return new RavenAuditLogger(logger);
        });
    }

    public static RavenAuditLogger GetAuditLoggerForDatabase(this RavenLogManager logManager, [NotNull] string databaseName)
    {
        if (databaseName == null)
            throw new ArgumentNullException(nameof(databaseName));

        return AuditLoggers.GetOrAdd(databaseName, r =>
        {
            var logger = LogManager.GetLogger("Audit")
                .WithProperty(Constants.Logging.Properties.Resource, r);

            return new RavenAuditLogger(logger);
        });
    }
#endif

    private static Logger GetLoggerForResourceInternal(RavenLogManager logManager, string name, LoggingResource resource, LoggingComponent component)
    {
        return LogManager.GetLogger(name)
            .WithProperty(Constants.Logging.Properties.Resource, resource)
            .WithProperty(Constants.Logging.Properties.Component, component?.ToString());
    }

    private static Logger GetLoggerForDatabaseInternal(RavenLogManager logManager, string name, string databaseName) =>
        LogManager.GetLogger(name)
            .WithProperty(Constants.Logging.Properties.Resource, databaseName);

    private static Logger GetLoggerForIndexInternal(RavenLogManager logManager, string name, string databaseName, string indexName) =>
        LogManager.GetLogger(name)
            .WithProperty(Constants.Logging.Properties.Resource, databaseName)
            .WithProperty(Constants.Logging.Properties.Component, indexName);

#if !RVN
    public static void ConfigureLogging(this RavenLogManager logManager, [NotNull] SetLogsConfigurationOperation.Parameters parameters)
    {
        if (parameters == null)
            throw new ArgumentNullException(nameof(parameters));

        if (parameters.MicrosoftLogs != null)
        {
            AssertNotFileConfig();

            SystemRule.FinalMinLevel = MicrosoftRule.FinalMinLevel = parameters.MicrosoftLogs.MinLevel.ToNLogFinalMinLogLevel();
        }

        if (parameters.Logs != null)
        {
            AssertNotFileConfig();

            DefaultRule.SetLoggingLevels(parameters.Logs.MinLevel.ToNLogLogLevel(), parameters.Logs.MaxLevel.ToNLogLogLevel());
            DefaultRule.FilterDefaultAction = parameters.Logs.LogFilterDefaultAction.ToNLogFilterResult();

            ApplyFilters(parameters.Logs.Filters, DefaultRule);
        }

        if (parameters.AdminLogs != null)
        {
            AdminLogsRule.SetLoggingLevels(parameters.AdminLogs.MinLevel.ToNLogLogLevel(), parameters.AdminLogs.MaxLevel.ToNLogLogLevel());
            AdminLogsRule.FilterDefaultAction = parameters.AdminLogs.LogFilterDefaultAction.ToNLogFilterResult();

            ApplyFilters(parameters.AdminLogs.Filters, AdminLogsRule);
        }

        LogManager.ReconfigExistingLoggers(purgeObsoleteLoggers: true);

        return;

        static void ApplyFilters(List<LogFilter> filters, LoggingRule rule)
        {
            rule.Filters.Clear();

            if (filters == null || filters.Count == 0)
                return;

            foreach (var filter in filters)
                rule.Filters.Add(new RavenConditionBasedFilter(filter));
        }
    }
#endif

    public static void ConfigureLogging(this RavenLogManager logManager, RavenConfiguration configuration)
    {
        if (configuration.Logs.ConfigPath != null)
        {
            LogManager.Setup(x => x.LoadConfigurationFromFile(configuration.Logs.ConfigPath.FullPath, optional: false));
            var c = LogManager.Configuration;
#if !RVN
            c.AddRule(AdminLogsRule);
#endif

            LogManager.Setup(x => x.LoadConfiguration(c));
            LogManager.ReconfigExistingLoggers(purgeObsoleteLoggers: true);

            return;
        }

        SystemRule.FinalMinLevel = MicrosoftRule.FinalMinLevel = configuration.Logs.MicrosoftMinLevel.ToNLogFinalMinLogLevel();
        ConsoleRule.DisableLoggingForLevels(LogLevel.Trace, LogLevel.Fatal);
#if !RVN
        PipeRule.DisableLoggingForLevels(LogLevel.Trace, LogLevel.Fatal);
#endif

        var fileTarget = new FileTarget
        {
            Name = nameof(FileTarget),
            CreateDirs = true,
            FileName = configuration.Logs.Path.Combine("${shortdate}.log").FullPath,
            ArchiveNumbering = ArchiveNumberingMode.DateAndSequence,
            Header = Constants.Logging.DefaultHeaderAndFooterLayout,
            Layout = Constants.Logging.DefaultLayout,
            Footer = Constants.Logging.DefaultHeaderAndFooterLayout,
            ConcurrentWrites = false,
            WriteFooterOnArchivingOnly = true,
            ArchiveAboveSize = configuration.Logs.ArchiveAboveSize.GetValue(SizeUnit.Bytes),
            EnableArchiveFileCompression = configuration.Logs.EnableArchiveFileCompression
        };

        if (configuration.Logs.MaxArchiveDays.HasValue)
            fileTarget.MaxArchiveDays = configuration.Logs.MaxArchiveDays.Value;

        if (configuration.Logs.MaxArchiveFiles.HasValue)
            fileTarget.MaxArchiveFiles = configuration.Logs.MaxArchiveFiles.Value;

        var fileTargetAsyncWrapper = new AsyncTargetWrapper(nameof(AsyncTargetWrapper), fileTarget);

        DefaultRule = new LoggingRule("*", configuration.Logs.MinLevel.ToNLogLogLevel(), configuration.Logs.MaxLevel.ToNLogLogLevel(), fileTargetAsyncWrapper)
        {
            RuleName = "Raven_Default"
        };

        DefaultAuditRule = new LoggingRule("Audit", LogLevel.Info, LogLevel.Info, NullTarget)
        {
            RuleName = "Raven_Default_Audit",
            Final = true
        };

        var config = new LoggingConfiguration();

        config.AddRule(SystemRule);
        config.AddRule(MicrosoftRule);
        config.AddRule(DefaultAuditRule);
        config.AddRule(DefaultRule);

        LogManager.Setup(x => x.LoadConfiguration(config));
        LogManager.ReconfigExistingLoggers(purgeObsoleteLoggers: true);
    }

#if !RVN
    public static void ConfigureAuditLog(this RavenLogManager logManager, RavenServer server, Logger logger)
    {
        var configuration = server.Configuration;

        if (configuration.Security.AuditLogPath == null)
            return;

        if (configuration.Security.AuthenticationEnabled == false)
        {
            if (logger.IsErrorEnabled)
                logger.Error("The audit log configuration 'Security.AuditLog.FolderPath' was specified, but the server is not running in a secured mode. Audit log disabled!");
            return;
        }

        // we have to do this manually because LoggingSource will ignore errors
        AssertCanWriteToAuditLogDirectory(configuration);

        var config = LogManager.Configuration ?? new LoggingConfiguration();

        var fileTarget = new FileTarget
        {
            Name = nameof(FileTarget),
            CreateDirs = true,
            FileName = configuration.Security.AuditLogPath.Combine("${shortdate}.audit.log").FullPath,
            ArchiveNumbering = ArchiveNumberingMode.DateAndSequence,
            Header = Constants.Logging.DefaultHeaderAndFooterLayout,
            Layout = Constants.Logging.DefaultLayout,
            Footer = Constants.Logging.DefaultHeaderAndFooterLayout,
            ConcurrentWrites = false,
            WriteFooterOnArchivingOnly = true,
            ArchiveAboveSize = configuration.Security.AuditLogArchiveAboveSize.GetValue(SizeUnit.Bytes),
            EnableArchiveFileCompression = configuration.Security.AuditLogEnableArchiveFileCompression
        };

        if (configuration.Security.AuditLogMaxArchiveDays.HasValue)
            fileTarget.MaxArchiveDays = configuration.Security.AuditLogMaxArchiveDays.Value;

        if (configuration.Security.AuditLogMaxArchiveFiles.HasValue)
            fileTarget.MaxArchiveFiles = configuration.Security.AuditLogMaxArchiveFiles.Value;

        var fileTargetAsyncWrapper = new AsyncTargetWrapper(nameof(AsyncTargetWrapper), fileTarget);

        DefaultAuditRule.Targets.Clear();
        DefaultAuditRule.Targets.Add(fileTargetAsyncWrapper);

        LogManager.Setup(x => x.LoadConfiguration(config));
        LogManager.ReconfigExistingLoggers(purgeObsoleteLoggers: true);

        if (RavenLogManager.Instance.IsAuditEnabled)
        {
            var auditLog = RavenLogManager.Instance.GetAuditLoggerForServer();
            auditLog.Audit($"Server started up, listening to {string.Join(", ", configuration.Core.ServerUrls)} with certificate {server.Certificate?.Certificate?.Subject} ({server.Certificate?.Certificate?.Thumbprint}), public url: {configuration.Core.PublicServerUrl}");
        }

        return;

        static void AssertCanWriteToAuditLogDirectory(RavenConfiguration configuration)
        {
            if (Directory.Exists(configuration.Security.AuditLogPath.FullPath) == false)
            {
                try
                {
                    Directory.CreateDirectory(configuration.Security.AuditLogPath.FullPath);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Cannot create audit log directory: {configuration.Security.AuditLogPath.FullPath}, treating this as a fatal error", e);
                }
            }
            try
            {
                var testFile = configuration.Security.AuditLogPath.Combine("write.test").FullPath;
                File.WriteAllText(testFile, "test we can write");
                File.Delete(testFile);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Cannot create new file in audit log directory: {configuration.Security.AuditLogPath.FullPath}, treating this as a fatal error", e);
            }
        }
    }

    public static LogsConfiguration GetLogsConfiguration(this RavenLogManager logManager, RavenServer server)
    {
        var defaultRule = DefaultRule;
        if (defaultRule == null)
            return null;

        var currentMinLevel = defaultRule.Levels.FirstOrDefault() ?? LogLevel.Off;
        var currentMaxLevel = defaultRule.Levels.LastOrDefault() ?? LogLevel.Off;

        return new LogsConfiguration
        {
            MinLevel = server.Configuration.Logs.MinLevel,
            MaxLevel = server.Configuration.Logs.MaxLevel,
            MaxArchiveFiles = server.Configuration.Logs.MaxArchiveFiles,
            EnableArchiveFileCompression = server.Configuration.Logs.EnableArchiveFileCompression,
            MaxArchiveDays = server.Configuration.Logs.MaxArchiveDays,
            ArchiveAboveSizeInMb = server.Configuration.Logs.ArchiveAboveSize.GetValue(SizeUnit.Megabytes),
            Path = server.Configuration.Logs.Path.FullPath,
            CurrentMinLevel = currentMinLevel.FromNLogLogLevel(),
            CurrentMaxLevel = currentMaxLevel.FromNLogLogLevel()
        };
    }

    public static AuditLogsConfiguration GetAuditLogsConfiguration(this RavenLogManager logManager, RavenServer server)
    {
        var defaultAuditRule = DefaultAuditRule;
        var currentLevel = defaultAuditRule?.Levels.FirstOrDefault() ?? LogLevel.Off;

        return new AuditLogsConfiguration
        {
            MaxArchiveFiles = server.Configuration.Security.AuditLogMaxArchiveFiles,
            EnableArchiveFileCompression = server.Configuration.Security.AuditLogEnableArchiveFileCompression,
            MaxArchiveDays = server.Configuration.Security.AuditLogMaxArchiveDays,
            ArchiveAboveSizeInMb = server.Configuration.Security.AuditLogArchiveAboveSize.GetValue(SizeUnit.Megabytes),
            Path = server.Configuration.Security.AuditLogPath?.FullPath,
            Level = currentLevel.FromNLogLogLevel(),
        };
    }


    public static MicrosoftLogsConfiguration GetMicrosoftLogsConfiguration(this RavenLogManager logManager, RavenServer server)
    {
        if (DefaultRule == null)
            return null;

        var microsoftRule = MicrosoftRule;
        var currentMinLevel = microsoftRule.FinalMinLevel ?? LogLevel.Off;

        return new MicrosoftLogsConfiguration
        {
            CurrentMinLevel = currentMinLevel.FromNLogLogLevel(), // TODO [ppekrol] this is incorrect, need to convert
            MinLevel = server.Configuration.Logs.MicrosoftMinLevel
        };
    }

    public static AdminLogsConfiguration GetAdminLogsConfiguration(this RavenLogManager logManager, RavenServer server)
    {
        var webSocketRule = AdminLogsRule;
        var currentMinLevel = webSocketRule.Levels.FirstOrDefault() ?? LogLevel.Off;
        var currentMaxLevel = webSocketRule.Levels.LastOrDefault() ?? LogLevel.Off;

        return new AdminLogsConfiguration
        {
            CurrentMinLevel = currentMinLevel.FromNLogLogLevel(),
            CurrentMaxLevel = currentMaxLevel.FromNLogLogLevel()
        };
    }

    public static IEnumerable<FileInfo> GetLogFiles(this RavenLogManager logManager, RavenServer server, DateTime? from, DateTime? to)
    {
        AssertNotFileConfig();

        var path = server.Configuration.Logs.Path.FullPath;
        if (Path.Exists(path) == false)
            yield break;

        foreach (var file in Directory.GetFiles(path, "*.log", SearchOption.TopDirectoryOnly))
        {
            var fileInfo = new FileInfo(file);
            var fileName = fileInfo.Name;
            var fileExtension = fileInfo.Extension;
            var fileNameWithoutExtension = fileName;
            if (string.IsNullOrEmpty(fileExtension) == false)
                fileNameWithoutExtension = fileNameWithoutExtension[..^fileExtension.Length];

            var firstIndexOfDot = fileNameWithoutExtension.IndexOf('.');
            if (firstIndexOfDot != -1)
                fileNameWithoutExtension = fileNameWithoutExtension[..firstIndexOfDot];

            if (DateTime.TryParseExact(fileNameWithoutExtension, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime dateTime) == false)
                continue;

            if (from.HasValue && dateTime < from)
                continue;

            if (to.HasValue && dateTime > to)
                continue;

            yield return fileInfo;
        }
    }

    private static void AssertNotFileConfig()
    {
        if (DefaultRule == null)
            throw new InvalidOperationException($"Cannot perform given action, because Logging was configured via NLog.config file.");
    }
#endif
}
