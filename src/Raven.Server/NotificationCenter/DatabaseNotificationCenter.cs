﻿using System;
using System.Threading;
using JetBrains.Annotations;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.BackgroundWork;

namespace Raven.Server.NotificationCenter;

internal sealed class DatabaseNotificationCenter : AbstractDatabaseNotificationCenter
{
    [NotNull]
    private readonly DocumentDatabase _database;

    public DatabaseNotificationCenter([NotNull] DocumentDatabase database)
        : base(database.ServerStore, database.Name, database.Configuration, database.DatabaseShutdown)
    {
        _database = database;
    }

    public override void Initialize()
    {
        BackgroundWorkers.Add(new DatabaseStatsSender(_database, this));

        base.Initialize();
    }
}
