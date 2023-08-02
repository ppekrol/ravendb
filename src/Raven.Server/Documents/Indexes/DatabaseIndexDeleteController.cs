﻿using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;

namespace Raven.Server.Documents.Indexes;

internal class DatabaseIndexDeleteController : AbstractIndexDeleteController
{
    private readonly DocumentDatabase _database;

    public DatabaseIndexDeleteController([NotNull] DocumentDatabase database)
        : base(database.ServerStore)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
    }

    protected override string GetDatabaseName() => _database.Name;

    protected override IndexDefinitionBaseServerSide GetIndexDefinition(string name)
    {
        var index = _database.IndexStore.GetIndex(name);
        return index?.Definition;
    }

    protected override async ValueTask CreateIndexAsync(IndexDefinition definition, string raftRequestId)
    {
        await _database.IndexStore.CreateIndex(definition, raftRequestId);
    }

    protected override async ValueTask WaitForIndexNotificationAsync(long index)
    {
        await _database.RachisLogIndexNotifications.WaitForIndexNotification(index, _database.ServerStore.Engine.OperationTimeout);
    }
}
