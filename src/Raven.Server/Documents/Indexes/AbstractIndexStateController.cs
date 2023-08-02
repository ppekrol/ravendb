﻿using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Indexes;

namespace Raven.Server.Documents.Indexes;

internal abstract class AbstractIndexStateController
{
    protected readonly ServerStore ServerStore;

    protected AbstractIndexStateController([NotNull] ServerStore serverStore)
    {
        ServerStore = serverStore ?? throw new ArgumentNullException(nameof(serverStore));
    }

    protected abstract string GetDatabaseName();

    protected abstract void ValidateIndex(string name, IndexState state);

    protected abstract ValueTask WaitForIndexNotificationAsync(long index);

    public async Task SetStateAsync(string name, IndexState state, string raftRequestId)
    {
        ValidateIndex(name, state);

        var command = new SetIndexStateCommand(name, state, GetDatabaseName(), raftRequestId);

        var (index, _) = await ServerStore.SendToLeaderAsync(command);

        await WaitForIndexNotificationAsync(index);
    }
}
