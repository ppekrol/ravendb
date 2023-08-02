using Raven.Server.Documents.Sharding.NotificationCenter;

namespace Raven.Server.Documents.Sharding;

internal partial class ShardedDatabaseContext
{
    public readonly ShardedDatabaseNotificationCenter NotificationCenter;
}
