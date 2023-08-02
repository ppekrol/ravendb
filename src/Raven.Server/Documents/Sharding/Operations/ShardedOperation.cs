using Raven.Server.Documents.Operations;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Operations;

internal sealed class ShardedOperation : AbstractOperation
{
    [JsonDeserializationIgnore]
    public ShardedDatabaseMultiOperation Operation;
}
