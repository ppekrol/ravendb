﻿using Raven.Client.Documents.Changes;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Changes;

public interface IShardedDatabaseChanges :
    IDocumentChanges<BlittableJsonReaderObject>,
    IIndexChanges<BlittableJsonReaderObject>,
    ICounterChanges<BlittableJsonReaderObject>,
    ITimeSeriesChanges<BlittableJsonReaderObject>,
    IConnectableChanges<IShardedDatabaseChanges>
{
}
