using System;
using System.Collections.Generic;

namespace Raven.Server.Documents.Handlers.Batches.Commands;

internal interface IBatchCommand : IDisposable
{
    public HashSet<string> ModifiedCollections { get; set; }

    public string LastChangeVector { get; set; }

    public long LastTombstoneEtag { get; set; }

    public bool IsClusterTransaction { get; set; }
}
