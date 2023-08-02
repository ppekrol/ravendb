﻿using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes.Static.Sharding;

internal sealed class OrchestratorIndexingScope : CurrentIndexingScope
{
    public OrchestratorIndexingScope(TransactionOperationContext context, UnmanagedBuffersPoolWithLowMemoryHandling unmanagedBuffersPool) 
        : base(null, null, null, null, context, null, unmanagedBuffersPool)
    {
    }

    public override bool SupportsDynamicFieldsCreation => false;

    public override bool SupportsSpatialFieldsCreation => false;
}
