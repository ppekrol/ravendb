﻿using Sparrow.Json;
using Sparrow.LowMemory;

namespace Raven.Server.ServerWide
{
    internal sealed class UnmanagedBuffersPoolWithLowMemoryHandling : UnmanagedBuffersPool, ILowMemoryHandler
    {
        public UnmanagedBuffersPoolWithLowMemoryHandling(string debugTag, string databaseName = null) : base(debugTag, databaseName)
        {
            LowMemoryNotification.Instance?.RegisterLowMemoryHandler(this);
        }
    }
}