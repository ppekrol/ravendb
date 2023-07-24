using NLog;
using Sparrow.Json;
using Sparrow.LowMemory;

namespace Raven.Server.ServerWide
{
    public class UnmanagedBuffersPoolWithLowMemoryHandling : UnmanagedBuffersPool, ILowMemoryHandler
    {
        public UnmanagedBuffersPoolWithLowMemoryHandling(Logger logger, string debugTag, string databaseName = null) 
            : base(logger, debugTag, databaseName)
        {
            LowMemoryNotification.Instance?.RegisterLowMemoryHandler(this);
        }
    }
}
