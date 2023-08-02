namespace Raven.Server.ServerWide.Memory
{
    internal sealed class ProcessMemoryUsage
    {
        public ProcessMemoryUsage(long workingSet, long privateMemory)
        {
            WorkingSet = workingSet;
            PrivateMemory = privateMemory;
        }

        public readonly long WorkingSet;

        public readonly long PrivateMemory;
    }
}