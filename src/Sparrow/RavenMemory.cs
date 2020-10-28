using System;
using Sparrow.Json;

namespace Sparrow
{
    public unsafe readonly struct RavenMemory
    {
        public readonly byte* Address;

        public readonly Memory<byte> Memory;

        private readonly UnmanagedMemoryManager _memoryManager;

        public RavenMemory(byte* address, Memory<byte> memory)
        {
            Address = address;
            _memoryManager = null;
            Memory = memory;
        }

        public RavenMemory(byte* address, int size)
        {
            Address = address;
            _memoryManager = new UnmanagedMemoryManager(address, size);
            Memory = _memoryManager.Memory;
        }
    }
}
