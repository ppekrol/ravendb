using System;
using System.Buffers;
using Sparrow.Global;

namespace Sparrow.Json
{
    public unsafe class MemoryBuffer
    {
        public const int Size = 32 * Constants.Size.Kilobyte;

        public readonly MemoryBufferFragment Base;

        public MemoryBuffer(Memory<byte> buffer, int generation, JsonOperationContext context)
        {
            fixed (byte* pointer = buffer.Span)
                Base = new MemoryBufferFragment(buffer, pointer, generation, context);
        }

        public static ReturnBuffer ShortTermSingleUse(out MemoryBuffer buffer)
        {
            var bytes = ArrayPool<byte>.Shared.Rent(Size);
            var memory = new Memory<byte>(bytes);
            var memoryHandle = memory.Pin();

            buffer = new MemoryBuffer(memory, 0, null);

            return new ReturnBuffer(bytes, memoryHandle, buffer);
        }

        internal (IDisposable ReleaseBuffer, MemoryBuffer Buffer) Clone<T>(JsonContextPoolBase<T> pool)
            where T : JsonOperationContext
        {
            if (Base.Length != Size)
                throw new InvalidOperationException("Cloned buffer must be of the same size");

            var releaseCtx = pool.AllocateOperationContext(out T ctx);
            var returnBuffer = ctx.GetMemoryBuffer(out var buffer);
            var clean = new Disposer(returnBuffer, releaseCtx);
            try
            {
                Base.Memory.Span.CopyTo(buffer.Base.Memory.Span);

                return (clean, buffer);
            }
            catch
            {
                clean.Dispose();
                throw;
            }
        }

        private class Disposer : IDisposable
        {
            private readonly IDisposable[] _toDispose;

            public Disposer(params IDisposable[] toDispose)
            {
                _toDispose = toDispose;
            }

            public void Dispose()
            {
                foreach (var disposable in _toDispose)
                {
                    disposable.Dispose();
                }
            }
        }

        public struct ReturnBuffer : IDisposable
        {
            private AllocatedMemoryData _allocatedMemory;
            private byte[] _bytes;
            private MemoryHandle _memoryHandle;
#if DEBUG
            private MemoryBuffer _buffer;
#endif
            private readonly JsonOperationContext _parent;

            public ReturnBuffer(byte[] bytes, MemoryHandle memoryHandle, MemoryBuffer buffer)
            {
                _bytes = bytes;
                _memoryHandle = memoryHandle;
#if DEBUG
                _buffer = buffer;
#endif
                _allocatedMemory = null;
                _parent = null;
            }

            public ReturnBuffer(AllocatedMemoryData allocatedMemory, MemoryBuffer buffer, JsonOperationContext parent)
            {
                _bytes = null;
                _memoryHandle = default;
#if DEBUG
                _buffer = buffer;
#endif
                _allocatedMemory = allocatedMemory;
                _parent = parent;
            }

            public void Dispose()
            {
                if (_allocatedMemory == null && _bytes == null)
                    return;

#if DEBUG
                if (_buffer != null)
                {
                    //Debug.Assert(_buffer.IsReleased == false, "_buffer.IsReleased == false");
                    //_buffer.IsReleased = true;

                    _buffer = null;
                }
#endif

                if (_parent != null)
                {
                    //_parent disposal sets _managedBuffers to null,
                    //throwing ObjectDisposedException() to make it more visible
                    if (_parent.Disposed)
                        ThrowParentWasDisposed();

                    if (_allocatedMemory != null)
                    {
                        _parent.ReturnMemory(_allocatedMemory);
                        _allocatedMemory = null;
                    }
                }

                if (_bytes != null)
                {
                    _memoryHandle.Dispose();
                    _memoryHandle = default;

                    ArrayPool<byte>.Shared.Return(_bytes);
                    _bytes = null;
                }
            }

            private static void ThrowParentWasDisposed()
            {
                throw new ObjectDisposedException(
                    "ReturnBuffer should not be disposed after it's parent operation context was disposed");
            }
        }
    }

    public readonly unsafe struct MemoryBufferFragment
    {
#if DEBUG
        private readonly Memory<byte> _memory;
        private readonly byte* _pointer;
        private readonly int _length;

        private readonly int _generation;
        private readonly JsonOperationContext _parent;

        public Memory<byte> Memory
        {
            get
            {
                AssertState();
                return _memory;
            }
        }

        public byte* Pointer
        {
            get
            {
                AssertState();
                return _pointer;
            }
        }

        public int Length
        {
            get
            {
                AssertState();
                return _length;
            }
        }

#else
        public readonly Memory<byte> Memory;

        public readonly byte* Pointer;

        public readonly int Length;
#endif

        internal MemoryBufferFragment(Memory<byte> buffer, byte* pointer, int generation, JsonOperationContext context)
        {
            _memory = buffer;
            _pointer = pointer;

            _length = buffer.Length;
#if DEBUG
            _generation = generation;
            _parent = context;
#endif
        }

#if DEBUG

        private void AssertState()
        {
            if (_parent != null && _parent.Generation != _generation)
                throw new InvalidOperationException($"Buffer was created during generation '{_generation}', but current generation is '{_parent.Generation}'. Context was probably returned or reset.");
        }

        internal MemoryBufferFragment Slice(int start, int length)
        {
            return new MemoryBufferFragment(Memory.Slice(start, length), Pointer + start, _generation, _parent);
        }

#endif
    }
}
