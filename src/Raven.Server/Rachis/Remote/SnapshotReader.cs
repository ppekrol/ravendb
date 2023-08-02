using System;
using System.Buffers;
using System.IO;
using Sparrow.Binary;

namespace Raven.Server.Rachis.Remote
{
    internal sealed class RemoteToStreamSnapshotReader : RemoteSnapshotReader
    {
        private readonly Stream _stream;

        public RemoteToStreamSnapshotReader(RemoteConnection parent, Stream stream) 
            : base(parent)
        {
            _stream = stream;
        }

        public override void ReadExactly(int size)
        {
            base.ReadExactly(size);
            _stream.Write(Buffer, 0, size);
        }
    }
        
    internal sealed class StreamSnapshotReader : SnapshotReader
    {
        private readonly Stream _stream;

        public StreamSnapshotReader(Stream stream)
        {
            _stream = stream;
        }

        protected override int InternalRead(int offset, int count) => _stream.Read(Buffer, offset, count);
    }
        
    interal class RemoteSnapshotReader : SnapshotReader
    {
        private readonly RemoteConnection _parent;

        public RemoteSnapshotReader(RemoteConnection parent)
        {
            _parent = parent;
        }

        protected override int InternalRead(int offset, int count) => _parent.Read(Buffer, offset, count);
    }
    
    internal abstract class SnapshotReader : IDisposable
    {
        public byte[] Buffer { get; private set;}

        protected SnapshotReader()
        {
            Buffer = ArrayPool<byte>.Shared.Rent(1024);
        }
        
        public int ReadInt32()
        {
            ReadExactly(sizeof(int));
            return BitConverter.ToInt32(Buffer, 0);
        }

        public long ReadInt64()
        {
            ReadExactly(sizeof(long));
            return BitConverter.ToInt64(Buffer, 0);
        }

        public virtual void ReadExactly(int size)
        {
            if (Buffer.Length < size)
            {
                ArrayPool<byte>.Shared.Return(Buffer);
                Buffer = ArrayPool<byte>.Shared.Rent(Bits.PowerOf2(size));
            }
            var totalRead = 0;
            while (totalRead < size)
            {
                var read = InternalRead(totalRead, size - totalRead);
                if (read == 0)
                    throw new EndOfStreamException();
                totalRead += read;
            }
        }
            
        protected abstract int InternalRead(int offset, int count);
        public void Dispose()
        {
            ArrayPool<byte>.Shared.Return(Buffer);
        }
    }
}
