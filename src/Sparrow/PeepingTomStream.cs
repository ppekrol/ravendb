using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Sparrow
{
    public class PeepingTomStream : IDisposable
    {
        public const int BufferWindowSize = 4096;

        private readonly MemoryBuffer _bufferWindow;
        private int _pos;
        private readonly Stream _stream;
        private bool _firstWindow = true;
        private MemoryBuffer.ReturnBuffer _returnedBuffer;

        public PeepingTomStream(Stream stream, JsonOperationContext context)
        {
            _stream = stream;
            _returnedBuffer = context.GetMemoryBuffer(out _bufferWindow);
        }

        public MemoryBufferFragment Read(MemoryBuffer buffer)
        {
            var read = _stream.Read(buffer);
            return ReadInternal(read);
        }

        public async ValueTask<MemoryBufferFragment> ReadAsync(MemoryBuffer buffer, CancellationToken token = default)
        {
            var read = await _stream.ReadAsync(buffer, token).ConfigureAwait(false);
            return ReadInternal(read);
        }

        private unsafe MemoryBufferFragment ReadInternal(MemoryBufferFragment buffer)
        {
            var totalToRead = buffer.Length < BufferWindowSize ? buffer.Length : BufferWindowSize;

            var pDest = _bufferWindow.Base.Pointer;
            var pSrc = buffer.Pointer;
            var pBufferWindowStart = pDest + _pos;
            var pBufferStart = pSrc + buffer.Length - totalToRead;

            _pos += totalToRead;

            if (_pos > BufferWindowSize) // copy in two parts
            {
                var newTotal = BufferWindowSize - (_pos - totalToRead);
                Memory.Copy(pBufferWindowStart, pBufferStart, newTotal);
                var nextLength = totalToRead - newTotal;
                Debug.Assert(nextLength <= BufferWindowSize);
                Memory.Copy(pDest, pBufferStart + newTotal, nextLength);

                _firstWindow = false;
                _pos %= BufferWindowSize;
            }
            else
            {
                Memory.Copy(pBufferWindowStart, pBufferStart, totalToRead);
            }

            return buffer;
        }

        public unsafe byte[] PeepInReadStream()
        {
            // return the last 4K starting at the last position in the array,
            // and continue to copy from the start of the array till the last position.
            // however if the buffer wasn't overrunning its tail (_firstWindow == true) then
            // we copy from the start to last position only.
            int start, size;
            if (_firstWindow)
            {
                start = 0;
                size = _pos;
            }
            else
            {
                start = _pos;
                size = BufferWindowSize;
            }
            // search for the first byte which represent a single UTF character
            // (because 'start' might point to a byte in a middle of set of bytes
            // representing single character, so 0x80 represent start of char in utf8)
            var originalStart = start;

            for (var p = _bufferWindow.Base.Pointer; (*(p + start) & 0x80) != 0; p++)
            {
                start++;
                size--;

                // requested size doesn't contains utf8 character
                if (size == 0)
                    return new byte[0];

                // looped through the entire buffer without utf8 character found
                if (start == originalStart)
                    return new byte[0];

                if (start >= BufferWindowSize)
                    start = 0;
            }
            var buf = new byte[size];
            if (size == 0)
                return buf;
            byte* pSrc = _bufferWindow.Base.Pointer;
            fixed (byte* pDest = buf)
            {
                var firstSize = size - start;
                Memory.Copy(pDest, pSrc + start, firstSize);
                Memory.Copy(pDest + firstSize, pSrc, start);
                return buf;
            }
        }

        public void Dispose()
        {
            // we do not dispose _stream
            _returnedBuffer.Dispose();
        }
    }
}
