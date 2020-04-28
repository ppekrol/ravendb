using System;
using System.IO;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Sparrow.Extensions
{
    internal static class MemoryBufferExtensions
    {
        public static MemoryBufferFragment Read(this Stream stream, MemoryBuffer buffer)
        {
            var read = stream.Read(buffer.Base.Memory.Span);
            if (read == 0)
                return default;

            return buffer.Base.Slice(0, read);
        }

        public static async ValueTask<MemoryBufferFragment> ReadAsync(this Stream stream, MemoryBuffer buffer, CancellationToken token = default)
        {
            var read = await stream.ReadAsync(buffer.Base.Memory, token).ConfigureAwait(false);
            if (read == 0)
                return default;

            return buffer.Base.Slice(0, read);
        }

        public static async ValueTask<MemoryBufferFragment> ReceiveAsync(this WebSocket webSocket, MemoryBuffer buffer, CancellationToken token = default)
        {
            var result = await webSocket.ReceiveAsync(buffer.Base.Memory, token).ConfigureAwait(false);
            if (result.MessageType == WebSocketMessageType.Close)
                return default;

            return buffer.Base.Slice(0, result.Count);
        }

        public static void Parse(this UnmanagedJsonParser parser, JsonOperationContext context, Stream stream, MemoryBuffer buffer)
        {
            var needsBuffer = true;
            while (true)
            {
                if (needsBuffer)
                {
                    var bytesFragment = stream.Read(buffer);

                    context.EnsureNotDisposed();

                    if (bytesFragment.Memory.IsEmpty)
                        throw new EndOfStreamException("Stream ended without reaching end of JSON content");

                    parser.SetBuffer(bytesFragment);
                }

                var result = parser.Read();
                if (result)
                    return;

                needsBuffer = parser.BufferOffset == parser.BufferSize;
            }
        }

        public static async ValueTask ParseAsync(
            this UnmanagedJsonParser parser,
            JsonOperationContext context,
            Stream stream,
            MemoryBuffer buffer,
            int maxSize = int.MaxValue,
            string debugTag = null,
            CancellationToken? token = null)
        {
            var needsBuffer = true;
            while (true)
            {
                if (needsBuffer)
                {
                    var bytesFragment = await stream.ReadAsync(buffer, token ?? default).ConfigureAwait(false);

                    context.EnsureNotDisposed();

                    if (bytesFragment.Memory.IsEmpty)
                        throw new EndOfStreamException("Stream ended without reaching end of JSON content");

                    maxSize -= bytesFragment.Length;
                    if (maxSize < 0)
                        throw new ArgumentException($"The maximum size allowed for '{debugTag}' ({maxSize}) has been exceeded, aborting");

                    parser.SetBuffer(bytesFragment);
                }

                var result = parser.Read();
                if (result)
                    return;

                needsBuffer = parser.BufferOffset == parser.BufferSize;
            }
        }

        public static async Task ParseAsync(this UnmanagedJsonParser parser, JsonOperationContext context, WebSocket webSocket, MemoryBuffer buffer, CancellationToken token)
        {
            var needsBuffer = true;
            while (true)
            {
                if (needsBuffer)
                {
                    var bytesFragment = await webSocket.ReceiveAsync(buffer, token).ConfigureAwait(false);

                    token.ThrowIfCancellationRequested();
                    context.EnsureNotDisposed();

                    if (bytesFragment.Memory.IsEmpty)
                        throw new EndOfStreamException("Stream ended without reaching end of JSON content");

                    parser.SetBuffer(bytesFragment);
                }

                var result = parser.Read();
                if (result)
                    return;

                needsBuffer = parser.BufferOffset == parser.BufferSize;
            }
        }
    }
}
