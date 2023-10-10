using System;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow;
using Sparrow.Utils;

namespace Raven.Client.Json
{
    internal sealed class BlittableJsonContent : HttpContent
    {
        private readonly Func<Stream, Task> _asyncTaskWriter;
        private readonly DocumentConventions _conventions;

        public BlittableJsonContent(Func<Stream, Task> writer, DocumentConventions conventions)
        {
            _asyncTaskWriter = writer ?? throw new ArgumentNullException(nameof(writer));
            _conventions = conventions;

            if (_conventions.UseHttpCompression)
                Headers.ContentEncoding.Add(_conventions.HttpCompressionAlgorithm.GetContentEncoding());
        }

        public static long _wrote;

        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext context)
        {
            if (_conventions.UseHttpCompression == false)
            {
                await _asyncTaskWriter(stream).ConfigureAwait(false);
                return;
            }

            switch (_conventions.HttpCompressionAlgorithm)
            {
                case HttpCompressionAlgorithm.Gzip:
#if NETSTANDARD2_0 || NETCOREAPP2_1
                    using (var gzipStream = new GZipStream(stream, CompressionLevel.Fastest, leaveOpen: true))
#else
                    await using (var gzipStream = new GZipStream(stream, CompressionLevel.Fastest, leaveOpen: true))
#endif
                    {
                        await _asyncTaskWriter(gzipStream).ConfigureAwait(false);
                    }
                    break;
#if FEATURE_BROTLI_SUPPORT
                case HttpCompressionAlgorithm.Brotli:
                    await using (var brotliStream = new BrotliStream(stream, CompressionLevel.Optimal, leaveOpen: true))
                    {
                        await _asyncTaskWriter(brotliStream).ConfigureAwait(false);
                    }
                    break;
#endif
#if FEATURE_ZSTD_SUPPORT
                case HttpCompressionAlgorithm.Zstd:
                    await using (var zstdStream = ZstdStream.Compress(stream, CompressionLevel.Fastest, leaveOpen: true))
                    {
                        await _asyncTaskWriter(zstdStream).ConfigureAwait(false);
                    }
                    break;
#endif
                default:
                    throw new ArgumentOutOfRangeException();
            }

            if (_conventions.Id == Guid.Empty)
                return;

            var propertyInfo = stream.GetType().GetProperty("BytesWritten", BindingFlags.Instance | BindingFlags.Public);
            var bytesWritten = (long)propertyInfo.GetValue(stream);
            Interlocked.Add(ref _wrote, bytesWritten);
        }

        protected override bool TryComputeLength(out long length)
        {
            length = -1;
            return false;
        }

        public class CountingStream : Stream
        {
            private readonly Stream _inner;

            public static long _wrote;

            public CountingStream(Stream inner)
            {
                _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            }

            public override Task FlushAsync(CancellationToken cancellationToken)
            {
                return _inner.FlushAsync(cancellationToken);
            }

#if NET6_0_OR_GREATER
            public override ValueTask DisposeAsync()
            {
                //Console.WriteLine($"Wrote: {new Size(_wrote, SizeUnit.Bytes)}");
                return _inner.DisposeAsync();
            }
#endif

#if NET6_0_OR_GREATER
            public override int Read(Span<byte> buffer)
            {
                return _inner.Read(buffer);
            }
#endif

#if NET6_0_OR_GREATER
            public override void Write(ReadOnlySpan<byte> buffer)
            {
                Interlocked.Add(ref _wrote, buffer.Length);
                _inner.Write(buffer);
            }
#endif

            public override void Close()
            {
                _inner.Close();
            }

            public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                Interlocked.Add(ref _wrote, count);
                return _inner.WriteAsync(buffer, offset, count, cancellationToken);
            }

#if NET6_0_OR_GREATER
            public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = new CancellationToken())
            {
                Interlocked.Add(ref _wrote, buffer.Length);
                return _inner.WriteAsync(buffer, cancellationToken);
            }
#endif

            public override bool CanTimeout => _inner.CanTimeout;

#if NET6_0_OR_GREATER
            public override void CopyTo(Stream destination, int bufferSize)
            {
                _inner.CopyTo(destination, bufferSize);
            }
#endif

            public override Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
            {
                return _inner.CopyToAsync(destination, bufferSize, cancellationToken);
            }

            public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                return _inner.ReadAsync(buffer, offset, count, cancellationToken);
            }

            public override int ReadByte()
            {
                return _inner.ReadByte();
            }

            public override int ReadTimeout
            {
                get
                {
                    return _inner.ReadTimeout;
                }
                set
                {
                    _inner.ReadTimeout = value;
                }
            }

            public override void WriteByte(byte value)
            {
                Interlocked.Increment(ref _wrote);
                _inner.WriteByte(value);
            }

            public override int WriteTimeout
            {
                get
                {
                    return _inner.WriteTimeout;
                }
                set
                {
                    _inner.WriteTimeout = value;
                }
            }

#if NET6_0_OR_GREATER
            public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = new CancellationToken())
            {
                return _inner.ReadAsync(buffer, cancellationToken);
            }
#endif

            protected override void Dispose(bool disposing)
            {
                _inner.Dispose();

                //Console.WriteLine($"Wrote: {new Size(_wrote, SizeUnit.Bytes)}");
            }

            public override void Flush()
            {
                _inner.Flush();
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                return _inner.Read(buffer, offset, count);
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                return _inner.Seek(offset, origin);
            }

            public override void SetLength(long value)
            {
                _inner.SetLength(value);
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                Interlocked.Add(ref _wrote, count);
                _inner.Write(buffer, offset, count);
            }

            public override bool CanRead => _inner.CanRead;
            public override bool CanSeek => _inner.CanSeek;
            public override bool CanWrite => _inner.CanWrite;
            public override long Length => _inner.Length;
            public override long Position
            {
                get
                {
                    return _inner.Position;
                }

                set
                {
                    _inner.Position = value;
                }
            }
        }
    }
}
