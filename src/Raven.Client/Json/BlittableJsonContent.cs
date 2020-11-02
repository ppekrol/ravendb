using System;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

namespace Raven.Client.Json
{
    internal class BlittableJsonContent : HttpContent
    {
        private readonly Func<Stream, Task> _asyncTaskWriter;

        private readonly Action<Stream> _writer;

        public BlittableJsonContent(Func<Stream, Task> writer)
            : this()
        {
            _asyncTaskWriter = writer ?? throw new ArgumentNullException(nameof(writer));
        }

        public BlittableJsonContent(Action<Stream> writer)
            : this()
        {
            _writer = writer ?? throw new ArgumentNullException(nameof(writer));
        }

        private BlittableJsonContent()
        {
            Headers.ContentEncoding.Add("gzip");
        }

        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext context)
        {
            using (var gzipStream = new GZipStream(stream, CompressionMode.Compress, leaveOpen: true))
            {
                if (_asyncTaskWriter != null)
                {
                    await _asyncTaskWriter(gzipStream).ConfigureAwait(false);
                    return;
                }

                _writer(gzipStream);
            }
        }

        protected override bool TryComputeLength(out long length)
        {
            length = -1;
            return false;
        }
    }
}
