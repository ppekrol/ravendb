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
        private readonly Func<Stream, Task> _asyncWriter;

        private readonly Action<Stream> _writer;

        public BlittableJsonContent(Func<Stream, Task> writer)
            : this()
        {
            _asyncWriter = writer ?? throw new ArgumentNullException(nameof(writer));
            Headers.ContentEncoding.Add("gzip");
        }

        public BlittableJsonContent(Action<Stream> writer)
            : this()
        {
            _writer = writer ?? throw new ArgumentNullException(nameof(writer));
            Headers.ContentEncoding.Add("gzip");
        }

        private BlittableJsonContent()
        {
            Headers.ContentEncoding.Add("gzip");
        }

        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext context)
        {
            using (var gzipStream = new GZipStream(stream, CompressionMode.Compress, leaveOpen: true))
            {
                if (_asyncWriter != null)
                {
                    await _asyncWriter(gzipStream).ConfigureAwait(false);
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
