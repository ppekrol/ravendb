using System;
using System.IO;
using System.IO.Compression;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Utils;

namespace Raven.Client.Http;

public enum HttpCompressionAlgorithm
{
    Gzip,
#if FEATURE_BROTLI_SUPPORT
    Brotli,
#endif
#if FEATURE_ZSTD_SUPPORT
    Zstd
#endif
}

internal static class HttpCompressionAlgorithmExtensions
{
    internal static string GetContentEncoding(this HttpCompressionAlgorithm compressionAlgorithm)
    {
        switch (compressionAlgorithm)
        {
            case HttpCompressionAlgorithm.Gzip:
                return Constants.Headers.Encodings.Gzip;
#if FEATURE_BROTLI_SUPPORT
            case HttpCompressionAlgorithm.Brotli:
                return Constants.Headers.Encodings.Brotli;
#endif
#if FEATURE_ZSTD_SUPPORT
            case HttpCompressionAlgorithm.Zstd:
                return Constants.Headers.Encodings.Zstd;
#endif
            default:
                throw new ArgumentOutOfRangeException(nameof(compressionAlgorithm), compressionAlgorithm, null);
        }
    }

    internal static async Task<Stream> ReadAsStreamWithZstdSupportAsync(this HttpResponseMessage response)
    {
        var contentStream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
        var contentStreamType = contentStream.GetType();
#if FEATURE_BROTLI_SUPPORT
        if (contentStreamType == typeof(BrotliStream))
            return contentStream;
#endif
        if (contentStreamType == typeof(GZipStream))
            return contentStream;

#if !FEATURE_ZSTD_SUPPORT
        return contentStream;
#else
        if (response.Content.Headers.TryGetValues(Constants.Headers.ContentEncoding, out var values) == false)
            return contentStream;

        foreach (var value in values)
        {
            if (value == Constants.Headers.Encodings.Zstd)
                return ZstdStream.Decompress(contentStream);
        }

        return contentStream;
#endif
    }
}
