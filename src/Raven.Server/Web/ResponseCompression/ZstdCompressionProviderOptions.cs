using Microsoft.Extensions.Options;

namespace Raven.Server.Web.ResponseCompression
{
    /// <summary>
    /// Options for the ZstdCompressionProvider
    /// </summary>
    public sealed class ZstdCompressionProviderOptions : IOptions<ZstdCompressionProviderOptions>
    {
        /// <inheritdoc />
        ZstdCompressionProviderOptions IOptions<ZstdCompressionProviderOptions>.Value => this;
    }
}
