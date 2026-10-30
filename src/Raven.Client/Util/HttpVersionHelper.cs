using System;

namespace Raven.Client.Util
{
    internal static class HttpVersionHelper
    {
        private static readonly Version Version20 = new Version(2, 0);

        /// <summary>
        /// WebSockets are not supported over HTTP/3 (RFC 9220) by the server, so HTTP/3 is downgraded to HTTP/2 for them.
        /// </summary>
        public static Version ForWebSocket(Version httpVersion)
        {
            if (httpVersion != null && httpVersion.Major >= 3)
                return Version20;

            return httpVersion;
        }
    }
}
