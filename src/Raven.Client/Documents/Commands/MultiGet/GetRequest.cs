using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands.MultiGet
{
    public class GetRequest
    {
        /// <summary>
        /// Request url (relative).
        /// </summary>
        public string Url { get; set; }

        /// <summary>
        /// Request headers.
        /// </summary>
        public IDictionary<string, string> Headers { get; set; }

        /// <summary>
        /// Query information e.g. "?pageStart=10&amp;pageSize=20".
        /// </summary>
        public string Query { get; set; }

        public HttpMethod Method { get; set; }

        /// <summary>
        /// Concatenated Url and Query.
        /// </summary>
        public string UrlAndQuery
        {
            get
            {
                if (Query == null)
                    return Url;

                if (Query.StartsWith("?"))
                    return Url + Query;
                return Url + "?" + Query;
            }
        }

        public bool CanCacheAggressively { get; set; } = true;

        public IContent Content { get; set; }

        public GetRequest()
        {
            Headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        }

        public interface IContent
        {
            ValueTask WriteContentAsync(AsyncBlittableJsonTextWriter writer, JsonOperationContext context, CancellationToken token = default);
        }
    }
}
