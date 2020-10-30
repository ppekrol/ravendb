using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands.MultiGet;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries;
using Raven.Client.Extensions;
using Sparrow.Json;

namespace Raven.Client.Documents.Session.Operations.Lazy
{
    internal class IndexQueryContent : GetRequest.IContent
    {
        private readonly DocumentConventions _conventions;
        private readonly IndexQuery _query;

        public IndexQueryContent(DocumentConventions conventions, IndexQuery query)
        {
            _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            _query = query ?? throw new ArgumentNullException(nameof(query));
        }

        public ValueTask WriteContentAsync(AsyncBlittableJsonTextWriter writer, JsonOperationContext context, CancellationToken token = default)
        {
            return writer.WriteIndexQueryAsync(_conventions, context, _query, token);
        }
    }
}
