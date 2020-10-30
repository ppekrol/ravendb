using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries;
using Sparrow.Json;

namespace Raven.Client.Extensions
{
    internal static class BlittableJsonTextWriterExtensions
    {
        public static async ValueTask WriteIndexQueryAsync(this AsyncBlittableJsonTextWriter writer, DocumentConventions conventions, JsonOperationContext context, IndexQuery query, CancellationToken token = default)
        {
            await writer.WriteStartObjectAsync().ConfigureAwait(false);

            await writer.WritePropertyNameAsync(nameof(query.Query)).ConfigureAwait(false);
            await writer.WriteStringAsync(query.Query).ConfigureAwait(false);
            await writer.WriteCommaAsync().ConfigureAwait(false);

#pragma warning disable 618
            if (query.PageSizeSet && query.PageSize >= 0)
            {
                await writer.WritePropertyNameAsync(nameof(query.PageSize)).ConfigureAwait(false);
                await writer.WriteIntegerAsync(query.PageSize).ConfigureAwait(false);
                await writer.WriteCommaAsync().ConfigureAwait(false);
            }
#pragma warning restore 618

            if (query.WaitForNonStaleResults)
            {
                await writer.WritePropertyNameAsync(nameof(query.WaitForNonStaleResults)).ConfigureAwait(false);
                await writer.WriteBoolAsync(query.WaitForNonStaleResults).ConfigureAwait(false);
                await writer.WriteCommaAsync().ConfigureAwait(false);
            }

#pragma warning disable 618
            if (query.Start > 0)
            {
                await writer.WritePropertyNameAsync(nameof(query.Start)).ConfigureAwait(false);
                await writer.WriteIntegerAsync(query.Start).ConfigureAwait(false);
                await writer.WriteCommaAsync().ConfigureAwait(false);
            }
#pragma warning restore 618

            if (query.WaitForNonStaleResultsTimeout.HasValue)
            {
                await writer.WritePropertyNameAsync(nameof(query.WaitForNonStaleResultsTimeout)).ConfigureAwait(false);
                await writer.WriteStringAsync(query.WaitForNonStaleResultsTimeout.Value.ToInvariantString()).ConfigureAwait(false);
                await writer.WriteCommaAsync().ConfigureAwait(false);
            }

            if (query.DisableCaching)
            {
                await writer.WritePropertyNameAsync(nameof(query.DisableCaching)).ConfigureAwait(false);
                await writer.WriteBoolAsync(query.DisableCaching).ConfigureAwait(false);
                await writer.WriteCommaAsync().ConfigureAwait(false);
            }

            if (query.SkipDuplicateChecking)
            {
                await writer.WritePropertyNameAsync(nameof(query.SkipDuplicateChecking)).ConfigureAwait(false);
                await writer.WriteBoolAsync(query.SkipDuplicateChecking).ConfigureAwait(false);
                await writer.WriteCommaAsync().ConfigureAwait(false);
            }

            await writer.WritePropertyNameAsync(nameof(query.QueryParameters)).ConfigureAwait(false);
            if (query.QueryParameters != null)
                await writer.WriteObjectAsync(conventions.Serialization.DefaultConverter.ToBlittable(query.QueryParameters, context)).ConfigureAwait(false);
            else
                await writer.WriteNullAsync().ConfigureAwait(false);

            await writer.WriteEndObjectAsync().ConfigureAwait(false);
        }
    }
}
