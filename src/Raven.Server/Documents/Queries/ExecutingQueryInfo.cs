using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Client.Documents.Queries;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public class ExecutingQueryInfo
    {
        public DateTime StartTime { get; }

        public string IndexName { get; set; }

        public IIndexQuery QueryInfo { get; }

        public long QueryId { get; }

        public bool IsStreaming { get; }

        public OperationCancelToken Token { get; }

        public long DurationInMs => _stopwatch.ElapsedMilliseconds;

        public TimeSpan Duration => _stopwatch.Elapsed;

        private readonly Stopwatch _stopwatch;

        public ExecutingQueryInfo(DateTime startTime, string indexName, IIndexQuery queryInfo, long queryId, bool isStreaming, OperationCancelToken token)
        {
            StartTime = startTime;
            IndexName = indexName;
            QueryInfo = queryInfo;
            QueryId = queryId;
            IsStreaming = isStreaming;
            _stopwatch = Stopwatch.StartNew();
            Token = token;
        }

        public async ValueTask Write(AsyncBlittableJsonTextWriter writer, JsonOperationContext context)
        {
            await writer.WriteStartObjectAsync();

            await writer.WritePropertyNameAsync(nameof(DurationInMs));
            await writer.WriteDoubleAsync(DurationInMs);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(Duration));
            await writer.WriteStringAsync(Duration.ToString());
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(IndexName));
            await writer.WriteStringAsync(IndexName);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(QueryId));
            await writer.WriteIntegerAsync(QueryId);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(StartTime));
            await writer.WriteDateTimeAsync(StartTime, isUtc: true);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(QueryInfo));
            await writer.WriteIndexQuery(context, QueryInfo);
            await writer.WriteCommaAsync();

            await writer.WritePropertyNameAsync(nameof(IsStreaming));
            await writer.WriteBoolAsync(IsStreaming);

            await writer.WriteEndObjectAsync();
        }
    }
}
