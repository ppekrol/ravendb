using System;
using System.Diagnostics;
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

        public void Write(AsyncBlittableJsonTextWriter writer, JsonOperationContext context)
        {
            writer.WriteStartObjectAsync();

            writer.WritePropertyNameAsync(nameof(DurationInMs));
            writer.WriteDoubleAsync(DurationInMs);
            writer.WriteCommaAsync();
            
            writer.WritePropertyNameAsync(nameof(Duration));
            writer.WriteStringAsync(Duration.ToString());
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(IndexName));
            writer.WriteStringAsync(IndexName);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(QueryId));
            writer.WriteIntegerAsync(QueryId);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(StartTime));
            writer.WriteDateTimeAsync(StartTime, isUtc: true);
            writer.WriteCommaAsync();

            writer.WritePropertyNameAsync(nameof(QueryInfo));
            writer.WriteIndexQuery(context, QueryInfo);
            writer.WriteCommaAsync();
            
            writer.WritePropertyNameAsync(nameof(IsStreaming));
            writer.WriteBoolAsync(IsStreaming);

            writer.WriteEndObjectAsync();
        }
    }
}
