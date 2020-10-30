using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.ServerWide;
using Raven.Server.Utils.Stats;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public class LiveRunningQueriesCollector : LivePerformanceCollector<LiveRunningQueriesCollector.ExecutingQueryCollection>
    {
        private readonly ServerStore _serverStore;
        private readonly HashSet<string> _dbNames;

        public LiveRunningQueriesCollector(ServerStore serverStore, HashSet<string> dbNames)
            : base(serverStore.ServerShutdown, "Server")
        {
            _dbNames = dbNames;
            _serverStore = serverStore;

            Start();
        }

        protected override TimeSpan SleepTime => TimeSpan.FromSeconds(1);

        protected override bool ShouldEnqueue(List<ExecutingQueryCollection> items)
        {
            // always enqueue new message
            return true;
        }

        protected override async Task StartCollectingStats()
        {
            var stats = PreparePerformanceStats();
            Stats.Enqueue(stats);

            await RunInLoop();
        }

        protected override List<ExecutingQueryCollection> PreparePerformanceStats()
        {
            var result = new List<ExecutingQueryCollection>();

            foreach ((var dbName, Task<DocumentDatabase> value) in _serverStore.DatabasesLandlord.DatabasesCache)
            {
                if (value.IsCompletedSuccessfully == false)
                    continue;

                var dbNameAsString = dbName.ToString();

                if (_dbNames != null && !_dbNames.Contains(dbNameAsString))
                    continue;

                var database = value.Result;

                foreach (var group in database.QueryRunner.CurrentlyRunningQueries
                    .Where(x => x.DurationInMs > 100)
                    .GroupBy(x => x.IndexName))
                {
                    result.Add(new ExecutingQueryCollection
                    {
                        DatabaseName = dbNameAsString,
                        IndexName = group.Key,
                        RunningQueries = group.ToList()
                    });
                }
            }

            return result;
        }

        protected override void WriteStats(List<ExecutingQueryCollection> stats, AsyncBlittableJsonTextWriter writer, JsonOperationContext context)
        {
            writer.WriteStartArrayAsync();

            var isFirst = true;

            foreach (var executingQueryCollection in stats)
            {
                if (isFirst == false)
                {
                    writer.WriteCommaAsync();
                }

                writer.WriteStartObjectAsync();

                isFirst = false;
                writer.WritePropertyNameAsync(nameof(executingQueryCollection.DatabaseName));
                writer.WriteStringAsync(executingQueryCollection.DatabaseName);
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(executingQueryCollection.IndexName));
                writer.WriteStringAsync(executingQueryCollection.IndexName);
                writer.WriteCommaAsync();

                writer.WritePropertyNameAsync(nameof(executingQueryCollection.RunningQueries));
                writer.WriteStartArrayAsync();

                var firstInnerQuery = true;
                foreach (var executingQueryInfo in executingQueryCollection.RunningQueries)
                {
                    if (firstInnerQuery == false)
                        writer.WriteCommaAsync();

                    firstInnerQuery = false;
                    executingQueryInfo.Write(writer, context);
                }
                writer.WriteEndArrayAsync();

                writer.WriteEndObjectAsync();

            }

            writer.WriteEndArrayAsync();
        }

        public class ExecutingQueryCollection
        {
            public string DatabaseName { get; set; }
            public string IndexName { get; set; }
            public List<ExecutingQueryInfo> RunningQueries { get; set; }
        }

    }
}
