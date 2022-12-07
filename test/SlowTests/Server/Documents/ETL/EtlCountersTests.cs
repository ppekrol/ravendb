using Tests.Infrastructure;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Config;
using Raven.Server.ServerWide.Context;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL
{
    public class EtlCountersTests : EtlTestBase
    {
        private const int _waitInterval = 1000;

        private readonly Options _options = Debugger.IsAttached
            ? new Options {ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Etl.ExtractAndTransformTimeout)] = "300"}
            : null;
        
        public EtlCountersTests(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(16, JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        [RavenData(1024, JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task EtlCounter_WhenUseAddCountersAndRemoveCounterFromSrc_ShouldRemoveTheCounterFromDest(Options options, int count)
        {
            var optChaining = options.JavascriptEngineMode.ToString() == "Jint" ? "" : "?";
            var zeroIfNull = options.JavascriptEngineMode.ToString() == "Jint" ? "" : " ?? 0";
            string script = @$"
var doc = loadToUsers(this);
var counters = this['@metadata']['@counters'];
for (var i = 0; i < counters{optChaining}.length{zeroIfNull}; i++) {{
    doc.addCounter(loadCounter(counters[i]));
}}";

            // TODO: ego dispose the document stores ??
            var (src, dest, _) = CreateSrcDestAndAddEtl("Users", script, srcOptions:_options);

            var entity = new User();
            var counters = Enumerable.Range(0, count).Select(i => "Likes" + i).ToArray();
                
            using (var session = src.OpenAsyncSession())
            {
                await session.StoreAsync(entity);

                foreach (var counter in counters)
                {
                    session.CountersFor(entity.Id).Increment(counter);
                }
                await session.SaveChangesAsync();
            }
            await AssertWaitForTrueAsync(async () =>
            {
                using var session = dest.OpenAsyncSession();
                var all = (await session.CountersFor(entity.Id).GetAllAsync()).Keys.ToArray();
                return all.Except(counters).Any() == false;
            }, interval:_waitInterval);

            using (var session = src.OpenAsyncSession())
            {
                foreach (var counter in counters)
                {
                    session.CountersFor(entity.Id).Delete(counter);
                }
                await session.SaveChangesAsync();
            }
            await AssertWaitForTrueAsync(async () =>
            {
                using var session = dest.OpenAsyncSession();
                return (await session.CountersFor(entity.Id).GetAllAsync()).Any() == false;
            }, interval:_waitInterval);
        }


        [Theory]
        [RavenData(16, JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        [RavenData(1024, JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task EtlCounter_WhenUseAddCountersAndRemoveCounterFromSrc_ShouldRemoveTheCounterFromDest2(Options options, int count)
        {
            var optChaining = options.JavascriptEngineMode.ToString() == "Jint" ? "" : "?";
            var zeroIfNull = options.JavascriptEngineMode.ToString() == "Jint" ? "" : " ?? 0";
            string script = @$"
var doc = loadToUsers(this);
var counters = this['@metadata']['@counters'];
for (var i = 0; i < counters{optChaining}.length{zeroIfNull}; i++) {{
    doc.addCounter(loadCounter(counters[i]));
}}";
            var (src, dest, _) = CreateSrcDestAndAddEtl("Users", script, srcOptions: _options);

            var entity = new User();
            var counters = Enumerable.Range(0, count).Select(i => "Likes" + i).ToArray();

            using (var session = src.OpenAsyncSession())
            {
                await session.StoreAsync(entity);

                foreach (var counter in counters)
                {
                    session.CountersFor(entity.Id).Increment(counter);
                }
                await session.SaveChangesAsync();
            }
            await AssertWaitForTrueAsync(async () =>
            {
                using var session = dest.OpenAsyncSession();
                var all = (await session.CountersFor(entity.Id).GetAllAsync()).Keys.ToArray();
                return all.Except(counters).Any() == false;
            }, interval: _waitInterval);

            using (var session = src.OpenAsyncSession())
            {
                foreach (var counter in counters)
                {
                    session.CountersFor(entity.Id).Delete(counter);
                }
                await session.SaveChangesAsync();
            }

            var srcDatabase = await GetDatabase(src.Database);
            using (srcDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                await AssertWaitForTrueAsync(async () =>
                {
                    using var session = dest.OpenAsyncSession();
                    return (await session.CountersFor(entity.Id).GetAllAsync()).Any() == false;
                }, interval: _waitInterval);

                await AssertWaitForTrueAsync(async () =>
                {
                    await srcDatabase.TombstoneCleaner.ExecuteCleanup();
                    using var readTransaction = context.OpenReadTransaction();
                    var numOfCounters = srcDatabase.DocumentsStorage.CountersStorage.GetNumberOfCountersAndDeletedCountersForDocument(context, entity.Id);
                    return numOfCounters == 0;
                }, interval: _waitInterval);
            }
        }
    }
}
