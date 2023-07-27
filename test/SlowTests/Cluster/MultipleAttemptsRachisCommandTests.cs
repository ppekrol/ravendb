using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Serialization;
using Nito.AsyncEx;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Cluster;

public class MultipleAttemptsRachisCommandTests : ClusterTestBase
{
    public MultipleAttemptsRachisCommandTests(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task AddOrUpdateCompareExchangeCommand_WhenCommandSentTwice_SecondAttemptShouldNotReturnNull()
    {
        try
        {
            LoggingSource.Instance.SetupLogMode(LogMode.Information, "/home/haludi/work/ravendb/RavenDB-20762/logs", null, null, false);

            var customSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)] = $"{10000}",
                [RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)] = $"{10000}"
            };
            var (nodes, leader) = await CreateRaftCluster(2, watcherCluster: true, customSettings: customSettings);
            using var store = GetDocumentStore(new Options {Server = leader});

            OpenBrowser($"{leader.WebUrl}/studio/index.html#admin/settings/cluster");
            Console.WriteLine();
            var longCommandTasks = Enumerable.Range(0, 5000).Select(i => Task.Run(async () =>
                // var longCommandTasks =  Enumerable.Range(0, 5 * 1024).Select(i => Task.Run(async () =>
            {
                string uniqueRequestId = RaftIdGenerator.NewId();
                string mykey = $"mykey{i}";
                var timeoutAttemptTask = Task.Run(async () =>
                {
                    try
                    {
                        using (leader.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                        {
                            var value = context.ReadObject(new DynamicJsonValue {[$"prop{i}"] = "my value"}, "compare exchange");
                            var toRunTwiceCommand1 = new AddOrUpdateCompareExchangeCommand(store.Database, mykey, value, 0, uniqueRequestId);
                            toRunTwiceCommand1.Timeout = TimeSpan.FromSeconds(1);
                            await leader.ServerStore.Engine.CurrentLeader.PutAsync(toRunTwiceCommand1, toRunTwiceCommand1.Timeout.Value);
                        }
                    }
                    catch (TimeoutException)
                    {
                        // ignored
                    }
                });

                await Task.Delay(1);

                using (leader.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    // using JsonOperationContext context2 = JsonOperationContext.ShortTermSingleUse();
                    var value = context.ReadObject(new DynamicJsonValue {[$"prop{i}"] = "my value"}, "compare exchange");
                    var toRunTwiceCommand2 = new AddOrUpdateCompareExchangeCommand(store.Database, mykey, value, 0, uniqueRequestId);
                    // toRunTwiceCommand2.Context = context2;
                    toRunTwiceCommand2.Timeout = TimeSpan.FromSeconds(120);

                    // var serverStore = nodes[i % 2].ServerStore;
                    var serverStore = leader.ServerStore;
                    using var dis = serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context2);
                    var (_, result) = await serverStore.SendToLeaderAsync(context2, toRunTwiceCommand2);
                    // var (_, result) = await leader.ServerStore.Engine.CurrentLeader.PutAsync(toRunTwiceCommand2, toRunTwiceCommand2.Timeout.Value);
                    Assert.NotNull(result);
                    using var contextResult = (ContextResult)result;
                    var compareExchangeResult = (CompareExchangeCommandBase.CompareExchangeResult)contextResult.Result;

                    Assert.Equal(value, compareExchangeResult.Value);

                    await timeoutAttemptTask;
                }

            })).ToArray();

            await Task.WhenAll(longCommandTasks);
        }
        finally
        {
            Console.WriteLine();
        }
    }

    [Fact]
    public async Task TestCase()
    {
        var ravenServer = Server;

        var tasks = Enumerable.Range(0, 5000).Select(i => Task.Run(async () =>
        {
            Console.WriteLine($"allocate {i}");
            return ravenServer.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context);
        })).ToArray();
        
        await Task.WhenAll(tasks);
        GC.Collect();
        GC.WaitForPendingFinalizers();
        var j = 0;
        var tasks2 = tasks.Select(t => Task.Run(async () =>
        {
            Console.WriteLine($"Free {Interlocked.Increment(ref j)}");
            var dis = await t;
            dis.Dispose();
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }));
        
        await Task.WhenAll(tasks2);
        
        var tasks3 = Enumerable.Range(0, 5000).Select(i  =>
        {
            Console.WriteLine($"allocate {i}");
            IDisposable allocateOperationContext = ravenServer.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context);
            Assert.False(context._arenaAllocator._isDisposed);
            return allocateOperationContext;
        }).ToArray();
        foreach (IDisposable VARIABLE in tasks3)
        {
            VARIABLE.Dispose();
        }
    }

}
