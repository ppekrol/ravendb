﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3.Model;
using FastTests;
using Microsoft.Azure.Documents.Spatial;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using SlowTests.MailingList;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17650 : ClusterTestBase
    {
        public RavenDB_17650(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Should_Retry_When_DatabaseDisabledException_Was_Thrown()
        {
            using var store = GetDocumentStore(new Options()
            {
                ReplicationFactor = 1,
                RunInMemory = false
            });
            
            string id = "User/33-A";
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Id = id, Name = "1" });
                await session.StoreAsync(new User { Name = "2" });
                await session.SaveChangesAsync();
            }

            await store.Subscriptions
                .CreateAsync(new SubscriptionCreationOptions<User>
                {
                    Name = "BackgroundSubscriptionWorker"
                });
            
            using var worker = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions("BackgroundSubscriptionWorker"));

            // disable database
            var disableSucceeded = store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, true));
            Assert.True(disableSucceeded.Success);
            Assert.True(disableSucceeded.Disabled);

            var cts = new CancellationTokenSource();
            var failMre = new ManualResetEvent(false);
            worker.OnSubscriptionConnectionRetry += _ =>
            {
                failMre.Set();
            };
            var successMre = new ManualResetEvent(false);
            var _ = worker.Run( batch =>
            {
                successMre.Set();
            }, cts.Token);

            //enable database
            Assert.True(failMre.WaitOne(TimeSpan.FromSeconds(15)), "Subscription didn't fail as expected.");
            var enableSucceeded = store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, false));
            Assert.False(enableSucceeded.Disabled);
            Assert.True(enableSucceeded.Success);
            Assert.True(successMre.WaitOne(TimeSpan.FromSeconds(15)), "Subscription didn't success as expected.");
        }

        [Fact]
        public async Task Should_Retry_When_AllNodesTopologyDownException_Was_Thrown()
        {
            var node = GetNewServer(new ServerCreationOptions { RunInMemory = false });
            using var store = GetDocumentStore(new Options()
            {
                ReplicationFactor = 1,
                RunInMemory = false,
                Server = node
            });
            string id = "User/33-A";
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Id = id, Name = "1" });
                await session.StoreAsync(new User { Name = "2" });
                await session.SaveChangesAsync();
            }
            WaitForUserToContinueTheTest(store);

            await store.Subscriptions
                .CreateAsync(new SubscriptionCreationOptions<User>
                {
                    Name = "BackgroundSubscriptionWorker"
                });

            using var worker = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions("BackgroundSubscriptionWorker"));

            // dispose nodes
            var result = await DisposeServerAndWaitForFinishOfDisposalAsync(node);

            var cts = new CancellationTokenSource();
            var failMre = new ManualResetEvent(false);
            worker.OnSubscriptionConnectionRetry += _ =>
            {
                failMre.Set();
            };
            var successMre = new ManualResetEvent(false);
            var _ = worker.Run( batch =>
            {
                successMre.Set();
            }, cts.Token);

            //revive node
            Assert.True(failMre.WaitOne(TimeSpan.FromSeconds(15)), "Subscription didn't fail as expected.");
            var cs = new Dictionary<string, string>(DefaultClusterSettings);
            cs[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = result.Url;
            var revivedServer = GetNewServer(new ServerCreationOptions
            {
                DeletePrevious = false,
                RunInMemory = false,
                DataDirectory = result.DataDirectory,
                CustomSettings = cs
            });
            Assert.True(successMre.WaitOne(TimeSpan.FromSeconds(15)), "Subscription didn't success as expected.");
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }

        }
    }
}
