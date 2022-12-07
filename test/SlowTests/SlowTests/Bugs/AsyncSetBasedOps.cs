﻿using Tests.Infrastructure;
// -----------------------------------------------------------------------
//  <copyright file="AsyncSetBasedOps.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Server.Config;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.SlowTests.Bugs
{
    public class AsyncSetBasedOps : RavenTestBase
    {
        public AsyncSetBasedOps(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string FirstName;
#pragma warning disable 414,649
            public string LastName;
            public string FullName;
#pragma warning restore 414,649
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task AwaitAsyncPatchByIndexShouldWork(Options options)
        {
            options.ModifyDatabaseRecord += record => record.Settings[RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false";
            using (var store = GetDocumentStore(options))
            {
                string lastUserId = null;

                QueryStatistics stats;
                using (var session = store.OpenSession())
                {
                    session.Query<User>()
                        .Statistics(out stats)
                        .Where(x => x.FirstName == "John")
                        .ToList();
                }

                using (var bulkInsert = store.BulkInsert())
                {
                    for (int i = 0; i < 1000 * 10; i++)
                    {
                        lastUserId = await bulkInsert.StoreAsync(
                            new User
                            {
                                FirstName = "First #" + i,
                                LastName = "Last #" + i
                            }
                        );
                    }
                }

                Indexes.WaitForIndexing(store, timeout: TimeSpan.FromMinutes(5));

                await (await store.Operations.SendAsync(new PatchByQueryOperation(
                    new IndexQuery { Query = $"FROM INDEX '{stats.IndexName}' UPDATE {{ this.FullName = this.FirstName + ' ' + this.LastName; }}" }
                )))
                .WaitForCompletionAsync(TimeSpan.FromSeconds(60 * 5)); //TODO: EGOR it takes more than 1min on v8

                using (var db = store.OpenAsyncSession())
                {
                    var lastUser = await db.LoadAsync<User>(lastUserId);
                    Assert.NotNull(lastUser.FullName);
                }
            }
        }

    }
}
