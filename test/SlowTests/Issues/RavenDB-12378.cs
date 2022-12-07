﻿using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_12378 : RavenTestBase
    {
        public RavenDB_12378(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanUseQueryParametersInDeclareFunction(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new Order
                    {
                        Company = "Companies/1"
                    });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var result = s.Advanced
                        .RawQuery<Order>(
                            @"declare function project(o) {
                                o.Company = $key + '/new';
                                return o;
                            }
                            from Orders as entry
                            where entry.Company in ($key) 
                            select project(entry)")
                        .AddParameter("key", "Companies/1")
                        .ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal("Companies/1/new", result[0].Company);
                }
            }
        }
    }
}
