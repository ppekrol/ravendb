﻿using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Orders;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16293 : RavenTestBase
    {
        public RavenDB_16293(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanUpdateIndexWithAdditionalSources()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "HR"
                    });

                    session.SaveChanges();
                }

                new Companies_ByName_Without().Execute(store);

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                using (var session = store.OpenSession())
                {
                    var companies = session
                        .Query<Company, Companies_ByName_Without>()
                        .Statistics(out var stats)
                        .Where(x => x.Name == "HR")
                        .ToList();

                    Assert.Equal(1, companies.Count);
                    Assert.True(stats.DurationInMs >= 0, $"{stats.DurationInMs} >= 0");
                }

                new Companies_ByName_With().Execute(store);

                Indexes.WaitForIndexing(store, allowErrors: true);

                RavenTestHelper.AssertNoIndexErrors(store);

                using (var session = store.OpenSession())
                {
                    var companies = session
                        .Query<Company, Companies_ByName_With>()
                        .Statistics(out var stats)
                        .Where(x => x.Name == "HR")
                        .ToList();

                    Assert.Equal(1, companies.Count);
                    Assert.True(stats.DurationInMs >= 0, $"{stats.DurationInMs} >= 0");
                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanUpdateIndexWithAdditionalSources_JavaScript(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "HR"
                    });

                    session.SaveChanges();
                }

                new Companies_ByName_Without_JavaScript().Execute(store);

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                using (var session = store.OpenSession())
                {
                    var companies = session
                        .Query<Company, Companies_ByName_Without_JavaScript>()
                        .Statistics(out var stats)
                        .Where(x => x.Name == "HR")
                        .ToList();

                    Assert.Equal(1, companies.Count);
                    Assert.True(stats.DurationInMs >= 0, $"{stats.DurationInMs} >= 0");
                }

                new Companies_ByName_With_JavaScript().Execute(store);

                Indexes.WaitForIndexing(store, allowErrors: true);

                RavenTestHelper.AssertNoIndexErrors(store);

                using (var session = store.OpenSession())
                {
                    var companies = session
                        .Query<Company, Companies_ByName_With_JavaScript>()
                        .Statistics(out var stats)
                        .Where(x => x.Name == "HR")
                        .ToList();

                    Assert.Equal(1, companies.Count);
                    Assert.True(stats.DurationInMs >= 0, $"{stats.DurationInMs} >= 0");
                }
            }
        }

        private class Companies_ByName_Without : AbstractIndexCreationTask<Company>
        {
            public override string IndexName => "Companies/ByName";

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps = { "from company in docs.Companies select new { Name = Helper.GetName(company.Name) }" },
                    AdditionalSources = new System.Collections.Generic.Dictionary<string, string>
                    {
                        {
                            "Helper",
                            @"
            public static class Helper
            {
                public static string GetName(string name)
                {
                    return ""HR"";
                }
            }
"
                        }
                    }
                };
            }
        }

        private class Companies_ByName_With : AbstractIndexCreationTask<Company>
        {
            public override string IndexName => "Companies/ByName";

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps = { "from company in docs.Companies select new { Name = Helper.GetName(company.Name) }" },
                    AdditionalSources = new System.Collections.Generic.Dictionary<string, string>
                    {
                        {
                            "Helper",
                            @"
            public static class Helper
            {
                public static string GetName(string name)
                {
                    return name;
                }
            }
"
                        }
                    }
                };
            }
        }

        private class Companies_ByName_Without_JavaScript : AbstractJavaScriptIndexCreationTask
        {
            public override string IndexName => "Companies/ByName";

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps = { @"map('Companies', c => ({ Name: getName(c.Name) }))" },
                    AdditionalSources = new System.Collections.Generic.Dictionary<string, string>
                    {
                        {
                            "Helper",
                            @"
            function getName(name)
            {
                return 'HR';
            }
"
                        }
                    }
                };
            }
        }

        private class Companies_ByName_With_JavaScript : AbstractJavaScriptIndexCreationTask
        {
            public override string IndexName => "Companies/ByName";

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps = { @"map('Companies', c => ({ Name: getName(c.Name)}))" },
                    AdditionalSources = new System.Collections.Generic.Dictionary<string, string>
                    {
                        {
                            "Helper",
                            @"
            function getName(name)
            {
                return name;
            }
"
                        }
                    }
                };
            }
        }
    }
}
