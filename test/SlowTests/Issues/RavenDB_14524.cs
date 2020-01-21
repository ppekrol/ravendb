using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14524 : RavenTestBase
    {
        public RavenDB_14524(ITestOutputHelper output) : base(output)
        {
        }

        private class ByNameIndex : AbstractIndexCreationTask<TestDocument, ByNameIndex.Result>
        {
            public class Result
            {
                public string Name { get; set; }
                public int Order { get; set; } // NOT WORKING
                //public int MyOrder { get; set; } // WORKING
            }

            public ByNameIndex()
            {
                Map = documents => from document in documents
                                   select new
                                   {
                                       Name = document.Name,
                                       Order = document.Order // NOT WORKING
                                                              // MyOrder = document.Order // WORKING
                                   };

                StoreAllFields(FieldStorage.Yes);
            }
        }

        private class TestDocument
        {
            public string Name { get; set; }
            public int Order { get; set; }
        }

        [Fact]
        public void ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new ByNameIndex());

                using (var session = store.OpenSession())
                {
                    session.Store(new TestDocument
                    {
                        Name = "Hello world!",
                        Order = 1
                    });
                    session.Store(new TestDocument
                    {
                        Name = "Goodbye...",
                        Order = 2
                    });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var documents = session
                        .Query<ByNameIndex.Result, ByNameIndex>()
                        .ProjectInto<ByNameIndex.Result>()
                        .ToList();

                    Assert.Equal(2, documents.Count);
                }
            }
        }
    }
}
