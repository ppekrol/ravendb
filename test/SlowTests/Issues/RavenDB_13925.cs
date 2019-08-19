using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Raven.Server.Config;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_13925 : RavenTestBase
    {
        [Fact]
        public void CanUse_ThrowIfProjectedFieldCannotBeExtractedFromIndex_Configuration()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.ThrowIfProjectedFieldCannotBeExtractedFromIndex)] = "true"
            }))
            {
                new Products_Stored().Execute(store);
                new Products_NotStored().Execute(store);
                new Products_Mixed().Execute(store);
                new Products_NotStored_Configuration().Execute(store);
                new Products_AllStored().Execute(store);

                // dynamic and collection queries
                using (var session = store.OpenSession())
                {
                    // should not throw
                    var products = session.Query<Product>()
                        .ToList();

                    // should not throw
                    products = session.Query<Product>()
                        .OrderBy(x => x.Name)
                        .ToList();

                    // should not throw
                    var productNames = session.Query<Product>()
                        .OrderBy(x => x.Name)
                        .Select(x => x.Name)
                        .ToList();
                }

                WaitForIndexing(store);

                // static queries
                using (var session = store.OpenSession())
                {
                    var products = session.Query<Product, Products_Stored>()
                        .ToList();

                    var productNames = session.Query<Product, Products_Stored>()
                        .Select(x => x.Name)
                        .ToList();

                    var productCategories = session.Query<Product, Products_Stored>()
                        .Select(x => x.Category)
                        .ToList();

                    Assert.Throws<InvalidQueryException>(() =>
                    {
                        var productPricePerUnits = session.Query<Product, Products_Stored>()
                            .Select(x => x.PricePerUnit)
                            .ToList();
                    });

                    var productNamesAndCategories = session.Query<Product, Products_Stored>()
                        .Select(x => new { x.Name, x.Category })
                        .ToList();

                    Assert.Throws<InvalidQueryException>(() =>
                    {
                        var productNamesAndCategoriesAndOther = session.Query<Product, Products_Stored>()
                            .Select(x => new { x.Name, x.Category, x.PricePerUnit })
                            .ToList();
                    });
                }

                // static queries
                using (var session = store.OpenSession())
                {
                    var products = session.Query<Product, Products_NotStored>()
                        .ToList();

                    Assert.Throws<InvalidQueryException>(() =>
                    {
                        var productNames = session.Query<Product, Products_NotStored>()
                            .Select(x => x.Name)
                            .ToList();
                    });

                    Assert.Throws<InvalidQueryException>(() =>
                    {
                        var productCategories = session.Query<Product, Products_NotStored>()
                            .Select(x => x.Category)
                            .ToList();
                    });

                    Assert.Throws<InvalidQueryException>(() =>
                    {
                        var productPricePerUnits = session.Query<Product, Products_NotStored>()
                            .Select(x => x.PricePerUnit)
                            .ToList();
                    });

                    Assert.Throws<InvalidQueryException>(() =>
                    {
                        var productNamesAndCategories = session.Query<Product, Products_NotStored>()
                            .Select(x => new { x.Name, x.Category })
                            .ToList();
                    });

                    Assert.Throws<InvalidQueryException>(() =>
                    {
                        var productNamesAndCategoriesAndOther = session.Query<Product, Products_NotStored>()
                            .Select(x => new { x.Name, x.Category, x.PricePerUnit })
                            .ToList();
                    });
                }

                // static queries
                using (var session = store.OpenSession())
                {
                    var products = session.Query<Product, Products_Mixed>()
                        .ToList();

                    Assert.Throws<InvalidQueryException>(() =>
                    {
                        var productNames = session.Query<Product, Products_Mixed>()
                            .Select(x => x.Category)
                            .ToList();
                    });

                    Assert.Throws<InvalidQueryException>(() =>
                    {
                        var productCategories = session.Query<Product, Products_Mixed>()
                            .Select(x => x.Category)
                            .ToList();
                    });

                    Assert.Throws<InvalidQueryException>(() =>
                    {
                        var productPricePerUnits = session.Query<Product, Products_Mixed>()
                            .Select(x => x.PricePerUnit)
                            .ToList();
                    });

                    Assert.Throws<InvalidQueryException>(() =>
                    {
                        var productNamesAndCategories = session.Query<Product, Products_Mixed>()
                            .Select(x => new { x.Name, x.Category })
                            .ToList();
                    });

                    Assert.Throws<InvalidQueryException>(() =>
                    {
                        var productNamesAndCategoriesAndOther = session.Query<Product, Products_Mixed>()
                            .Select(x => new { x.Name, x.Category, x.PricePerUnit })
                            .ToList();
                    });
                }

                // static queries
                using (var session = store.OpenSession())
                {
                    var products = session.Query<Product, Products_NotStored_Configuration>()
                        .ToList();

                    var productNames = session.Query<Product, Products_NotStored_Configuration>()
                        .Select(x => x.Category)
                        .ToList();

                    var productCategories = session.Query<Product, Products_NotStored_Configuration>()
                        .Select(x => x.Category)
                        .ToList();

                    var productPricePerUnits = session.Query<Product, Products_NotStored_Configuration>()
                        .Select(x => x.PricePerUnit)
                        .ToList();

                    var productNamesAndCategories = session.Query<Product, Products_NotStored_Configuration>()
                        .Select(x => new { x.Name, x.Category })
                        .ToList();

                    var productNamesAndCategoriesAndOther = session.Query<Product, Products_NotStored_Configuration>()
                        .Select(x => new { x.Name, x.Category, x.PricePerUnit })
                        .ToList();
                }

                // static queries
                using (var session = store.OpenSession())
                {
                    var products = session.Query<Product, Products_AllStored>()
                        .ToList();

                    var productNames = session.Query<Product, Products_AllStored>()
                        .Select(x => x.Name)
                        .ToList();

                    var productCategories = session.Query<Product, Products_AllStored>()
                        .Select(x => x.Category)
                        .ToList();

                    Assert.Throws<InvalidQueryException>(() =>
                    {
                        var productPricePerUnits = session.Query<Product, Products_AllStored>()
                            .Select(x => x.PricePerUnit)
                            .ToList();
                    });

                    var productNamesAndCategories = session.Query<Product, Products_AllStored>()
                        .Select(x => new { x.Name, x.Category })
                        .ToList();

                    Assert.Throws<InvalidQueryException>(() =>
                    {
                        var productNamesAndCategoriesAndOther = session.Query<Product, Products_AllStored>()
                            .Select(x => new { x.Name, x.Category, x.PricePerUnit })
                            .ToList();
                    });
                }
            }
        }

        private class Products_Stored : AbstractIndexCreationTask<Product>
        {
            public Products_Stored()
            {
                Map = products => from p in products
                                  select new
                                  {
                                      Name = p.Name,
                                      Category = p.Category
                                  };

                Store(x => x.Name, FieldStorage.Yes);
                Store(x => x.Category, FieldStorage.Yes);
            }
        }

        private class Products_NotStored_Configuration : AbstractIndexCreationTask<Product>
        {
            public Products_NotStored_Configuration()
            {
                Map = products => from p in products
                                  select new
                                  {
                                      Name = p.Name,
                                      Category = p.Category
                                  };

                Store(x => x.Name, FieldStorage.No);
                Store(x => x.Category, FieldStorage.No);

                Configuration[RavenConfiguration.GetKey(x => x.Indexing.ThrowIfProjectedFieldCannotBeExtractedFromIndex)] = "false";
            }
        }

        private class Products_NotStored : AbstractIndexCreationTask<Product>
        {
            public Products_NotStored()
            {
                Map = products => from p in products
                                  select new
                                  {
                                      Name = p.Name,
                                      Category = p.Category
                                  };

                Store(x => x.Name, FieldStorage.No);
                Store(x => x.Category, FieldStorage.No);
            }
        }

        private class Products_Mixed : AbstractIndexCreationTask<Product>
        {
            public Products_Mixed()
            {
                Map = products => from p in products
                                  select new
                                  {
                                      Name = p.Name,
                                      Category = p.Category
                                  };

                Store(x => x.Name, FieldStorage.Yes);
                Store(x => x.Category, FieldStorage.No);
            }
        }

        private class Products_AllStored : AbstractIndexCreationTask<Product>
        {
            public Products_AllStored()
            {
                Map = products => from p in products
                    select new
                    {
                        Name = p.Name,
                        Category = p.Category
                    };

                StoreAllFields(FieldStorage.Yes);
            }
        }
    }
}
