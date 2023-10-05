﻿using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Session;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client
{
    public class CRUD : RavenTestBase
    {
        public CRUD(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(true, DatabaseMode = RavenDatabaseMode.Single)]
        //[RavenData(false, DatabaseMode = RavenDatabaseMode.All)]
        public void CRUD_Operations(Options options, bool useCompression)
        {
            options.ModifyDocumentStore = x =>
            {
                x.Conventions.UseHttpCompression = useCompression;
                x.Conventions.UseHttpDecompression = useCompression;
            };
            using (var store = GetDocumentStore(options))
            {
                using (var newSession = store.OpenSession())
                {
                    newSession.Store(new User { Name = "user1" }, "users/1");
                    var user2 = new User {Name = "user2", Age = 1};
                    newSession.Store(user2, "users/2");
                    var user3 = new User { Name = "user3", Age = 1 };
                    newSession.Store(user3, "users/3");
                    newSession.Store(new User { Name = "user4" }, "users/4");
                    
                    newSession.Delete(user2);
                    user3.Age = 3;
                    newSession.SaveChanges();

                    var tempUser = newSession.Load<User>("users/2");
                    Assert.Null(tempUser);
                    tempUser = newSession.Load<User>("users/3");
                    Assert.Equal(tempUser.Age, 3);
                    var user1 = newSession.Load<User>("users/1");
                    var user4 = newSession.Load<User>("users/4");
                    
                    newSession.Delete(user4);
                    user1.Age = 10;
                    newSession.SaveChanges();

                    tempUser = newSession.Load<User>("users/4");
                    Assert.Null(tempUser);
                    tempUser = newSession.Load<User>("users/1");
                    Assert.Equal(tempUser.Age, 10);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CRUD_Operations_with_what_changed(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var newSession = store.OpenSession())
                {
                    newSession.Store(new User { Name = "user1" }, "users/1");
                    var user2 = new User { Name = "user2", Age = 1 };
                    newSession.Store(user2, "users/2");
                    var user3 = new User { Name = "user3", Age = 1 };
                    newSession.Store(user3, "users/3");
                    newSession.Store(new User { Name = "user4" }, "users/4");

                    newSession.Delete(user2);
                    user3.Age = 3;
                    
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 4);
                    newSession.SaveChanges();

                    var tempUser = newSession.Load<User>("users/2");
                    Assert.Null(tempUser);
                    tempUser = newSession.Load<User>("users/3");
                    Assert.Equal(tempUser.Age, 3);
                    var user1 = newSession.Load<User>("users/1");
                    var user4 = newSession.Load<User>("users/4");

                    newSession.Delete(user4);
                    user1.Age = 10;
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 2);
                    newSession.SaveChanges();

                    tempUser = newSession.Load<User>("users/4");
                    Assert.Null(tempUser);
                    tempUser = newSession.Load<User>("users/1");
                    Assert.Equal(tempUser.Age, 10);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CRUD_Operations_With_Array_In_Object(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var newSession = store.OpenSession())
                {
                    var family = new Family()
                    {
                        Names = new[] {"Hibernating Rhinos", "RavenDB"}
                    };
                    newSession.Store(family, "family/1");
                    newSession.SaveChanges();

                    var newFamily = newSession.Load<Family>("family/1");

                    newFamily.Names = new[] {"Toli", "Mitzi", "Boki"};
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 1);
                    newSession.SaveChanges();
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CRUD_Operations_With_Array_In_Object_2(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var newSession = store.OpenSession())
                {
                    var family = new Family()
                    {
                        Names = new[] { "Hibernating Rhinos", "RavenDB" }
                    };
                    newSession.Store(family, "family/1");
                    newSession.SaveChanges();

                    var newFamily = newSession.Load<Family>("family/1");
                    newFamily.Names = new[] {"Hibernating Rhinos", "RavenDB"};
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 0);
                    newFamily.Names = new[] { "RavenDB", "Hibernating Rhinos" };
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 1);
                    newSession.SaveChanges();
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CRUD_Operations_With_Array_In_Object_3(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var newSession = store.OpenSession())
                {
                    var family = new Family()
                    {
                        Names = new[] { "Hibernating Rhinos", "RavenDB" }
                    };
                    newSession.Store(family, "family/1");
                    newSession.SaveChanges();

                    var newFamily = newSession.Load<Family>("family/1");

                    newFamily.Names = new[] { "RavenDB" };
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 1);
                    newSession.SaveChanges();
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CRUD_Operations_With_Array_In_Object_4(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var newSession = store.OpenSession())
                {
                    var family = new Family()
                    {
                        Names = new[] { "Hibernating Rhinos", "RavenDB" }
                    };
                    newSession.Store(family, "family/1");
                    newSession.SaveChanges();

                    var newFamily = newSession.Load<Family>("family/1");

                    newFamily.Names = new[] { "RavenDB", "Hibernating Rhinos", "Toli", "Mitzi", "Boki" };
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 1);
                    newSession.SaveChanges();
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CRUD_Operations_With_Array_In_Object_6(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var newSession = store.OpenSession())
                {
                    var family = new Family()
                    {
                        Names = new[] { "Hibernating Rhinos", "RavenDB" }
                    };
                    newSession.Store(family, "family/1");
                    newSession.SaveChanges();

                    var newFamily = newSession.Load<Family>("family/1");

                    newFamily.Names = new[] { "RavenDB", "Toli" };
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 1);
                    newSession.SaveChanges();
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CRUD_Operations_With_Null(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var newSession = store.OpenSession())
                {
                    newSession.Store(new User { Name = null }, "users/1");
                    newSession.SaveChanges();
                    var user = newSession.Load<User>("users/1");
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 0);
                    user.Age = 3;
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 1);
                }
            }
        }

        public class Family
        {
            public string[] Names { get; set; }
        }

        public class FamilyMembers
        {
            public member[] Members { get; set; }
        }

#pragma warning disable CS8981 // The type name only contains lower-cased ascii characters. Such names may become reserved for the language.
        public class member
#pragma warning restore CS8981 // The type name only contains lower-cased ascii characters. Such names may become reserved for the language.
        {
            public string Name { get; set; }
            public int Age { get; set; }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CRUD_Operations_With_Array_of_objects(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var newSession = store.OpenSession())
                {
                    var family = new FamilyMembers()
                    {
                        Members = new [] {
                            new member()
                            {
                                Name = "Hibernating Rhinos",
                                Age = 8
                            },
                            new member()
                            {
                                Name = "RavenDB",
                                Age = 4
                            }
                        }
                    };
                    newSession.Store(family, "family/1");
                    newSession.SaveChanges();

                    var newFamily = newSession.Load<FamilyMembers>("family/1");
                    newFamily.Members = new[]
                    {
                        new member()
                        {
                            Name = "RavenDB",
                            Age = 4
                        },
                        new member()
                        {
                            Name = "Hibernating Rhinos",
                            Age = 8
                        }
                    };

                    var changes = newSession.Advanced.WhatChanged();

                    Assert.Equal(1 , changes.Count);
                    Assert.Equal(4 , changes["family/1"].Length);

                    Array.Sort(changes["family/1"], (x, y) => x.FieldFullName.CompareTo(y.FieldFullName));

                    Assert.Equal("Name", changes["family/1"][1].FieldName);
                    Assert.Equal(DocumentsChanges.ChangeType.FieldChanged, changes["family/1"][1].Change);
                    Assert.Equal("Hibernating Rhinos", changes["family/1"][1].FieldOldValue.ToString());
                    Assert.Equal("RavenDB", changes["family/1"][1].FieldNewValue.ToString());

                    Assert.Equal("Age", changes["family/1"][0].FieldName);
                    Assert.Equal(DocumentsChanges.ChangeType.FieldChanged, changes["family/1"][0].Change);
                    Assert.Equal(8L, changes["family/1"][0].FieldOldValue);
                    Assert.Equal(4L, changes["family/1"][0].FieldNewValue);

                    Assert.Equal("Name", changes["family/1"][3].FieldName);
                    Assert.Equal(DocumentsChanges.ChangeType.FieldChanged, changes["family/1"][3].Change);
                    Assert.Equal("RavenDB", changes["family/1"][3].FieldOldValue.ToString());
                    Assert.Equal("Hibernating Rhinos", changes["family/1"][3].FieldNewValue.ToString());

                    Assert.Equal("Age", changes["family/1"][2].FieldName);
                    Assert.Equal(DocumentsChanges.ChangeType.FieldChanged, changes["family/1"][2].Change);
                    Assert.Equal(4L, changes["family/1"][2].FieldOldValue);
                    Assert.Equal(8L, changes["family/1"][2].FieldNewValue);

                    newFamily.Members = new[]
                    {
                        new member()
                        {
                            Name = "Toli",
                            Age = 5
                        },
                        new member()
                        {
                            Name = "Boki",
                            Age = 15
                        }
                    };

                    changes = newSession.Advanced.WhatChanged();

                    Assert.Equal(1, changes.Count);
                    Assert.Equal(4, changes["family/1"].Length);

                    Array.Sort(changes["family/1"], (x, y) => x.FieldFullName.CompareTo(y.FieldFullName));

                    Assert.Equal("Name", changes["family/1"][1].FieldName);
                    Assert.Equal(DocumentsChanges.ChangeType.FieldChanged, changes["family/1"][1].Change);
                    Assert.Equal("Hibernating Rhinos", changes["family/1"][1].FieldOldValue.ToString());
                    Assert.Equal("Toli", changes["family/1"][1].FieldNewValue.ToString());

                    Assert.Equal("Age", changes["family/1"][0].FieldName);
                    Assert.Equal(DocumentsChanges.ChangeType.FieldChanged, changes["family/1"][0].Change);
                    Assert.Equal(8L, changes["family/1"][0].FieldOldValue);
                    Assert.Equal(5L, changes["family/1"][0].FieldNewValue);

                    Assert.Equal("Name", changes["family/1"][3].FieldName);
                    Assert.Equal(DocumentsChanges.ChangeType.FieldChanged, changes["family/1"][3].Change);
                    Assert.Equal("RavenDB", changes["family/1"][3].FieldOldValue.ToString());
                    Assert.Equal("Boki", changes["family/1"][3].FieldNewValue.ToString());

                    Assert.Equal("Age", changes["family/1"][2].FieldName);
                    Assert.Equal(DocumentsChanges.ChangeType.FieldChanged, changes["family/1"][2].Change);
                    Assert.Equal(4L, changes["family/1"][2].FieldOldValue);
                    Assert.Equal(15L, changes["family/1"][2].FieldNewValue);
                }
            }
        }

        public class Arr1
        {
            public string[] str { get; set; }
        }

        public class Arr2
        {
            public Arr1[] arr1 { get; set; }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CRUD_Operations_With_Array_of_Arrays(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var newSession = store.OpenSession())
                {
                    var arr = new Arr2()
                    {
                        arr1 = new Arr1[]
                        {
                            new Arr1()
                            {
                                str = new [] {"a", "b"}
                            },
                            new Arr1()
                            {
                                str = new [] {"c", "d"}
                            }
                        } 
                    };
                    newSession.Store(arr, "arr/1");
                    newSession.SaveChanges();

                    var newArr = newSession.Load<Arr2>("arr/1");
                    newArr.arr1 = new Arr1[]
                        {
                            new Arr1()
                            {
                                str = new [] {"d", "c"}
                            },
                            new Arr1()
                            {
                                str = new [] {"a", "b"}
                            }
                       };
                    var whatChanged = newSession.Advanced.WhatChanged();
                    Assert.Equal(1, whatChanged.Count);

                    var change = whatChanged["arr/1"];
                    Assert.Equal(4, change.Length);
                    Assert.Equal("a", change[0].FieldOldValue.ToString());
                    Assert.Equal("d", change[0].FieldNewValue.ToString());

                    Assert.Equal("b", change[1].FieldOldValue.ToString());
                    Assert.Equal("c", change[1].FieldNewValue.ToString());

                    Assert.Equal("c", change[2].FieldOldValue.ToString());
                    Assert.Equal("a", change[2].FieldNewValue.ToString());

                    Assert.Equal("d", change[3].FieldOldValue.ToString());
                    Assert.Equal("b", change[3].FieldNewValue.ToString());

                    newSession.SaveChanges();
                }

                using (var newSession = store.OpenSession())
                {
                    var newArr = newSession.Load<Arr2>("arr/1");
                    newArr.arr1 = new Arr1[]
                    {
                        new Arr1()
                        {
                            str = new [] {"q", "w"}
                        },
                        new Arr1()
                        {
                            str = new [] {"a", "b"}
                        }
                    };
                    var whatChanged = newSession.Advanced.WhatChanged();
                    Assert.Equal(whatChanged.Count, 1);

                    var change = whatChanged["arr/1"];
                    Assert.Equal(2, change.Length);
                    Assert.Equal("d", change[0].FieldOldValue.ToString());
                    Assert.Equal("q", change[0].FieldNewValue.ToString());

                    Assert.Equal("c", change[1].FieldOldValue.ToString());
                    Assert.Equal("w", change[1].FieldNewValue.ToString());
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CRUD_Can_Update_Property_To_Null(Options options)
        {
            //RavenDB-8345

            using (var store = GetDocumentStore(options))
            {
                using (var newSession = store.OpenSession())
                {
                    newSession.Store(new User { Name = "user1" }, "users/1");
                    newSession.SaveChanges();
                }

                using (var newSession = store.OpenSession())
                {
                    var user = newSession.Load<User>("users/1");
                    user.Name = null;
                    newSession.SaveChanges();
                }

                using (var newSession = store.OpenSession())
                {
                    var user = newSession.Load<User>("users/1");
                    Assert.Null(user.Name);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CRUD_Can_Update_Property_From_Null_To_Object(Options options)
        {
            //RavenDB-8345

            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Poc
                    {
                        Name = "aviv",
                        Obj = null
                    }, "pocs/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var poc = session.Load<Poc>("pocs/1");
                    Assert.Null(poc.Obj);

                    poc.Obj = new
                    {
                        a = 1,
                        b = "2"
                    };
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var poc = session.Load<Poc>("pocs/1");
                    Assert.NotNull(poc.Obj);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Load_WhenDocumentNotFound_ShouldTrack(Options options)
        {
            using var store = GetDocumentStore(options);
            const string notExistId1 = "notExistId1";
            const string notExistId2 = "notExistId2";
            var user = new User();
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(user);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                _ = await session.LoadAsync<User>(notExistId1);
            
                Assert.True(session.Advanced.IsLoaded(notExistId1));
                
                _ = await session.LoadAsync<User>(notExistId1);
                Assert.Equal(1, session.Advanced.NumberOfRequests);
            }
            
            using (var session = store.OpenAsyncSession())
            {
                _ = await session.LoadAsync<User>(new []{notExistId1, notExistId2});

                Assert.True(session.Advanced.IsLoaded(notExistId1));
                Assert.True(session.Advanced.IsLoaded(notExistId2));
                
                _ = await session.LoadAsync<User>(new []{notExistId1, notExistId2});
                Assert.Equal(1, session.Advanced.NumberOfRequests);
            }
            
            using (var session = store.OpenAsyncSession())
            {
                _ = await session.LoadAsync<User>(new []{user.Id, notExistId1});

                Assert.True(session.Advanced.IsLoaded(user.Id));
                Assert.True(session.Advanced.IsLoaded(notExistId1));
                
                _ = await session.LoadAsync<User>(notExistId1);
                Assert.Equal(1, session.Advanced.NumberOfRequests);
            }
        }
        
        class Poc
        {
            public string Name { get; set; }
            public object Obj { get; set; }
        }
    }
}
