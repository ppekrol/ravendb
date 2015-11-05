// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1601.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Database.Bundles.ScriptedIndexResults;
using Raven.Database.Json;
using Raven.Json.Linq;
using Raven.Tests.Bundles.ScriptedIndexResults;
using Raven.Tests.Common;

using Xunit;
using Raven.Client.Linq;
using System.Linq;
using FluentAssertions;
using Raven.Abstractions.Logging;

namespace Raven.Tests.Issues
{
    public class RavenDB_3996 : RavenTest
    {
        protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
        {
            configuration.Settings["Raven/ActiveBundles"] = "ScriptedIndexResults";
        }

        [Fact]
        public void Test()
        {
            using (var store = NewDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new ScriptedIndexResults
                    {
                        Id = ScriptedIndexResults.IdPrefix + new TestIndex().IndexName,
                        IndexScript = @"//something",
                        DeleteScript = @""
                    });
                    s.SaveChanges();
                }
                string docId;
                using (var s = store.OpenSession())
                {
                    var animal = new Animal
                    {
                        Id = "pluto",
                        Name = "Pluto",
                        Type = "Dog"
                    };
                    s.Store(animal);
                    
                    s.SaveChanges();
                }

                new TestIndex().Execute(store);

                WaitForIndexing(store);
                
                // TODO: make assert more robust
                var warnLogs = LogManager.GetTarget<TestMemoryTarget>()["<system>"].WarnLog;
                warnLogs.Should().NotContain(entry => entry.FormattedMessage.Contains("Could not apply index script"));
            }
        }


        public class TestIndex : AbstractMultiMapIndexCreationTask<TestIndex.Result>
        {
            public class Result
            {
                public string NullString { get; set; }
            }
            public TestIndex()
            {
                AddMap<Animal>(animals =>
                    from animal in animals
                    select new
                    {
                        NullString = (string)null
                    });
                Store(a => a.NullString, FieldStorage.Yes);
            }
        }
    }
}
