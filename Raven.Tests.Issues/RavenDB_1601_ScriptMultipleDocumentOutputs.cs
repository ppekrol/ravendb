// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1601_ScriptMultipleDocumentOutputs.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
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

namespace Raven.Tests.Issues
{
    public class RavenDB_1601_ScriptMultipleDocumentOutputs : RavenTest
    {
        protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
        {
            configuration.Settings["Raven/ActiveBundles"] = "ScriptedIndexResults";
        }
        
        [Fact]
        public void EachDocumentOutputHasItsOwnKey()
        {
            using (var store = NewDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new ScriptedIndexResults
                    {
                        Id = ScriptedIndexResults.IdPrefix + new Animals_Multiple_Personality().IndexName,
                        IndexScript = @"
var docId = 'AnimalPersonality/'+ key;
PutDocument(docId, this);",
                        DeleteScript = @""
                    });
                    s.SaveChanges();
                }
                using (var s = store.OpenSession())
                {
                    s.Store(new Animal
                    {
                        Name = "Pluto",
                        Type = "Dog"
                    });
                    
                    s.SaveChanges();
                }

                new Animals_Multiple_Personality().Execute(store);

                WaitForIndexing(store);

                using (var s = store.OpenSession())
                {
                    s.Advanced.LoadStartingWith<RavenJObject>("AnimalPersonality/").ToArray().Select(r => r.Value<string>("Personality"))
                        .Should()
                        .Contain(new[] { "good", "bad" }, "each personality is indexed separately", reasonArg: null);
                }
            }
        }
        

        public class Animals_Multiple_Personality : AbstractIndexCreationTask<Animal, Animals_Multiple_Personality.Result>
        {
            public class Result
            {
                public string Name { get; set; }
                public string Personality { get; set; }
            }
            public Animals_Multiple_Personality()
            {
                Map = animals =>
                      from animal in animals
                      from personality in new string[] {"good", "bad"}
                      select new
                      {
                          animal.Name,
                          Personality = personality
                      };
                Store(a => a.Name, FieldStorage.Yes);
                Store(a => a.Personality, FieldStorage.Yes);
            }
        }
    }
}