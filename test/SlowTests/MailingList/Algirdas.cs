﻿// -----------------------------------------------------------------------
//  <copyright file="Algirdas.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class Algirdas : RavenTestBase
    {
        public Algirdas(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CheckForCorrectDateCompareBetweenLocalAndUtc(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var localDate = DateTimeOffset.Now;
                var obj = new ObjectWithDate
                {
                    LocalDate = localDate,
                    UtcDate = localDate.ToUniversalTime()
                };

                using (var session = store.OpenSession())
                {
                    session.Store(obj);
                    session.SaveChanges();
                }

                using (var readSession = store.OpenSession())
                {
                    var equal = readSession.Query<ObjectWithDate>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .FirstOrDefault(d => d.LocalDate == obj.UtcDate);

                    if (equal == null)
                        throw new Exception("Couldn't find object");
                }
            }
        }

        private class ObjectWithDate
        {
            public string Id { get; set; }
            public DateTimeOffset UtcDate { get; set; }
            public DateTimeOffset LocalDate { get; set; }
        }
    }
}
