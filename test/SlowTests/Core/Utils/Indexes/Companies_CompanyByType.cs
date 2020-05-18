// -----------------------------------------------------------------------
//  <copyright file="QueryResultsStreaming.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Company = SlowTests.Core.Utils.Entities.Company;

namespace SlowTests.Core.Utils.Indexes
{
    public class Companies_CompanyByType : AbstractIndexCreationTask<Company, Companies_CompanyByType.ReduceResult>
    {
        public class ReduceResult
        {
            public Company.CompanyType Type { get; set; }
            public int ContactsCount { get; set; }
            public DateTime LastModified { get; set; }
        }

        public Companies_CompanyByType()
        {
            Map = companies => from company in companies
                               select new
                               {
                                   Type = company.Type,
                                   ContactsCount = company.Contacts.Count,
                                   LastModified = MetadataFor(company).Value<DateTime>(Constants.Documents.Metadata.LastModified)
                               };

            Reduce = results => from result in results
                                group result by result.Type
                                    into g
                                select new
                                {
                                    Type = g.Key,
                                    ContactsCount = g.Sum(x => x.ContactsCount),
                                    LastModified = g.Select(x => x.LastModified).First()
                                };
        }
    }
}
