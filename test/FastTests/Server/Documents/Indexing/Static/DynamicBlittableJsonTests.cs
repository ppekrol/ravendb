﻿using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes.Static;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Documents.Indexing.Static
{
    public class DynamicBlittableJsonTests : ParallelTestBase
    {
        private readonly JsonOperationContext _ctx;
        private readonly List<BlittableJsonReaderObject> _docs = new List<BlittableJsonReaderObject>();

        public DynamicBlittableJsonTests(ITestOutputHelper output) : base(output)
        {
            _ctx = JsonOperationContext.ShortTermSingleUse();
        }

        [Fact]
        public void Can_get_simple_values()
        {
            var now = SystemTime.UtcNow;

            using (var lazyStringValue = _ctx.GetLazyString("22.0"))
            {
                
                var stringValue = _ctx.GetLazyString("Arek");
                var doc = create_doc(new DynamicJsonValue
                {
                    ["Name"] = "Arek",
                    ["Address"] = new DynamicJsonValue
                    {
                        ["City"] = "NYC"
                    },
                    ["NullField"] = null,
                    ["Age"] = new LazyNumberValue(lazyStringValue),
                    ["LazyName"] = stringValue,
                    ["Friends"] = new DynamicJsonArray
                {
                    new DynamicJsonValue
                    {
                        ["Name"] = "Joe"
                    },
                    new DynamicJsonValue
                    {
                        ["Name"] = "John"
                    }
                },
                    [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    {
                        [Constants.Documents.Metadata.Collection] = "Users",
                        [Constants.Documents.Metadata.LastModified] = now.GetDefaultRavenFormat(true)
                    }
                }, "users/1");

                dynamic user = new DynamicBlittableJson(doc);

                Assert.Equal("Arek", user.Name);
                Assert.Equal("NYC", user.Address.City);
                Assert.Equal("users/1", user.Id);
                Assert.Equal(DynamicNullObject.Null, user.NullField);
                Assert.Equal(22.0, user.Age);
                Assert.Equal("Arek", user.LazyName);
                Assert.Equal(2, user.Friends.Length);
                Assert.Equal("Users", user[Constants.Documents.Metadata.Key][Constants.Documents.Metadata.Collection]);
                Assert.Equal(now, user[Constants.Documents.Metadata.Key].Value<DateTime>(Constants.Documents.Metadata.LastModified));
            }
        }

        internal Document create_doc(DynamicJsonValue document, string id)
        {
            var data = _ctx.ReadObject(document, id);

            _docs.Add(data);

            return new Document
            {
                Data = data,
                Id = _ctx.GetLazyString(id),
                LowerId = _ctx.GetLazyString(id.ToLowerInvariant())
            };
        }

        public override void Dispose()
        {
            try
            {
                foreach (var docReader in _docs)
                {
                    docReader.Dispose();
                }

                _ctx.Dispose();
            }
            finally
            {
                base.Dispose();
            }
        }
    }
}
