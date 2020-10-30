﻿using System.IO;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_12731 : NoDisposalNeeded
    {
        public RavenDB_12731(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanCompareLazyStringValueAndLazyCompressedStringValue()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var ms = new MemoryStream())
            using (var writer = new AsyncBlittableJsonTextWriter(context, ms))
            {
                writer.WriteStartObjectAsync();
                writer.WritePropertyNameAsync("Test");
                writer.WriteStringAsync(new string('c', 1024 * 1024));
                writer.WriteEndObjectAsync();
                writer.FlushAsync();
                ms.Flush();

                ms.Position = 0;
                var json = context.Read(ms, "test");

                ms.Position = 0;
                var json2 = context.Read(ms, "test");

                var lcsv1 = (LazyCompressedStringValue)json["Test"];
                var lcsv2 = (LazyCompressedStringValue)json2["Test"];
                var lsv2 = lcsv2.ToLazyStringValue();

                Assert.Equal(lcsv1, lsv2);
                Assert.Equal(lsv2, lcsv1);
                Assert.Equal(lsv2, lcsv2);
                Assert.Equal(lcsv2, lsv2);
            }
        }
    }
}
