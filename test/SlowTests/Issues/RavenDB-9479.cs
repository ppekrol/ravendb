﻿using System.IO;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_9479 : RavenTestBase
    {
        public RavenDB_9479(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData((char)0)]
        [InlineData((char)1)]
        [InlineData((char)16)]
        [InlineData((char)32)]
        [InlineData((char)127)]
        [InlineData((char)159)]
        [InlineData((char)114)]
        public async Task GetStoreAndGetChar(char ch)
        {
            using (var store = GetDocumentStore())
            {
                string documentId;
                using (var session = store.OpenAsyncSession())
                {
                    var question = new Question
                    {
                        Letter = ch
                    };

                    await session.StoreAsync(question);
                    await session.SaveChangesAsync();

                    documentId = question.Id;
                }
                using (var session = store.OpenAsyncSession())
                {
                    var question = await session.LoadAsync<Question>(documentId);
                    Assert.Equal(ch, question.Letter);
                }
            }
        }

        [Fact]
        public async Task GetStoreAndGetControlChar()
        {
            using (var store = GetDocumentStore())
            {
                string documentId;
                using (var session = store.OpenAsyncSession())
                {
                    var question = new Question
                    {
                        Letter = '\0',
                        Name = "Hello \0 World"
                    };

                    await session.StoreAsync(question);
                    await session.SaveChangesAsync();

                    documentId = question.Id;
                }

                using (var session = store.OpenAsyncSession())
                {
                    var question = await session.LoadAsync<Question>(documentId);
                    Assert.Equal('\0', question.Letter);
                    Assert.Equal("Hello \0 World", question.Name);
                }
            }
        }

        [Fact]
        public async Task JsonWithEscapeChar()
        {
            using (var stream = new MemoryStream())
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                for (var i = 0; i < 100; i++)
                {
                    for (var j = 0; j < 100; j++)
                    {
                        stream.SetLength(0);

                        var expectedString = new string((char)i, j);

                        await using (var writer = new AsyncBlittableJsonTextWriter(context, stream))
                        {
                            await writer.WriteStartObjectAsync();

                            await writer.WritePropertyNameAsync("Name");
                            await writer.WriteStringAsync(expectedString);

                            await writer.WriteEndObjectAsync();
                        }

                        stream.Position = 0;
                        var json = context.ReadForDisk(stream, "json");
                        json.BlittableValidation();

                        Assert.True(json.TryGet("Name", out string actualString));
                        Assert.Equal(expectedString, actualString);
                    }
                }
            }
        }

        [Fact]
        public async Task JsonWithEscapeChar_Manual()
        {
            await using (var stream = new MemoryStream())
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var expectedString0 = new string((char)7, 10);
                var expectedString1 = new string((char)7, 3) + "bbbb" + new string((char)7, 2) + "cccc" + new string((char)7, 5);
                var expectedString2 = "zzzz" + new string((char)7, 3) + "bbbb" + new string((char)7, 2) + "cccc" + new string((char)7, 5);
                var expectedString3 = "zzzz" + new string((char)7, 3) + "bbbb" + new string((char)7, 2) + "cccc" + new string((char)7, 5) + "xxxx";
                string expectedJson;

                await using (var writer = new AsyncBlittableJsonTextWriter(context, stream))
                {
                    await writer.WriteStartObjectAsync();

                    await writer.WritePropertyNameAsync("Name0");
                    await writer.WriteStringAsync(expectedString0);
                    await writer.WriteCommaAsync();

                    await writer.WritePropertyNameAsync("Name1");
                    await writer.WriteStringAsync(expectedString1);
                    await writer.WriteCommaAsync();

                    await writer.WritePropertyNameAsync("Name2");
                    await writer.WriteStringAsync(expectedString2);
                    await writer.WriteCommaAsync();

                    await writer.WritePropertyNameAsync("Name3");
                    await writer.WriteStringAsync(expectedString3);

                    await writer.WriteEndObjectAsync();
                }

                stream.Position = 0;
                using (var sr = new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: 1024, leaveOpen: true))
                    expectedJson = await sr.ReadToEndAsync();

                Assert.Equal("{\"Name0\":\"\\u0007\\u0007\\u0007\\u0007\\u0007\\u0007\\u0007\\u0007\\u0007\\u0007\",\"Name1\":\"\\u0007\\u0007\\u0007bbbb\\u0007\\u0007cccc\\u0007\\u0007\\u0007\\u0007\\u0007\",\"Name2\":\"zzzz\\u0007\\u0007\\u0007bbbb\\u0007\\u0007cccc\\u0007\\u0007\\u0007\\u0007\\u0007\",\"Name3\":\"zzzz\\u0007\\u0007\\u0007bbbb\\u0007\\u0007cccc\\u0007\\u0007\\u0007\\u0007\\u0007xxxx\"}", expectedJson);

                stream.Position = 0;
                var json = context.ReadForDisk(stream, "json");
                json.BlittableValidation();

                var actualJson = json.ToString();
                Assert.Equal(expectedJson, actualJson);

                Assert.True(json.TryGet("Name0", out string actualString));
                Assert.Equal(expectedString0, actualString);

                Assert.True(json.TryGet("Name1", out actualString));
                Assert.Equal(expectedString1, actualString);

                Assert.True(json.TryGet("Name2", out actualString));
                Assert.Equal(expectedString2, actualString);

                Assert.True(json.TryGet("Name3", out actualString));
                Assert.Equal(expectedString3, actualString);
            }
        }

        private class Question
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public char Letter { get; set; }
        }
    }
}
