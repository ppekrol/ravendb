﻿using System.IO;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Conventions;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Raven.Client.Json.Serialization.NewtonsoftJson.Internal;
using Raven.Server.Json.Converters;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;
using BlittableJsonConverter = Raven.Server.Json.Converters.BlittableJsonConverter;

namespace SlowTests.Blittable
{
    public class JsonSerializerTests : RavenTestBase
    {
        public JsonSerializerTests(ITestOutputHelper output) : base(output)
        {
        }

        public class Command
        {
            public BlittableJsonReaderArray BlittableArray { get; set; }
            public BlittableJsonReaderObject BlittableObject { get; set; }
            public LazyStringValue LazyString { get; set; }

            [JsonProperty]
            private readonly int _id;

            public Command(int id = 0)
            {
                _id = id;
            }

            public int GetId() => _id;
        }

        [Fact]
        public void JsonDeserialize_WhenBlittableArrayBlittableObjectAndLazyStringAreNull_ShouldResultInCopy()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext readContext))
            {
                Command actual;
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext writeContext))
                using (var writer = new BlittableJsonWriter(writeContext))
                {
                    var jsonSerializer = new JsonSerializer
                    {
                        ContractResolver = new DefaultRavenContractResolver(DocumentConventions.Default.Serialization),
                    };
                    jsonSerializer.Converters.Add(BlittableJsonReaderArrayConverter.Instance);
                    jsonSerializer.Converters.Add(LazyStringValueJsonConverter.Instance);
                    jsonSerializer.Converters.Add(BlittableJsonConverter.Instance);

                    var command = new Command();
                    jsonSerializer.Serialize(writer, command);
                    writer.FinalizeDocument();

                    var toDeseialize = writer.CreateReader();

                    using (var reader = new BlittableJsonReader(readContext))
                    {
                        reader.Initialize(toDeseialize);
                        actual = jsonSerializer.Deserialize<Command>(reader);
                    }
                }

                Assert.Null(actual.BlittableArray);
                Assert.Null(actual.BlittableObject);
                Assert.Null(actual.LazyString);
            }
        }

        [Fact]
        public void JsonDeserialize_WhenHasBlittableJsonReaderArrayProperty_ShouldResultInCopy()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext readContext))
            {
                Command actual;
                BlittableJsonReaderArray expected;
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext writeContext))
                using (var writer = new BlittableJsonWriter(writeContext))
                {
                    var data = new { Property = new[] { "Value1", "Value2" } };
                    var readerObject = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(data, readContext);
                    readerObject.TryGet(nameof(data.Property), out expected);

                    var jsonSerializer = new JsonSerializer
                    {
                        ContractResolver = new DefaultRavenContractResolver(DocumentConventions.Default.Serialization),
                    };
                    jsonSerializer.Converters.Add(BlittableJsonReaderArrayConverter.Instance);
                    jsonSerializer.Converters.Add(LazyStringValueJsonConverter.Instance);
                    jsonSerializer.Converters.Add(BlittableJsonConverter.Instance);

                    var command = new Command { BlittableArray = expected };
                    jsonSerializer.Serialize(writer, command);
                    writer.FinalizeDocument();

                    var toDeseialize = writer.CreateReader();

                    using (var reader = new BlittableJsonReader(readContext))
                    {
                        reader.Initialize(toDeseialize);
                        actual = jsonSerializer.Deserialize<Command>(reader);
                    }
                }

                Assert.Equal(expected, actual.BlittableArray);
            }
        }

        [Fact]
        public void JsonDeserialize_WhenHasBlittableObjectPropertyAndWriteAndReadFromStream_ShouldResultInCommandWithTheProperty()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var jsonSerializer = new JsonSerializer
                {
                    ContractResolver = new DefaultRavenContractResolver(DocumentConventions.Default.Serialization),
                };
                jsonSerializer.Converters.Add(BlittableJsonConverter.Instance);

                var data = new { SomeProperty = "SomeValue" };
                var expected = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(data, context);
                var command = new Command { BlittableObject = expected };

                //Serialize
                BlittableJsonReaderObject toStream;
                using (var writer = new BlittableJsonWriter(context))
                {
                    jsonSerializer.Serialize(writer, command);
                    writer.FinalizeDocument();

                    toStream = writer.CreateReader();
                }

                //Simulates copying to file and loading
                BlittableJsonReaderObject fromStream;
                using (Stream stream = new MemoryStream())
                {
                    //Pass to stream
                    using (var textWriter = new AsyncBlittableJsonTextWriter(context, stream))
                    {
                        context.WriteAsync(textWriter, toStream);
                    }

                    //Get from stream
                    stream.Position = 0;

                    var state = new JsonParserState();
                    var parser = new UnmanagedJsonParser(context, state, "some tag");
                    var peepingTomStream = new PeepingTomStream(stream, context);

                    using (context.GetMemoryBuffer(out var buffer))
                    using (var builder =
                        new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None, "some tag", parser, state))
                    {
                        UnmanagedJsonParserHelper.Read(peepingTomStream, parser, state, buffer);
                        UnmanagedJsonParserHelper.ReadObject(builder, peepingTomStream, parser, buffer);

                        fromStream = builder.CreateReader();
                    }
                }

                //Deserialize
                BlittableJsonReaderObject actual;
                using (var reader = new BlittableJsonReader(context))
                {
                    reader.Initialize(fromStream);
                    var deserialized = jsonSerializer.Deserialize<Command>(reader);
                    actual = deserialized.BlittableObject;
                }

                Assert.Equal(expected, actual);
            }
        }

        [Fact]
        public void JsonDeserialize_WhenHasLazyStringProperty_ShouldResultInCopy()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext readContext))
            {
                Command actual;
                LazyStringValue expected;
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext writeContext))
                using (var writer = new BlittableJsonWriter(writeContext))
                {
                    expected = readContext.GetLazyString("Some Lazy String");
                    var jsonSerializer = new JsonSerializer
                    {
                        ContractResolver = new DefaultRavenContractResolver(DocumentConventions.Default.Serialization),
                        ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor,
                    };

                    jsonSerializer.Converters.Add(LazyStringValueJsonConverter.Instance);
                    var command = new Command { LazyString = expected };
                    jsonSerializer.Serialize(writer, command);
                    writer.FinalizeDocument();

                    var toDeseialize = writer.CreateReader();

                    using (var reader = new BlittableJsonReader(readContext))
                    {
                        reader.Initialize(toDeseialize);
                        actual = jsonSerializer.Deserialize<Command>(reader);
                    }
                }

                Assert.Equal(expected, actual.LazyString);
            }
        }

        [Fact]
        public void JsonDeserialize_WhenHasBlittableObjectProperty_ShouldResultInCopy()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext readContext))
            {
                Command actual;
                BlittableJsonReaderObject expected;
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext writeContext))
                using (var writer = new BlittableJsonWriter(writeContext))
                {
                    var data = new { Property = "Value" };
                    expected = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(data, readContext);
                    var jsonSerializer = new JsonSerializer
                    {
                        ContractResolver = new DefaultRavenContractResolver(DocumentConventions.Default.Serialization),
                    };

                    jsonSerializer.Converters.Add(BlittableJsonConverter.Instance);
                    var command = new Command { BlittableObject = expected };
                    jsonSerializer.Serialize(writer, command);
                    writer.FinalizeDocument();

                    var toDeseialize = writer.CreateReader();

                    using (var reader = new BlittableJsonReader(readContext))
                    {
                        reader.Initialize(toDeseialize);
                        actual = jsonSerializer.Deserialize<Command>(reader);
                    }
                }

                Assert.Equal(expected, actual.BlittableObject);
            }
        }

        [Fact]
        public void JsonSerialize_WhenLazyStringValueIsProperty_ShouldSerialize()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonWriter(context))
            {
                var expected = context.GetLazyString("igal");

                var jsonSerializer = new JsonSerializer
                {
                    ContractResolver = new DefaultRavenContractResolver(DocumentConventions.Default.Serialization),
                    ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor,
                };

                jsonSerializer.Converters.Add(LazyStringValueJsonConverter.Instance);
                var command = new Command { LazyString = expected };
                jsonSerializer.Serialize(writer, command);
                writer.FinalizeDocument();

                //Assert
                var reader = writer.CreateReader();
                reader.TryGet(nameof(Command.LazyString), out LazyStringValue actual);
                Assert.Equal(expected, actual);
            }
        }

        [Fact]
        public void JsonSerialize_WhenBlittableObjectIsProperty_ShouldResultInCopy()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonWriter(context))
            {
                var data = new { Property = "Value" };
                var expected = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(data, context);
                var jsonSerializer = new JsonSerializer
                {
                    ContractResolver = new DefaultRavenContractResolver(DocumentConventions.Default.Serialization),
                    ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor,
                };

                jsonSerializer.Converters.Add(BlittableJsonConverter.Instance);
                var command = new Command { BlittableObject = expected };
                jsonSerializer.Serialize(writer, command);
                writer.FinalizeDocument();

                //Assert
                var reader = writer.CreateReader();
                reader.TryGet(nameof(Command.BlittableObject), out BlittableJsonReaderObject actual);
                Assert.Equal(expected, actual);
            }
        }

        [Fact(Skip = "To consider if should support direct serialize of LazyStringValue")]
        //Todo To consider if should support direct serialize of LazyStringValue
        public void JsonSerialize_WhenLazyStringValueIsTheRoot_ShouldSerialize()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonWriter(context))
            {
                var expected = context.GetLazyString("igal");

                var jsonSerializer = new JsonSerializer
                {
                    ContractResolver = new DefaultRavenContractResolver(DocumentConventions.Default.Serialization),
                    ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor,
                };

                jsonSerializer.Converters.Add(LazyStringValueJsonConverter.Instance);
                jsonSerializer.Serialize(writer, expected);
                writer.FinalizeDocument();

                //Assert
                var reader = writer.CreateReader();
                //                Assert.Equal(expected, actual);
            }
        }

        [Fact]
        public void JsonSerialize_WhenNestedBlittableObjectIsProperty_ShouldSerialize()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            //TODO To consider if should support direct couple of write on the same context
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context2))
            using (var writer = new BlittableJsonWriter(context2))
            {
                var data = new { ParentProperty = new { NestedProperty = "Some Value" } };
                var parentBlittable = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(data, context);
                parentBlittable.TryGet(nameof(data.ParentProperty), out BlittableJsonReaderObject expected);
                var jsonSerializer = new JsonSerializer
                {
                    ContractResolver = new DefaultRavenContractResolver(DocumentConventions.Default.Serialization),
                    ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor,
                };

                jsonSerializer.Converters.Add(BlittableJsonConverter.Instance);
                var command = new Command { BlittableObject = expected };
                jsonSerializer.Serialize(writer, command);
                writer.FinalizeDocument();

                //Assert
                var reader = writer.CreateReader();
                reader.TryGet(nameof(Command.BlittableObject), out BlittableJsonReaderObject actual);
                Assert.Equal(expected, actual);
            }
        }

        [Fact(Skip = "To consider if should support direct serialize of BlittableObject")]
        //Todo To consider if should support direct serialize of BlittableObject
        public void JsonSerialize_WhenBlittableIsTheRoot_ShouldResultInCopy()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonWriter(context))
            {
                var data = new DynamicJsonValue
                {
                    ["Property"] = "Value"
                };
                var blittableData = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(data, context);

                var jsonSerializer = new JsonSerializer
                {
                    ContractResolver = new DefaultRavenContractResolver(DocumentConventions.Default.Serialization),
                    ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor,
                };

                jsonSerializer.Converters.Add(BlittableJsonConverter.Instance);
                jsonSerializer.Serialize(writer, blittableData);
                writer.FinalizeDocument();

                //Assert
                var result = writer.CreateReader();
                Assert.True(result.TryGet(nameof(data), out object _));
            }
        }
    }
}
