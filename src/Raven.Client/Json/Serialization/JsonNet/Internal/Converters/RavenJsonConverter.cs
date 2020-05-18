using System;
using System.Linq;
using Newtonsoft.Json;

namespace Raven.Client.Json.Serialization.JsonNet.Internal.Converters
{
    internal abstract class RavenJsonConverter : JsonConverter
    {
        protected object DeferReadToNextConverter(JsonReader reader, Type objectType, JsonSerializer serializer, object existingValue)
        {
            var anotherConverter = serializer.Converters
                .Skip(serializer.Converters.IndexOf(this) + 1)
                .FirstOrDefault(x => x.CanConvert(objectType));
            if (anotherConverter != null)
                return anotherConverter.ReadJson(reader, objectType, existingValue, serializer);
            return reader.Value;
        }
    }
}
