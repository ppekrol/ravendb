using System.Collections.Generic;
using Raven.Client.Documents.Operations.CompareExchange;
using Sparrow.Json;

namespace Raven.Server.Documents.Includes;

internal interface ICompareExchangeValueIncludes
{
    public Dictionary<string, CompareExchangeValue<BlittableJsonReaderObject>> Results { get; }
}
