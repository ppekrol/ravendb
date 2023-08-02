using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Test;

internal sealed class TestIndexResult
{
    public List<BlittableJsonReaderObject> IndexEntries;
    public List<Document> QueryResults;
    public List<BlittableJsonReaderObject> MapResults;
    public List<BlittableJsonReaderObject> ReduceResults;
    public bool HasDynamicFields;
    public bool IsStale;
    public IndexType IndexType;
    
    public async Task WriteTestIndexResultAsync(Stream responseBodyStream, DocumentsOperationContext context)
    {
        await using (var writer = new AsyncBlittableJsonTextWriter(context, responseBodyStream))
        {
            writer.WriteStartObject();

            writer.WriteArray(context, nameof(IndexEntries), IndexEntries, (w, c, indexEntry) =>
            {
                w.WriteObject(indexEntry);
            });
                        
            writer.WriteComma();
                        
            writer.WriteArray(context, nameof(QueryResults), QueryResults, (w, c, queryResult) =>
            {
                w.WriteObject(queryResult.Data);
            });
                        
            writer.WriteComma();
            
            writer.WriteArray(context, nameof(MapResults), MapResults, (w, c, mapResult) =>
            {
                w.WriteObject(mapResult);
            });

            writer.WriteComma();
            
            writer.WriteArray(context, nameof(ReduceResults), ReduceResults, (w, c, indexingFunctionResult) =>
            {
                w.WriteObject(indexingFunctionResult);
            });
            
            writer.WriteComma();
            
            writer.WritePropertyName(nameof(HasDynamicFields));
            writer.WriteBool(HasDynamicFields);
            
            writer.WriteComma();
            
            writer.WritePropertyName(nameof(IsStale));
            writer.WriteBool(IsStale);
            
            writer.WriteComma();
            
            writer.WritePropertyName(nameof(IndexType));
            writer.WriteString(IndexType.ToString());

            writer.WriteEndObject();
        }
    }
}
