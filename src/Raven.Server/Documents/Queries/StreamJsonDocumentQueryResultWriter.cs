using System;
using System.IO;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Session;
using Raven.Server.Json;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public class StreamJsonDocumentQueryResultWriter : IStreamQueryResultWriter<Document>
    {
        private AsyncBlittableJsonTextWriter _writer;
        private HttpResponse _response;
        private JsonOperationContext _context;
        private bool _first = true;

        public StreamJsonDocumentQueryResultWriter(HttpResponse response, Stream stream, JsonOperationContext context)
        {
            _context = context;
            _writer = new AsyncBlittableJsonTextWriter(context, stream);
            _response = response;
        }

        public void Dispose()
        {
            _writer.Dispose();
        }

        public void StartResponse()
        {
            _writer.WriteStartObjectAsync();
        }

        public void StartResults()
        {
            _writer.WritePropertyNameAsync("Results");
            _writer.WriteStartArrayAsync();
        }

        public void EndResults()
        {
            _writer.WriteEndArrayAsync();
        }

        public void AddResult(Document res)
        {
            if (_first == false)
            {
                _writer.WriteCommaAsync();
            }
            else
            {
                _first = false;
            }
            _writer.WriteDocument(_context, res, metadataOnly: false);
        }

        public void EndResponse()
        {
            _writer.WriteEndObjectAsync();
        }

        public void WriteError(Exception e)
        {
            _writer.WriteCommaAsync();

            _writer.WritePropertyNameAsync("Error");
            _writer.WriteStringAsync(e.ToString());
        }

        public void WriteError(string error)
        {            
            _writer.WritePropertyNameAsync("Error");
            _writer.WriteStringAsync(error);
        }

        public void WriteQueryStatistics(long resultEtag, bool isStale, string indexName, long totalResults, DateTime timestamp)
        {
            _writer.WritePropertyNameAsync(nameof(StreamQueryStatistics.ResultEtag));
            _writer.WriteIntegerAsync(resultEtag);
            _writer.WriteCommaAsync();

            _writer.WritePropertyNameAsync(nameof(StreamQueryStatistics.IsStale));
            _writer.WriteBoolAsync(isStale);
            _writer.WriteCommaAsync();

            _writer.WritePropertyNameAsync(nameof(StreamQueryStatistics.IndexName));
            _writer.WriteStringAsync(indexName);
            _writer.WriteCommaAsync();

            _writer.WritePropertyNameAsync(nameof(StreamQueryStatistics.TotalResults));
            _writer.WriteIntegerAsync(totalResults);
            _writer.WriteCommaAsync();

            _writer.WritePropertyNameAsync(nameof(StreamQueryStatistics.IndexTimestamp));
            _writer.WriteStringAsync(timestamp.GetDefaultRavenFormat(isUtc: true));
            _writer.WriteCommaAsync();
        }

        public bool SupportError => true;
        public bool SupportStatistics => true;
    }
}
