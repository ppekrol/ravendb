﻿using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class StreamCsvBlittableQueryResultWriter : StreamCsvResultWriter<BlittableJsonReaderObject>
    {
        public override ValueTask AddResult(BlittableJsonReaderObject res)
        {
            WriteCsvHeaderIfNeeded(res, false);

            foreach (var (_, path) in GetProperties())
            {
                var o = new BlittablePath(path).Evaluate(res);
                GetCsvWriter().WriteField(o?.ToString());
            }

            GetCsvWriter().NextRecord();
            return default;
        }

        public StreamCsvBlittableQueryResultWriter(HttpResponse response, Stream stream, string[] properties = null,
            string csvFileName = "export") : base(response, stream, properties, csvFileName)
        {
        }
    }
}
