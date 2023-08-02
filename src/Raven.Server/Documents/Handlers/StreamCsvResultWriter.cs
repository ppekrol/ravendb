﻿using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Json;
using Raven.Client.Util;
using Raven.Server.Documents.Queries;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    internal abstract class StreamCsvResultWriter<T> : IStreamQueryResultWriter<T>
    {
        private readonly StreamWriter _writer;
        private readonly CsvWriter _csvWriter;
        private (string, string)[] _properties;
        private bool _writeHeader = true;

        private readonly HashSet<string> _metadataPropertiesToSkip = new HashSet<string>
        {
            Constants.Documents.Metadata.Attachments,
            Constants.Documents.Metadata.Counters,
            Constants.Documents.Metadata.Flags,
            Constants.Documents.Metadata.ChangeVector,
            Constants.Documents.Metadata.Id,
            Constants.Documents.Metadata.LastModified,
            Constants.Documents.Metadata.IndexScore,
        };

        protected StreamCsvResultWriter(HttpResponse response, Stream stream, string[] properties = null, string csvFileNamePrefix = "export")
        {
            var encodedCsvFileName = Uri.EscapeDataString($"{csvFileNamePrefix}_{SystemTime.UtcNow.ToString("yyyyMMdd_HHmm", CultureInfo.InvariantCulture)}.csv");

            response.Headers[Constants.Headers.ContentDisposition] = $"attachment; filename=\"{encodedCsvFileName}\"; filename*=UTF-8''{encodedCsvFileName}";
            response.Headers[Constants.Headers.ContentType] = "text/csv";

            _writer = new StreamWriter(stream, Encoding.UTF8);
            _csvWriter = new CsvWriter(_writer, new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                Delimiter = ","
            });

            //We need to write headers without the escaping but the path should be escaped
            //so @metadata.@collection should not be written in the header as @metadata\.@collection
            //We can't escape while constructing the path since we will write the escaping in the header this way, we need both.
            _properties = properties?.Select(p => (p, Escape(p))).ToArray();
        }

        protected void WriteCsvHeaderIfNeeded(BlittableJsonReaderObject blittable, bool writeIds = true)
        {
            if (_writeHeader == false)
                return;
            if (_properties == null)
            {
                _properties = GetPropertiesRecursive((string.Empty, string.Empty), blittable, writeIds).ToArray();
            }
            _writeHeader = false;
            foreach ((var property, var path) in _properties)
            {
                _csvWriter.WriteField(property);
            }

            _csvWriter.NextRecord();
        }

        private readonly char[] _splitter = { '.' };

        private string Escape(string s)
        {
            var tokens = s.Split(_splitter, StringSplitOptions.RemoveEmptyEntries);
            return string.Join('.', tokens.Select(BlittablePath.EscapeString));
        }

        public async ValueTask DisposeAsync()
        {
            if (_csvWriter != null)
                await _csvWriter.DisposeAsync();

            if (_writer != null)
                await _writer.DisposeAsync();
        }

        public void StartResponse()
        {
        }

        public void StartResults()
        {
        }

        public void EndResults()
        {
        }

        public abstract ValueTask AddResultAsync(T res, CancellationToken token);

        public CsvWriter GetCsvWriter()
        {
            return _csvWriter;
        }

        public (string, string)[] GetProperties()
        {
            return _properties;
        }

        private IEnumerable<(string Property, string Path)> GetPropertiesRecursive((string ParentProperty, string ParentPath) propertyTuple, BlittableJsonReaderObject obj, bool addId = true)
        {
            var inMetadata = Constants.Documents.Metadata.Key.Equals(propertyTuple.ParentPath);
            if (addId)
            {
                yield return (Constants.Documents.Metadata.Id, Constants.Documents.Metadata.Id);
            }

            foreach (var p in obj.GetPropertyNames())
            {
                // skip reserved metadata properties
                if (inMetadata && p.StartsWith('@') && _metadataPropertiesToSkip.Contains(p))
                    continue;

                if (p.StartsWith('@') && p.Equals(Constants.Documents.Metadata.Key) == false && propertyTuple.ParentPath.Equals(Constants.Documents.Metadata.Key) == false)
                    continue;

                var path = string.IsNullOrEmpty(propertyTuple.ParentPath) ? BlittablePath.EscapeString(p) : $"{propertyTuple.ParentPath}.{BlittablePath.EscapeString(p)}";
                var property = string.IsNullOrEmpty(propertyTuple.ParentPath) ? p : $"{propertyTuple.ParentPath}.{p}";
                if (obj.TryGetMember(p, out var res) && res is BlittableJsonReaderObject)
                {
                    foreach (var nested in GetPropertiesRecursive((property, path), res as BlittableJsonReaderObject, addId: false))
                    {
                        yield return nested;
                    }
                }
                else
                {
                    yield return (property, path);
                }
            }
        }

        public void EndResponse()
        {
        }

        public async ValueTask WriteErrorAsync(Exception e)
        {
            await _writer.WriteLineAsync(e.ToString());
        }

        public async ValueTask WriteErrorAsync(string error)
        {
            await _writer.WriteLineAsync(error);
        }

        public void WriteQueryStatistics(long resultEtag, bool isStale, string indexName, long totalResults, DateTime timestamp)
        {
            throw new NotImplementedException();
        }

        public bool SupportError => false;
        public bool SupportStatistics => false;
    }
}
