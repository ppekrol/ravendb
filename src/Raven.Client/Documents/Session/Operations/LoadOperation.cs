using System;
using System.Collections.Generic;
using System.Linq;
using NLog;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Logging;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Client.Documents.Session.Operations
{
    internal sealed class LoadOperation
    {
        private readonly InMemoryDocumentSessionOperations _session;
        private static readonly Logger Logger = RavenLogManager.Instance.GetLoggerForClient<LoadOperation>();

        private string[] _ids;
        private string[] _includes;
        private string[] _countersToInclude;
        private string[] _revisionsToIncludeByChangeVector;
        private DateTime? _revisionsToIncludeByDateTimeBefore;
        private string[] _compareExchangeValuesToInclude;
        private bool _includeAllCounters;
        private IEnumerable<AbstractTimeSeriesRange> _timeSeriesToInclude;

        private bool _resultsSet;
        private GetDocumentsResult _results;

        public LoadOperation(InMemoryDocumentSessionOperations session)
        {
            _session = session;
        }

        public GetDocumentsCommand CreateRequest()
        {
            if (_session.CheckIfIdAlreadyIncluded(_ids, _includes))
                return null;
            
            _session.IncrementRequestCount();
            if (Logger.IsDebugEnabled)
                Logger.Debug($"Requesting the following ids '{string.Join(", ", _ids)}' from {_session.StoreIdentifier}");

            if (_includeAllCounters)
                return new GetDocumentsCommand(_session.Conventions, _ids, _includes, includeAllCounters: true, timeSeriesIncludes: _timeSeriesToInclude, compareExchangeValueIncludes: _compareExchangeValuesToInclude, metadataOnly: false);
            
            return new GetDocumentsCommand(_session.Conventions, _ids, _includes, _countersToInclude, _revisionsToIncludeByChangeVector, _revisionsToIncludeByDateTimeBefore, _timeSeriesToInclude, _compareExchangeValuesToInclude, metadataOnly: false);
        }

        public LoadOperation ById(string id)
        {
            if (string.IsNullOrWhiteSpace(id))
                return this;

            if (_ids == null)
                _ids = new[] { id };

            return this;
        }

        public LoadOperation WithIncludes(string[] includes)
        {
            _includes = includes;
            return this;
        }

        public LoadOperation WithCompareExchange(string[] compareExchangeValues)
        {
            _compareExchangeValuesToInclude = compareExchangeValues;
            return this;
        }

        public LoadOperation WithCounters(string[] counters)
        {
            if (counters != null)
                _countersToInclude = counters;
            return this;
        }
        
        public LoadOperation WithRevisions(string[] revisionsByChangeVector)
        {
            if (revisionsByChangeVector != null)
                _revisionsToIncludeByChangeVector = revisionsByChangeVector;
            return this;
        }
        
        public LoadOperation WithRevisions(DateTime? revisionByDateTimeBefore)
        {
            if (revisionByDateTimeBefore != null)
                _revisionsToIncludeByDateTimeBefore = revisionByDateTimeBefore;
            return this;
        }
        
        public LoadOperation WithAllCounters()
        {
            _includeAllCounters = true;
            return this;
        }

        public LoadOperation WithTimeSeries(IEnumerable<AbstractTimeSeriesRange> timeseries)
        {
            if (timeseries != null)
                _timeSeriesToInclude = timeseries;
            return this;
        }

        public LoadOperation ByIds(IEnumerable<string> ids)
        {
            _ids = ids
                .Where(id => string.IsNullOrWhiteSpace(id) == false)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToArray();

            return this;
        }

        public T GetDocument<T>()
        {
            if (_session.NoTracking)
            {
                if (_resultsSet == false && _ids.Length > 0)
                    throw new InvalidOperationException($"Cannot execute '{nameof(GetDocument)}' before operation execution.");

                if (_results == null || _results.Results == null || _results.Results.Length == 0)
                    return default;

                var document = _results.Results[0] as BlittableJsonReaderObject;
                if (document == null)
                    return default;

                var documentInfo = DocumentInfo.GetNewDocumentInfo(document);

                return _session.TrackEntity<T>(documentInfo);
            }

            return GetDocument<T>(_ids[0]);
        }

        private T GetDocument<T>(string id)
        {
            if (id == null)
                return default;

            if (_session.IsDeleted(id))
                return default;

            if (_session.DocumentsById.TryGetValue(id, out var doc))
                return _session.TrackEntity<T>(doc);

            if (_session.IncludedDocumentsById.TryGetValue(id, out doc))
                return _session.TrackEntity<T>(doc);

            return default;
        }

        public Dictionary<string, T> GetDocuments<T>()
        {
            var finalResults = new Dictionary<string, T>(StringComparer.OrdinalIgnoreCase);
            if (_session.NoTracking)
            {
                if (_resultsSet == false && _ids.Length > 0)
                    throw new InvalidOperationException($"Cannot execute '{nameof(GetDocuments)}' before operation execution.");

                foreach (var id in _ids)
                {
                    if (id == null)
                        continue;

                    finalResults[id] = default;
                }

                if (_results == null || _results.Results == null || _results.Results.Length == 0)
                    return finalResults;

                foreach (var document in GetDocumentsFromResult(_results))
                    finalResults[document.Id] = _session.TrackEntity<T>(document);

                return finalResults;
            }

            foreach (var id in _ids)
            {
                if (id == null)
                    continue;
                finalResults[id] = GetDocument<T>(id);
            }

            return finalResults;
        }

        public void SetResult(GetDocumentsResult result)
        {
            _resultsSet = true;

            if (_session.NoTracking)
            {
                _results = result;
                return;
            }
            
            if (result == null)
            {
                _session.RegisterMissing(_ids);
                return;
            }

            _session.RegisterIncludes(result.Includes);

            if (_includeAllCounters || _countersToInclude != null)
            {
                _session.RegisterCounters(result.CounterIncludes, _ids, _countersToInclude, _includeAllCounters);
            }

            if (_timeSeriesToInclude != null)
            {
                _session.RegisterTimeSeries(result.TimeSeriesIncludes);
            }
            if (_revisionsToIncludeByChangeVector != null || _revisionsToIncludeByDateTimeBefore != null)
            {
               _session.RegisterRevisionIncludes(result.RevisionIncludes);
            }

            if (_compareExchangeValuesToInclude != null)
            {
                _session.GetClusterSession().RegisterCompareExchangeValues(result.CompareExchangeValueIncludes);
            }

            foreach (var document in GetDocumentsFromResult(result))
                _session.DocumentsById.Add(document);

            foreach (var id in _ids)
            {
                if(_session.DocumentsById.TryGetValue(id, out _) == false)
                    _session.RegisterMissing(id);
            }
            
            _session.RegisterMissingIncludes(result.Results, result.Includes, _includes);
        }

        private static IEnumerable<DocumentInfo> GetDocumentsFromResult(GetDocumentsResult result)
        {
            foreach (BlittableJsonReaderObject document in result.Results)
            {
                if (document == null)
                    continue;

                yield return DocumentInfo.GetNewDocumentInfo(document);
            }
        }
    }
}
