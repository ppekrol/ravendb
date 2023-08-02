﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Queries.Results.TimeSeries
{
    internal sealed class AggregationHolder
    {
        public static readonly object NullBucket = new object();

        // local pool, for current query
        private readonly ObjectPool<ITimeSeriesAggregation[], TimeSeriesAggregationReset> _pool;
        private readonly int _poolSize = 32;

        private readonly DocumentsOperationContext _context;
        private readonly InterpolationType _interpolationType;

        private readonly AggregationType[] _types;
        public bool Contains(AggregationType type) => _types.Contains(type);

        private readonly string[] _names;
        private double? _percentile;

        private Dictionary<object, ITimeSeriesAggregation[]> _current;
        private Dictionary<object, PreviousAggregation> _previous;

        public bool HasValues => _current?.Count > 0;

        public AggregationHolder(DocumentsOperationContext context, Dictionary<AggregationType, string> types, InterpolationType interpolationType, double? percentile = null)
        {
            _context = context;

            _names = types.Values.ToArray();
            _types = types.Keys.ToArray();

            _percentile = percentile;

            _interpolationType = interpolationType;
            if (_interpolationType != InterpolationType.None)
                _poolSize *= 2;

            _pool = new ObjectPool<ITimeSeriesAggregation[], TimeSeriesAggregationReset>(TimeSeriesAggregationFactory, _poolSize);
        }

        private ITimeSeriesAggregation[] TimeSeriesAggregationFactory()
        {
            var bucket = new ITimeSeriesAggregation[_types.Length];
            for (int i = 0; i < _types.Length; i++)
            {
                var type = _types[i];
                var name = _names?[i];

                switch (type)
                {
                    case AggregationType.Average:
                        bucket[i] = new AverageAggregation(name);
                        continue;
                    case AggregationType.Percentile:
                        Debug.Assert(_percentile.HasValue, $"Invalid {nameof(AggregationType.Percentile)} aggregation method. 'percentile' argument has no value");
                        bucket[i] = new PercentileAggregation(name, _percentile.Value);
                        continue;
                    case AggregationType.Slope:
                        bucket[i] = new SlopeAggregation(name);
                        continue;
                    case AggregationType.StandardDeviation:
                        bucket[i] = new StandardDeviationAggregation(name);
                        continue;
                    default:
                        bucket[i] = new TimeSeriesAggregation(type, name);
                        break;
                }
            }

            return bucket;
        }

        public ITimeSeriesAggregation[] this[object bucket]
        {
            get
            {
                var key = Clone(bucket);

                _current ??= new Dictionary<object, ITimeSeriesAggregation[]>();
                if (_current.TryGetValue(key, out var value))
                    return value;

                return _current[key] = _pool.Allocate();
            }
        }

        public IEnumerable<DynamicJsonValue> AddCurrentToResults(RangeGroup range, double? scale)
        {
            if (_interpolationType != InterpolationType.None)
            {
                foreach (var gap in FillMissingGaps(range, scale))
                {
                    yield return gap;
                }
            }

            foreach (var kvp in _current)
            {
                var key = kvp.Key;
                var value = kvp.Value;

                if (value[0].Any == false)
                    continue;

                yield return ToJson(scale, range.Start, range.End, key, value);

                if (_interpolationType == InterpolationType.None)
                {
                    _pool.Free(value);
                    continue;
                }

                UpdatePrevious(key, range, value);
            }

            _current = null;
        }

        private DynamicJsonValue ToJson(double? scale, DateTime? from, DateTime? to, object key, ITimeSeriesAggregation[] value)
        {
            if (from == DateTime.MinValue)
                from = null;
            if (to == DateTime.MaxValue)
                to = null;

            var result = new DynamicJsonValue
            {
                [nameof(TimeSeriesRangeAggregation.From)] = from, 
                [nameof(TimeSeriesRangeAggregation.To)] = to, 
                [nameof(TimeSeriesRangeAggregation.Key)] = GetNameFromKey(key)
            };

            for (int i = 0; i < value.Length; i++)
            {
                result[value[i].Name] = new DynamicJsonArray(value[i].GetFinalValues(from, to, scale).Cast<object>());
            }

            return result;
        }

        private IEnumerable<(object Key, PreviousAggregation Previous, ITimeSeriesAggregation[] Current)> GetGapsPerBucket(DateTime to)
        {
            if (_current == null || _previous == null)
                yield break;

            foreach (var previous in _previous)
            {
                var key = previous.Key;
                if (_current.ContainsKey(key) == false)
                    continue;

                var gapData = previous.Value;
                if (gapData.Range.WithinNextRange(to))
                    continue;

                yield return (key, previous.Value, _current[key]);

                _previous.Remove(key);
                _pool.Free(previous.Value.Data);
            }
        }

        private object GetNameFromKey(object key)
        {
            if (key == NullBucket)
                return null;

            if (key is Document doc)
                return doc.Id;

            return key;
        }

        private void UpdatePrevious(object key, RangeGroup range, ITimeSeriesAggregation[] values)
        {
            _previous ??= new Dictionary<object, PreviousAggregation>();
            if (_previous.TryGetValue(key, out var result) == false)
            {
                result = _previous[key] = new PreviousAggregation();
            }
            else
            {
                _pool.Free(result.Data);
            }

            result.Data = values;
            result.Range = range;
        }

        private object Clone(object value)
        {
            if (value == null || value == NullBucket)
                return NullBucket;

            return value switch
            {
                LazyStringValue lsv => lsv.ToString(),
                string s => s,
                DateTime time => time.ToString("O"),
                DateTimeOffset offset => offset.ToString("O"),
                TimeSpan span => span.ToString("c"),
                ValueType _ => value,
                LazyCompressedStringValue lcsv => lcsv.ToString(),
                LazyNumberValue lnv => lnv.ToDouble(CultureInfo.InvariantCulture),
                BlittableJsonReaderObject json => json.CloneOnTheSameContext(),
                BlittableJsonReaderArray arr => arr.Clone(_context),
                Document doc => doc,
                _ => throw new NotSupportedException($"Unable to group by type: {value.GetType()}")
            };
        }

        internal sealed class PreviousAggregation
        {
            public RangeGroup Range;

            public ITimeSeriesAggregation[] Data;
        }

        private IEnumerable<DynamicJsonValue> FillMissingGaps(RangeGroup range, double? scale)
        {
            var to = range.Start;

            foreach (var result in GetGapsPerBucket(to))
            {
                var gapData = result.Previous;

                var from = gapData.Range.Start; // we have this point
                gapData.Range.MoveToNextRange();

                var start = gapData.Range.Start; // this one we miss
                var end = gapData.Range.End;

                var startData = result.Previous.Data;
                var endData = result.Current;

                Debug.Assert(start < to, "Invalid gap data");

                var point = start;

                switch (_interpolationType)
                {
                    case InterpolationType.Linear:
                        
                        Debug.Assert(startData.Length == endData.Length, "Invalid aggregation stats");

                        var numberOfAggregations = startData.Length;
                        var initial = new List<double>[numberOfAggregations];
                        var final = new List<double>[numberOfAggregations];
                        for (int i = 0; i < numberOfAggregations; i++)
                        {
                            Debug.Assert(startData[i].Aggregation == endData[i].Aggregation, "Invalid aggregation type");
                            initial[i] = new List<double>(startData[i].GetFinalValues(from, start, scale));
                            final[i] = new List<double>(endData[i].GetFinalValues(to, range.End ,scale));
                        }

                        var numberOfValues = Math.Min(startData[0].NumberOfValues, endData[0].NumberOfValues);
                        var interpolated = new double[numberOfValues];

                        while (start < to)
                        {
                            var gap = new DynamicJsonValue
                            {
                                [nameof(TimeSeriesRangeAggregation.From)] = start,
                                [nameof(TimeSeriesRangeAggregation.To)] = end,
                                [nameof(TimeSeriesRangeAggregation.Key)] = GetNameFromKey(result.Key)
                            };

                            var quotient = (double)(point.Ticks - from.Ticks) / (to.Ticks - from.Ticks);
                            for (int i = 0; i < startData.Length; i++)
                            {
                                LinearInterpolation(quotient, initial[i], final[i], interpolated);
                                gap[startData[i].Name] = new DynamicJsonArray(interpolated.Cast<object>());
                            }
                            
                            yield return gap;

                            gapData.Range.MoveToNextRange();
                            start = gapData.Range.Start;
                            end = gapData.Range.End;

                            point = start;
                        }

                        break;
                    case InterpolationType.Nearest:

                        while (start < to)
                        {
                            var nearest = point - from <= to - point
                                ? startData
                                : endData;

                            yield return ToJson(scale, start, end, result.Key, nearest);

                            gapData.Range.MoveToNextRange();
                            start = gapData.Range.Start;
                            end = gapData.Range.End;

                            point = start;
                        }

                        break;
                    case InterpolationType.Last:

                        while (start < to)
                        {
                            yield return ToJson(scale, start, end, result.Key, startData);

                            gapData.Range.MoveToNextRange();
                            start = gapData.Range.Start;
                            end = gapData.Range.End;

                            point = start;
                        }

                        break;
                    case InterpolationType.Next:

                        while (start < to)
                        {
                            yield return ToJson(scale, start, end, result.Key, endData);

                            gapData.Range.MoveToNextRange();
                            start = gapData.Range.Start;
                            end = gapData.Range.End;

                            point = start;
                        }

                        break;    
                    default:
                        throw new ArgumentOutOfRangeException("Unknown InterpolationType : " + _interpolationType);
                }
            }
        }

        private static void LinearInterpolation(double quotient, List<double> valuesA, List<double> valuesB, double[] result)
        {
            var minLength = Math.Min(valuesA.Count, valuesB.Count);
            if (minLength < valuesA.Count)
            {
                valuesA.RemoveRange(minLength - 1, valuesA.Count - minLength);
            }

            for (var index = 0; index < minLength; index++)
            {
                var yb = valuesB[index];
                var ya = valuesA[index];

                // y = yA + (yB - yA) * ((x - xa) / (xb - xa))
                result[index] = ya + (yb - ya) * quotient;
            }
        }

        private struct TimeSeriesAggregationReset : IResetSupport<ITimeSeriesAggregation[]>
        {
            public void Reset(ITimeSeriesAggregation[] values)
            {
                for (var index = 0; index < values.Length; index++)
                {
                    values[index].Clear();
                }
            }
        }
    }
}
