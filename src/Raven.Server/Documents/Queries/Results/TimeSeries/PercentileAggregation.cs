﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Server.Documents.TimeSeries;

namespace Raven.Server.Documents.Queries.Results.TimeSeries
{
    internal sealed class PercentileAggregation : TimeSeriesAggregationBase, ITimeSeriesAggregation
    {
        private readonly double _percentileFactor;
        private readonly List<SortedDictionary<double, int>> _rankedValues;
        private readonly List<long> _count;

        public new int NumberOfValues => _count.Count;

        public PercentileAggregation(string name = null, double? percentile = null) : base(AggregationType.Percentile, name)
        {
            if (percentile.HasValue == false)
                throw new ArgumentException(nameof(percentile));

            if (percentile.Value <= 0 || percentile.Value > 100)
                throw new ArgumentOutOfRangeException(
                    $"Invalid argument passed to '{nameof(AggregationType.Percentile)}' aggregation method: '{percentile}'. " +
                    "Argument must be a number between 0 and 100");

            _percentileFactor = percentile.Value / 100;
            _rankedValues = new List<SortedDictionary<double, int>>();
            _count = new List<long>();
        }

        void ITimeSeriesAggregation.Segment(Span<StatefulTimestampValue> values, bool isRaw)
        {
            throw new InvalidOperationException($"Cannot use method '{nameof(ITimeSeriesAggregation.Segment)}' on aggregation type '{nameof(AggregationType.Percentile)}' ");
        }

        void ITimeSeriesAggregation.Step(Span<double> values, bool isRaw)
        {
            if (isRaw == false)
                throw new InvalidOperationException($"Cannot use aggregation method '{nameof(AggregationType.Percentile)}' on rolled-up time series");

            InitValuesIfNeeded(values.Length);

            for (int i = 0; i < values.Length; i++)
            {
                var val = values[i];
                var sortedDict = _rankedValues[i];
                
                sortedDict.TryGetValue(val, out int valCount);
                sortedDict[val] = valCount + 1;

                _count[i]++;
            }
        }

        IEnumerable<double> ITimeSeriesAggregation.GetFinalValues(DateTime? from, DateTime? to, double? scale)
        {
            if (_finalValues != null)
                return _finalValues;

            return _finalValues = GetPercentile(scale ?? 1);
        }

        void ITimeSeriesAggregation.Clear()
        {
            _rankedValues.Clear();
            _count.Clear();
            Clear();
        }

        private void InitValuesIfNeeded(int upTo)
        {
            for (int i = _rankedValues.Count; i < upTo; i++)
            {
                _rankedValues.Add(new SortedDictionary<double, int>());
                _count.Add(0);
            }
        }

        private IEnumerable<double> GetPercentile(double scale)
        {
            // x = p(N+1) (except for edge cases)
            // f = floor(x), c = ceil(x)
            // v[K] = value of the 'Kth' item in sorted values

            // result = v[f] + ((x % 1) * (v[c] - v[f]))

            for (var i = 0; i < _rankedValues.Count; i++)
            {
                var sortedDict = _rankedValues[i];
                var itemsCount = _count[i];
                var n1 = itemsCount + 1;
                double remainder = 0;
                long rank;

                if (_percentileFactor <= 1d / n1)
                {
                    // edge case
                    rank = 1;
                }
                else if (_percentileFactor >= (double)itemsCount / n1)
                {
                    // edge case
                    rank = itemsCount;
                }
                else
                {
                    // general case
                    var x = _percentileFactor * n1;
                    rank = (long)Math.Floor(x);
                    remainder = x % 1;
                }

                // find v[rank], v[rank + 1] (if needed)
                var count = 0; 
                double? floorVal = null, ceilVal = null;
                foreach ((double val, int valCount) in sortedDict)
                {
                    if (floorVal.HasValue)
                    {
                        // found v[rank +1]
                        ceilVal = val;
                        break;
                    }

                    count += valCount;
                    if (count < rank) 
                        continue;

                    // found v[rank]
                    floorVal = val;
                    if (remainder == 0)
                        break;

                    if (count == rank)
                        continue;
                     
                    // floor and ceil have the same value
                    // no need for linear interpolation addition 
                    remainder = 0;
                    break;
                }

                Debug.Assert(floorVal.HasValue);
                var result = floorVal.Value;
                
                if (remainder != 0)
                {
                    Debug.Assert(ceilVal.HasValue);
                    result += remainder * (ceilVal.Value - floorVal.Value);
                }

                yield return scale * result;
            }
        }
    }
}
