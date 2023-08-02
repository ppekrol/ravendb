using System;
using System.Collections.Generic;
using System.Text;
using Sparrow;
using Raven.Client;

namespace Raven.Server.Documents.Queries.AST
{
    internal sealed class Query
    {
        public bool IsDistinct;
        public QueryExpression Where;
        public QueryExpression Filter;
        public FromClause From;
        public List<(QueryExpression Expression, StringSegment? Alias)> Select;
        public List<(QueryExpression Expression, StringSegment? Alias)> Load;
        public List<QueryExpression> Include;
        public List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending)> OrderBy;
        public List<(QueryExpression Expression, StringSegment? Alias)> GroupBy;

        public Dictionary<string, DeclaredFunction> DeclaredFunctions;

        public string QueryText;
        public (string FunctionText, Esprima.Ast.Program Program, HashSet<string> ReferencedParameters) SelectFunctionBody;
        public string UpdateBody;
        public ValueExpression Offset;
        public ValueExpression Limit;
        public ValueExpression FilterLimit;

        public List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending)> CachedOrderBy;

        public Query ShallowCopy()
        {
            return (Query)MemberwiseClone();
        }
        
        public bool TryAddFunction(DeclaredFunction func)
        {
            if (DeclaredFunctions == null)
                DeclaredFunctions = new Dictionary<string, DeclaredFunction>(StringComparer.OrdinalIgnoreCase);

            return DeclaredFunctions.TryAdd(func.Name, func);
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            new StringQueryVisitor(sb).Visit(this);
            return sb.ToString();
        }

        public bool TryAddTimeSeriesFunction(DeclaredFunction func)
        {
            if (DeclaredFunctions == null)
                DeclaredFunctions = new Dictionary<string, DeclaredFunction>(StringComparer.OrdinalIgnoreCase);

            func.Name = Constants.TimeSeries.QueryFunction + DeclaredFunctions.Count;

            return DeclaredFunctions.TryAdd(func.Name, func);
        }
    }

    internal sealed class DeclaredFunction
    {
        public string Name;
        public string FunctionText;
        public Esprima.Ast.Program JavaScript;
        public TimeSeriesFunction TimeSeries;
        public FunctionType Type;
        public List<QueryExpression> Parameters;

        public enum FunctionType
        {
            JavaScript,
            TimeSeries
        }
    }

    internal sealed class TimeSeriesFunction
    {
        public TimeSeriesBetweenExpression Between;
        public QueryExpression Source;
        public QueryExpression Where;
        public List<(QueryExpression QueryExpression, StringSegment? StringSegment)> Select;
        public StringSegment? LoadTagAs;
        public ValueExpression Last, First, Offset, Scale;
        public TimeSeriesGroupBy GroupBy;
    }

    internal struct TimeSeriesGroupBy
    {
        public ValueExpression TimePeriod;
        public bool Tag;
        public FieldExpression Field;

        public MethodExpression With;

        public bool HasGroupByTag => Tag || Field != null;
    }

    internal unsafe struct RangeGroup
    {
        public long Ticks;
        public int Months;
        public Alignment TicksAlignment;

        public enum Alignment
        {
            None,
            Millisecond,
            Second,
            Minute,
            Hour,
            Day
        }

        private const long TicksInMillisecond = 10_000;

        private long AlignedTicks
        {
            get
            {
                var ticks = TicksInMillisecond;
                switch (TicksAlignment)
                {
                    case Alignment.Day:
                        ticks *= 24;
                        goto case Alignment.Hour;

                    case Alignment.Hour:
                        ticks *= 60;
                        goto case Alignment.Minute;

                    case Alignment.Minute:
                        ticks *= 60;
                        goto case Alignment.Second;

                    case Alignment.Second:
                        ticks *= 1_000;
                        goto case Alignment.Millisecond;

                    case Alignment.Millisecond:
                        return ticks;

                    default:
                        throw new ArgumentOutOfRangeException($"Unknown type {TicksAlignment}");
                }
            }
        }

        private DateTime _origin;
        private DateTime _start;
        private DateTime _end;
        private bool _init;

        public DateTime Start => _start;
        public DateTime End => _end;

        private void AssertInit()
        {
            if (_init == false)
                throw new InvalidOperationException("You must initialize first.");
        }

        private void AssertNotInit()
        {
            if (_init)
                throw new InvalidOperationException("This range is already initialized.");
        }

        public void InitializeRange(DateTime origin)
        {
            AssertNotInit();

            if (Ticks != 0)
                InitByTicks(origin);
            else if (Months != 0)
                InitByMonth(origin);
            else
                throw new ArgumentException($"Either {nameof(Ticks)} or {nameof(Months)} should be set");

            _origin = _start;
            _init = true;
        }

        public void InitializeFullRange(DateTime start, DateTime end)
        {
            AssertNotInit();

            _start = start;
            _end = end;
            _init = true;
        }

        private void InitByTicks(DateTime first)
        {
            var ticks = first.Ticks;
            ticks -= (ticks % AlignedTicks);

            if (ticks > _start.Ticks)
            {
                var distance = ticks - _start.Ticks;
                var ticksToAdd = (distance / Ticks) * Ticks;
                _start = _start.AddTicks(ticksToAdd);
            }

            _start = DateTime.SpecifyKind(_start, first.Kind);
            _end = _start.AddTicks(Ticks);

        }

        private void InitByMonth(DateTime start)
        {
            var yearsPortion = Math.Max(1, Months / 12);
            var monthsRemaining = Months % 12;
            var year = start.Year - (start.Year % yearsPortion);
            int month = monthsRemaining == 0 ? 1 : ((start.Month - 1) / monthsRemaining * monthsRemaining) + 1;
            _start = new DateTime(year, month, 1, 0, 0, 0, start.Kind);
            _end = _start.AddMonths(Months);
        }

        public void MoveToNextRange(DateTime timestamp)
        {
            AssertInit();

            if (timestamp < _end)
                throw new InvalidOperationException($"The time '{timestamp}' is smaller then the end of current range");

            if (Ticks != 0)
            {
                var distance = timestamp - _origin;
                _start = _origin.AddTicks((distance.Ticks / Ticks) * Ticks);
                _end = _start.AddTicks(Ticks);
                return;
            }

            if (Months != 0)
            {
                InitByMonth(timestamp);
            }
        }

        public void MoveToNextRange()
        {
            MoveToNextRange(_end);
        }

        public bool WithinRange(DateTime timestamp)
        {
            if (_init == false)
                InitializeRange(timestamp);

            return timestamp >= _start && timestamp < _end;
        }

        public bool WithinNextRange(DateTime timestamp)
        {
            var copy = this; // this a struct, and we don't want to affect the state of the current one
            copy.MoveToNextRange(copy.End);
            return copy.WithinRange(timestamp);
        }

        public static RangeGroup ParseRangeFromString(string s, DateTime? from = null)
        {
            var range = new RangeGroup();
            var offset = 0;

            var duration = ParseNumber(s, ref offset);
            ParseRange(s, ref offset, ref range, duration);

            while (offset < s.Length && char.IsWhiteSpace(s[offset]))
            {
                offset++;
            }

            if (offset != s.Length)
                throw new ArgumentException("After range specification, found additional unknown data: " + s);

            if (from != null && range.Ticks != 0)
            {
                var ticks = from.Value.Ticks;
                ticks -= (ticks % range.AlignedTicks);
                range._start = new DateTime(ticks);
            }

            return range;
        }

        private const int TicksInOneSecond = 10_000_000;

        private static void ParseRange(string source, ref int offset, ref RangeGroup range, long duration)
        {
            if (offset >= source.Length)
                throw new ArgumentException("Unable to find range specification in: " + source);

            while (char.IsWhiteSpace(source[offset]) && offset < source.Length)
            {
                offset++;
            }

            if (offset >= source.Length)
                throw new ArgumentException("Unable to find range specification in: " + source);

            switch (char.ToLower(source[offset++]))
            {
                case 's':
                    if (TryConsumeMatch(source, ref offset, "seconds") == false)
                        TryConsumeMatch(source, ref offset, "second");

                    range.Ticks += duration * TicksInOneSecond;
                    range.TicksAlignment = Alignment.Second;
                    return;
                case 'm':
                    if (TryConsumeMatch(source, ref offset, "minutes") ||
                        TryConsumeMatch(source, ref offset, "minute") ||
                        TryConsumeMatch(source, ref offset, "min"))
                    {
                        range.Ticks += duration * TicksInOneSecond * 60;
                        range.TicksAlignment = Alignment.Minute;
                        return;
                    }

                    if (TryConsumeMatch(source, ref offset, "ms") ||
                        TryConsumeMatch(source, ref offset, "milliseconds") ||
                        TryConsumeMatch(source, ref offset, "milli"))
                    {
                        range.Ticks += duration * TicksInMillisecond;
                        range.TicksAlignment = Alignment.Millisecond;
                        return;
                    }

                    if (TryConsumeMatch(source, ref offset, "months") ||
                        TryConsumeMatch(source, ref offset, "month") ||
                        TryConsumeMatch(source, ref offset, "mon") ||
                        TryConsumeMatch(source, ref offset, "mo"))
                    {
                        AssertValidDurationInMonths(duration);
                        range.Months += (int)duration;
                        return;
                    }
                    range.TicksAlignment = Alignment.Minute;
                    range.Ticks += duration * TicksInOneSecond * 60;
                    return;
                case 'h':
                    if (TryConsumeMatch(source, ref offset, "hours") == false)
                        TryConsumeMatch(source, ref offset, "hour");

                    range.Ticks += duration * TicksInOneSecond * 60 * 60;
                    range.TicksAlignment = Alignment.Hour;

                    return;
                case 'd':
                    if (TryConsumeMatch(source, ref offset, "days") == false)
                        TryConsumeMatch(source, ref offset, "day");
                    range.Ticks += duration * TicksInOneSecond * 60 * 60 * 24;
                    range.TicksAlignment = Alignment.Day;

                    return;
                case 'q':
                    if (TryConsumeMatch(source, ref offset, "quarters") == false)
                        TryConsumeMatch(source, ref offset, "quarter");
                    duration *= 3;
                    AssertValidDurationInMonths(duration);
                    range.Months += (int)duration;
                    return;

                case 'y':
                    if (TryConsumeMatch(source, ref offset, "years") == false)
                        TryConsumeMatch(source, ref offset, "year");
                    duration *= 12;
                    AssertValidDurationInMonths(duration);
                    range.Months += (int)duration;
                    return;
                default:
                    throw new ArgumentException($"Unable to understand time range: '{source}'");
            }
        }

        public static TimeValue ParseTimePeriodFromString(long duration, string source, ref int offset)
        {
            TimeValue result;

            while (char.IsWhiteSpace(source[offset]) && offset < source.Length)
            {
                offset++;
            }

            if (offset >= source.Length)
                throw new ArgumentException("Unable to find range specification in: " + source);

            switch (char.ToLower(source[offset++]))
            {
                case 's':
                    if (TryConsumeMatch(source, ref offset, "seconds") == false)
                        TryConsumeMatch(source, ref offset, "second");

                    result = TimeValue.FromSeconds((int)duration);
                    break;
                case 'm':
                    if (TryConsumeMatch(source, ref offset, "minutes") ||
                        TryConsumeMatch(source, ref offset, "minute") ||
                        TryConsumeMatch(source, ref offset, "min"))
                    {
                        result = TimeValue.FromMinutes((int)duration);
                        break;
                    }

                    if (TryConsumeMatch(source, ref offset, "ms") ||
                        TryConsumeMatch(source, ref offset, "milliseconds") ||
                        TryConsumeMatch(source, ref offset, "milli"))
                    {
                        // TODO use TimeValue.FromMilliseconds when RavenDB-14988 is fixed
                        throw new NotSupportedException("Unsupported time period. Using milliseconds in Last/First is not supported : " + source);
                    }

                    if (TryConsumeMatch(source, ref offset, "months") ||
                        TryConsumeMatch(source, ref offset, "month") ||
                        TryConsumeMatch(source, ref offset, "mon") ||
                        TryConsumeMatch(source, ref offset, "mo"))
                    {
                        result = TimeValue.FromMonths((int)duration);
                        break;
                    }
                    goto default;
                case 'h':
                    if (TryConsumeMatch(source, ref offset, "hours") == false)
                        TryConsumeMatch(source, ref offset, "hour");

                    result = TimeValue.FromHours((int)duration);
                    break;

                case 'd':
                    if (TryConsumeMatch(source, ref offset, "days") == false)
                        TryConsumeMatch(source, ref offset, "day");
                    result = TimeValue.FromDays((int)duration);
                    break;

                case 'q':
                    if (TryConsumeMatch(source, ref offset, "quarters") == false)
                        TryConsumeMatch(source, ref offset, "quarter");

                    duration *= 3;
                    AssertValidDurationInMonths(duration);
                    result = TimeValue.FromMonths((int)duration);
                    break;

                case 'y':
                    if (TryConsumeMatch(source, ref offset, "years") == false)
                        TryConsumeMatch(source, ref offset, "year");
                    duration *= 12;
                    AssertValidDurationInMonths(duration);
                    result = TimeValue.FromMonths((int)duration);
                    break;
                default:
                    throw new ArgumentException($"Unable to understand time range: '{source}'");
            }

            while (offset < source.Length && char.IsWhiteSpace(source[offset]))
            {
                offset++;
            }

            if (offset != source.Length)
                throw new ArgumentException("After range specification, found additional unknown data: " + source);

            return result;
        }

        public static TimeValue ParseTimePeriodFromString(string source, ref int offset)
        {
            var duration = ParseNumber(source, ref offset);
            if (offset >= source.Length)
                throw new ArgumentException("Unable to find range specification in: " + source);

            return ParseTimePeriodFromString(duration, source, ref offset);
        }

        private static void AssertValidDurationInMonths(long duration)
        {
            if (duration > 120_000)
                throw new ArgumentException("The specified range results in invalid range, cannot have: " + duration + " months");
        }

        private static bool TryConsumeMatch(string source, ref int offset, string additionalMatch)
        {
            if (source.Length <= offset)
                return false;

            if (new StringSegment(source, offset - 1, source.Length - offset + 1).StartsWith(additionalMatch, StringComparison.OrdinalIgnoreCase))
            {
                offset += additionalMatch.Length - 1;
                return true;
            }

            return false;
        }

        private static long ParseNumber(string source, ref int offset)
        {
            int i;
            for (i = offset; i < source.Length; i++)
            {
                if (char.IsWhiteSpace(source[i]) == false)
                    break;
            }

            int j;
            for (j = 0; i < source.Length; j++)
            {
                if (char.IsNumber(source[i + j]) == false)
                    break;
            }

            fixed (char* s = source)
            {
                if (long.TryParse(new ReadOnlySpan<char>(s + i, j), out var amount))
                {
                    offset = i + j;
                    return amount;
                }
            }

            throw new ArgumentException("Unable to parse: '" + source.Substring(offset) + "' as a number");
        }

        public TimeValue ToTimeValue()
        {
            if (Ticks != 0)
                return TimeValue.FromSeconds((int)(Ticks / 10_000_000));

            return TimeValue.FromMonths(Months);
        }
    }
}
