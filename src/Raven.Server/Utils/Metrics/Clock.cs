﻿using System.Diagnostics;

namespace Raven.Server.Utils.Metrics
{
    internal sealed class Clock
    {
        public const int NanosecondsInSecond = 1000 * 1000 * 1000;
        public const int NanosecondsInMillisecond = 1000 * 1000;

        public static readonly long FrequencyFactor = (1000L * 1000L * 1000L) / Stopwatch.Frequency;
        public static long Nanoseconds => Stopwatch.GetTimestamp() * FrequencyFactor;
        public static long Milliseconds => Stopwatch.GetTimestamp() * FrequencyFactor / NanosecondsInMillisecond;
    }
}
