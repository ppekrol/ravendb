﻿using System;
using System.Collections.Generic;
using Raven.Client.Util;

namespace Raven.Server.Documents.ETL.Stats
{
    internal sealed class EtlPerformanceStats
    {
        public EtlPerformanceStats(TimeSpan duration)
        {
            DurationInMs = Math.Round(duration.TotalMilliseconds, 2);
        }

        public int Id { get; set; }

        public DateTime Started { get; set; }

        public DateTime? Completed { get; set; }

        public double DurationInMs { get; }

        public EtlPerformanceOperation Details { get; set; }

        public long LastLoadedEtag { get; set; }

        public int NumberOfLoadedItems { get; set; }

        public Dictionary<EtlItemType, long> LastExtractedEtags { get; set; }

        public Dictionary<EtlItemType, long> LastTransformedEtags { get; set; }

        public Dictionary<EtlItemType, long> LastFilteredOutEtags { get; set; }

        public Dictionary<EtlItemType, int> NumberOfExtractedItems { get; set; }

        public Dictionary<EtlItemType, int> NumberOfTransformedItems { get; set; }

        public Dictionary<EtlItemType, int> NumberOfTransformedTombstones { get; set; }

        public Size CurrentlyAllocated { get; set; }

        public Size BatchSize { get; set; }

        public string BatchTransformationCompleteReason { get; set; }

        public string BatchStopReason { get; set; }

        public int TransformationErrorCount { get; set; }

        public bool? SuccessfullyLoaded { get; set; }
    }
}
