// -----------------------------------------------------------------------
//  <copyright file="IndexesMetrics.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Sparrow.Json.Parsing;

namespace Raven.Server.Utils.Monitoring
{
    internal sealed class IndexMetrics
    {
        public string IndexName { get; set; }
        public IndexPriority Priority { get; set; }
        public IndexState State { get; set; }
        public int Errors { get; set; }
        public double? TimeSinceLastQueryInSec { get; set; }
        public double? TimeSinceLastIndexingInSec { get; set; }
        public IndexLockMode LockMode { get; set; }
        public bool IsInvalid { get; set; }
        public IndexRunningStatus Status { get; set; }
        public double MappedPerSec { get; set; }
        public double ReducedPerSec { get; set; }
        public IndexType Type { get; set; }
        public long EntriesCount { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(IndexName)] = IndexName,
                [nameof(Priority)] = Priority,
                [nameof(State)] = State,
                [nameof(Errors)] = Errors,
                [nameof(TimeSinceLastQueryInSec)] = TimeSinceLastQueryInSec,
                [nameof(TimeSinceLastIndexingInSec)] = TimeSinceLastIndexingInSec,
                [nameof(LockMode)] = LockMode,
                [nameof(IsInvalid)] = IsInvalid,
                [nameof(Status)] = Status,
                [nameof(MappedPerSec)] = MappedPerSec,
                [nameof(ReducedPerSec)] = ReducedPerSec,
                [nameof(Type)] = Type
            };
        }
    }
    
    internal sealed class IndexesMetrics
    {
        public List<PerDatabaseIndexMetrics> Results { get; set; } = new List<PerDatabaseIndexMetrics>();
        
        public string PublicServerUrl { get; set; }
        
        public string NodeTag { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(PublicServerUrl)] = PublicServerUrl,
                [nameof(NodeTag)] = NodeTag,
                [nameof(Results)] = Results.Select(x => x.ToJson()).ToList()
            };
        }
    }

    internal sealed class PerDatabaseIndexMetrics
    {
        public string DatabaseName { get; set; }
        public List<IndexMetrics> Indexes { get; set; } = new List<IndexMetrics>();

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(DatabaseName)] = DatabaseName,
                [nameof(Indexes)] = Indexes.Select(x => x.ToJson()).ToList()
            };
        }
    }
}
