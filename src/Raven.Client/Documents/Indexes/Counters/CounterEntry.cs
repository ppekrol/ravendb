﻿namespace Raven.Client.Documents.Indexes.Counters
{
    public sealed class CounterEntry
    {
        public string DocumentId { get; set; }

        public string Name { get; set; }

        public long Value { get; set; }
    }
}
