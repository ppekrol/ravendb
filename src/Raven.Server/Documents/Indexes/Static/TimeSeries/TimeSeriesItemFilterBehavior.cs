using System.Collections.Generic;

namespace Raven.Server.Documents.Indexes.Static.TimeSeries
{
    public class TimeSeriesItemFilterBehavior : IIndexItemFilterBehavior
    {
        private readonly Dictionary<string, IndexItem> _seenIds = new Dictionary<string, IndexItem>();

        public bool ShouldFilter(IndexItem item)
        {
            if (_seenIds.TryGetValue(item.LowerId, out var i))
            {

            }

            _seenIds.Add(item.LowerId, item);

            return false;
        }
    }
}
