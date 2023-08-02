using System;
using Lucene.Net.Search;

namespace Raven.Server.Documents.Queries.Sorting.AlphaNumeric
{
    internal sealed class RandomSortField : SortField
    {
        public RandomSortField(string field) : base(field ?? "RandomValue-" + Guid.NewGuid(), INT)
        {
        }

        public override FieldComparator GetComparator(int numHits, int sortPos)
        {
            // sortPost and reversed are ignored by the RandomFieldComparator
            return ComparatorSource.NewComparator(Field, numHits, sortPos, reversed: false);
        }

        public override FieldComparatorSource ComparatorSource => RandomFieldComparatorSource.Instance;
    }
}
