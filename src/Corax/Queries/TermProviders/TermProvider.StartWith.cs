using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Corax.Mappings;
using Voron;
using Voron.Data.CompactTrees;
using CompactTreeForwardIterator = Voron.Data.CompactTrees.CompactTree.Iterator<Voron.Data.Lookups.Lookup<Voron.Data.CompactTrees.CompactTree.CompactKeyLookup>.ForwardIterator>;

namespace Corax.Queries
{
    [DebuggerDisplay("{DebugView,nq}")]
    public struct StartWithTermProvider : ITermProvider
    {
        private readonly CompactTree _tree;
        private readonly IndexSearcher _searcher;
        private readonly FieldMetadata _field;
        private readonly CompactKey _startWith;

        private  CompactTreeForwardIterator _iterator;

        public StartWithTermProvider(IndexSearcher searcher, CompactTree tree, FieldMetadata field, CompactKey startWith)
        {
            _searcher = searcher;
            _field = field;
            _iterator = tree.Iterate();
            _startWith = startWith;
            _tree = tree;

            _iterator.Seek(_startWith);
        }

        public void Reset() => _iterator.Seek(_startWith);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Next(out TermMatch term)
        {
            var result = Next(out term, out var scope);
            scope.Dispose();
            return result;
        }

        public bool Next(out TermMatch term, out CompactKey compactKey)
        {
            if (_iterator.MoveNext(out compactKey, out var _) == false)
            {
                term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
                return false;
            }

            var key = compactKey.Decoded();
            if (key.StartsWith(_startWith.Decoded()) == false)
            {
                term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
                return false;
            }

            term = _searcher.TermQuery(_field, compactKey, _tree);
            return true;
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(StartWithTermProvider)}",
                            parameters: new Dictionary<string, string>()
                            {
                                { "Field", _field.ToString() },
                                { "Terms", _startWith.ToString()}
                            });
        }

        public string DebugView => Inspect().ToString();
    }
}
