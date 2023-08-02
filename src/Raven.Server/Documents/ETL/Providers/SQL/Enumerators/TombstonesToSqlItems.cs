﻿using System.Collections;
using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.SQL.Enumerators
{
    internal sealed class TombstonesToSqlItems : IEnumerator<ToSqlItem>
    {
        private readonly IEnumerator<Tombstone> _tombstones;
        private readonly string _collection;

        public TombstonesToSqlItems(IEnumerator<Tombstone> tombstones, string collection)
        {
            _tombstones = tombstones;
            _collection = collection;
        }

        private bool Filter()
        {
            var tombstone = _tombstones.Current;
            return tombstone.Type != Tombstone.TombstoneType.Document || tombstone.Flags.Contain(DocumentFlags.Artificial);
        }

        public bool MoveNext()
        {
            if (_tombstones.MoveNext() == false)
                return false;

            Current = new ToSqlItem(_tombstones.Current, _collection) {Filtered = Filter()};

            return true;
        }

        public void Reset()
        {
            throw new System.NotImplementedException();
        }

        object IEnumerator.Current => Current;

        public void Dispose()
        {
        }

        public ToSqlItem Current { get; private set; }
    }
}
