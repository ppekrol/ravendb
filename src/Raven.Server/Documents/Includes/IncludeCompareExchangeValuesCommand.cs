﻿using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Includes
{
    internal sealed class IncludeCompareExchangeValuesCommand : ICompareExchangeValueIncludes, IDisposable
    {
        private readonly ServerStore _serverStore;
        private readonly AbstractCompareExchangeStorage _compareExchangeStorage;
        private readonly char _identityPartsSeparator;

        private readonly string[] _includes;

        private HashSet<string> _includedKeys;

        private IDisposable _releaseContext;
        private ClusterOperationContext _serverContext;
        private readonly bool _throwWhenServerContextIsAllocated;
        public Dictionary<string, CompareExchangeValue<BlittableJsonReaderObject>> Results { get; set; }

        private IncludeCompareExchangeValuesCommand([NotNull] DocumentDatabase database, ClusterOperationContext serverContext, bool throwWhenServerContextIsAllocated, string[] compareExchangeValues)
            : this(database.ServerStore, database.CompareExchangeStorage, database.IdentityPartsSeparator, serverContext, throwWhenServerContextIsAllocated, compareExchangeValues)
        {
        }

        private IncludeCompareExchangeValuesCommand(ServerStore serverStore, AbstractCompareExchangeStorage compareExchangeStorage, char identityPartsSeparator, ClusterOperationContext serverContext, bool throwWhenServerContextIsAllocated, string[] compareExchangeValues)
        {
            _identityPartsSeparator = identityPartsSeparator;
            _serverStore = serverStore;
            _compareExchangeStorage = compareExchangeStorage;

            _serverContext = serverContext;
            _throwWhenServerContextIsAllocated = throwWhenServerContextIsAllocated;
            _includes = compareExchangeValues;
        }

        public static IncludeCompareExchangeValuesCommand ExternalScope(QueryOperationContext context, string[] compareExchangeValues)
        {
            return new IncludeCompareExchangeValuesCommand(context.Documents.DocumentDatabase, context.Server, throwWhenServerContextIsAllocated: true, compareExchangeValues);
        }

        public static IncludeCompareExchangeValuesCommand InternalScope(DocumentDatabase database, string[] compareExchangeValues)
        {
            return new IncludeCompareExchangeValuesCommand(database, serverContext: null, throwWhenServerContextIsAllocated: false, compareExchangeValues);
        }

        internal void AddRange(HashSet<string> keys)
        {
            if (keys == null)
                return;

            if (_includedKeys == null)
            {
                _includedKeys = new HashSet<string>(keys, StringComparer.OrdinalIgnoreCase);
                return;
            }

            foreach (var key in keys)
                _includedKeys.Add(key);
        }

        internal void Gather(Document document)
        {
            if (document == null)
                return;

            if (_includes == null || _includes.Length == 0)
                return;

            _includedKeys ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var include in _includes)
                IncludeUtil.GetDocIdFromInclude(document.Data, new StringSegment(include), _includedKeys, _identityPartsSeparator);
        }

        internal void Materialize()
        {
            if (_includedKeys == null || _includedKeys.Count == 0)
                return;

            if (_serverContext == null)
            {
                if (_throwWhenServerContextIsAllocated)
                    throw new InvalidOperationException("Cannot allocate new server context during materialization of compare exchange includes.");

                _releaseContext = _serverStore.Engine.ContextPool.AllocateOperationContext(out _serverContext);
                _serverContext.OpenReadTransaction();
            }

            foreach (var includedKey in _includedKeys)
            {
                if (string.IsNullOrEmpty(includedKey))
                    continue;

                var value = _compareExchangeStorage.GetCompareExchangeValue(_serverContext, includedKey);

                Results ??= new Dictionary<string, CompareExchangeValue<BlittableJsonReaderObject>>(StringComparer.OrdinalIgnoreCase);

                Results.Add(includedKey, new CompareExchangeValue<BlittableJsonReaderObject>(includedKey, value.Index, value.Value));
            }
        }

        public void Dispose()
        {
            _releaseContext?.Dispose();
            _releaseContext = null;
        }
    }
}
