﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Config;

namespace Raven.Server.Documents.Indexes.Static
{
    /// <summary>
    /// This is a static class because creating indexes is expensive, we want to cache them
    /// as much as possible, even across different databases and database instantiation. Per process,
    /// we are going to have a single cache for all indexes. This also plays nice with testing, which
    /// will build up and tear down a server frequently, so we can still reduce the cost of compiling
    /// the indexes.
    /// </summary>
    internal static class IndexCompilationCache
    {
        private static readonly ConcurrentDictionary<CacheKey, Lazy<AbstractStaticIndexBase>> _indexCache = new ConcurrentDictionary<CacheKey, Lazy<AbstractStaticIndexBase>>();

        public static AbstractStaticIndexBase GetIndexInstance(IndexDefinition definition, RavenConfiguration configuration, long indexVersion)
        {
            var type = definition.DetectStaticIndexType();
            if (type.IsJavaScript())
                return GenerateIndex(definition, configuration, type, indexVersion);

            switch (definition.SourceType)
            {
                case IndexSourceType.Documents:
                    return GetDocumentsIndexInstance(definition, configuration, type, indexVersion);

                case IndexSourceType.TimeSeries:
                    return GetIndexInstance<StaticTimeSeriesIndexBase>(definition, configuration, type, indexVersion);

                case IndexSourceType.Counters:
                    return GetIndexInstance<StaticCountersIndexBase>(definition, configuration, type, indexVersion);

                default:
                    throw new NotSupportedException($"Not supported source type '{definition.SourceType}'.");
            }
        }

        private static StaticIndexBase GetDocumentsIndexInstance(IndexDefinition definition, RavenConfiguration configuration, IndexType type, long indexVersion)
        {
            var key = GetCacheKey(definition);

            Lazy<AbstractStaticIndexBase> result = _indexCache.GetOrAdd(key, _ => new Lazy<AbstractStaticIndexBase>(() => GenerateIndex(definition, configuration, type, indexVersion)));

            try
            {
                return (StaticIndexBase)result.Value;
            }
            catch (Exception)
            {
                _indexCache.TryRemove(key, out _);
                throw;
            }
        }

        private static TIndexBase GetIndexInstance<TIndexBase>(IndexDefinition definition, RavenConfiguration configuration, IndexType type, long indexVersion)
            where TIndexBase : AbstractStaticIndexBase
        {
            var key = GetCacheKey(definition);

            Lazy<AbstractStaticIndexBase> result = _indexCache.GetOrAdd(key, _ => new Lazy<AbstractStaticIndexBase>(() => GenerateIndex(definition, configuration, type, indexVersion)));

            try
            {
                return (TIndexBase)result.Value;
            }
            catch (Exception)
            {
                _indexCache.TryRemove(key, out _);
                throw;
            }
        }

        private static CacheKey GetCacheKey(IndexDefinition definition)
        {
            var list = new List<string>();

            list.AddRange(definition.Maps);

            if (definition.Reduce != null)
                list.Add(definition.Reduce);

            if (definition.AdditionalSources != null)
            {
                foreach (var kvp in definition.AdditionalSources.OrderBy(x => x.Key))
                {
                    list.Add(kvp.Key);
                    list.Add(kvp.Value);
                }
            }

            if (definition.AdditionalAssemblies != null)
            {
                foreach (var additionalAssembly in definition.AdditionalAssemblies)
                {
                    if (additionalAssembly.AssemblyName != null)
                        list.Add(additionalAssembly.AssemblyName);

                    if (additionalAssembly.AssemblyPath != null)
                        list.Add(additionalAssembly.AssemblyPath);

                    if (additionalAssembly.PackageName != null)
                        list.Add(additionalAssembly.PackageName);

                    if (additionalAssembly.PackageVersion != null)
                        list.Add(additionalAssembly.PackageVersion);

                    if (additionalAssembly.PackageSourceUrl != null)
                        list.Add(additionalAssembly.PackageSourceUrl);

                    if (additionalAssembly.Usings != null && additionalAssembly.Usings.Count > 0)
                        list.AddRange(additionalAssembly.Usings);
                }
            }

            return new CacheKey(list);
        }

        internal static AbstractStaticIndexBase GenerateIndex(IndexDefinition definition, RavenConfiguration configuration, IndexType type, long indexVersion)
        {
            AbstractStaticIndexBase index;
            switch (type)
            {
                case IndexType.None:
                case IndexType.AutoMap:
                case IndexType.AutoMapReduce:
                case IndexType.Map:
                case IndexType.MapReduce:
                case IndexType.Faulty:
                    index = IndexCompiler.Compile(definition);
                    break;
                case IndexType.JavaScriptMap:
                case IndexType.JavaScriptMapReduce:
                    index = AbstractJavaScriptIndex.Create(definition, configuration, indexVersion);
                    break;
                default:
                    throw new ArgumentOutOfRangeException($"Can't generate index of unknown type {definition.DetectStaticIndexType()}");
            }
            

            return index;
        }

        private sealed class CacheKey : IEquatable<CacheKey>
        {
            private readonly int _hash;
            private readonly List<string> _items;

            public CacheKey(List<string> items)
            {
                _items = items;

                var hasher = new HashCode();
                foreach (var item in items)
                {
                    hasher.Add(item);
                }

                _hash = hasher.ToHashCode();
            }

            public override bool Equals(object obj)
            {
                if (obj is CacheKey cacheKey)
                    return Equals(cacheKey);
                return false;
            }

            public bool Equals(CacheKey other)
            {
                if (_items.Count != other._items.Count)
                    return false;
                for (int i = 0; i < _items.Count; i++)
                {
                    if (_items[i] != other._items[i])
                        return false;
                }
                return true;
            }

            public override int GetHashCode()
            {
                return _hash;
            }
        }
    }
}
