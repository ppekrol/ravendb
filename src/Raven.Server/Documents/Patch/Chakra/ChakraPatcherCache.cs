using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using Microsoft.Scripting;
using Microsoft.Scripting.JavaScript;
using Raven.Abstractions;
using Sparrow;

namespace Raven.Server.Documents.Patch.Chakra
{
    public class ChakraPatcherCache
    {
        private const int CacheMaxSize = 512;

        private readonly ConcurrentDictionary<CacheKey, CacheResult> _cache = new ConcurrentDictionary<CacheKey, CacheResult>();

        public CacheResult Get(PatchRequest patchRequest, string customFunctions)
        {
            var cacheKey = new CacheKey(patchRequest, customFunctions);
            CacheResult cacheResult;

            if (_cache.TryRemove(cacheKey, out cacheResult))
            {
                cacheResult.Usage++;
                return cacheResult;
            }

            var cachedResult = new CacheResult
            {
                Key = cacheKey,
                Usage = 1,
                Created = SystemTime.UtcNow,
                Patcher = CreatePatcher(patchRequest, customFunctions)
            };

            if (_cache.Count > CacheMaxSize)
            {
                foreach (var item in _cache
                    .OrderBy(x => x.Value?.Usage)
                    .ThenByDescending(x => x.Value?.Created)
                    .Take(CacheMaxSize / 10)
                    .Select(source => source.Key)
                    .ToList())
                {
                    _cache.TryRemove(item, out cacheResult);
                }
            }

            return cachedResult;
        }

        public void Return(CacheResult cacheResult)
        {
            _cache.AddOrUpdate(cacheResult.Key, cacheResult, (key, oldResult) =>
            {
                oldResult.Patcher.Dispose();
                return cacheResult;
            });
        }

        private static ChakraPatcher CreatePatcher(PatchRequest patchRequest, string customFunctions)
        {
            var runtime = new JavaScriptRuntime(new JavaScriptRuntimeSettings
            {
                AllowScriptInterrupt = true
            });

            var engine = runtime.CreateEngine();
            using (engine.AcquireContext())
            {
                AddScript(engine, "Raven.Server.Documents.Patch.lodash.js");
                //AddScript(engine, "Raven.Server.Documents.Patch.ToJson.js");
                AddScript(engine, "Raven.Server.Documents.Patch.RavenDB.js");

                JavaScriptFunction patchFn;
                try
                {
                    patchFn = (JavaScriptFunction)engine.Execute(new ScriptSource("Patch", string.Format("(() => (function() {{ {0} }}))()", patchRequest.Script)));
                }
                finally
                {
                    engine.AssertNoExceptions();
                }

                return new ChakraPatcher(runtime, engine, patchFn);
            }
        }

        private static void AddScript(JavaScriptEngine engine, string name)
        {
            try
            {
                engine.Execute(new ScriptSource(name, GetFromResources(name)));
            }
            finally
            {
                engine.AssertNoExceptions();
            }
        }

        private static string GetFromResources(string resourceName)
        {
            var assembly = typeof(PatchDocument).GetTypeInfo().Assembly;
            using (var stream = assembly.GetManifestResourceStream(resourceName))
            {
                using (var reader = new StreamReader(stream))
                {
                    return reader.ReadToEnd();
                }
            }
        }

        public class CacheKey
        {
            private readonly ulong _scriptHash;
            private readonly ulong _customFunctionsHash;

            public CacheKey(PatchRequest patchRequest, string customFunctions)
            {
                _scriptHash = Hashing.XXHash64.Calculate(patchRequest.Script, Encoding.UTF8);

                if (customFunctions != null)
                    _customFunctionsHash = Hashing.XXHash64.Calculate(customFunctions, Encoding.UTF8);
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != GetType()) return false;
                return Equals((CacheKey)obj);
            }

            private bool Equals(CacheKey other)
            {
                return _scriptHash == other._scriptHash && _customFunctionsHash == other._customFunctionsHash;
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return (_scriptHash.GetHashCode() * 397) ^ _customFunctionsHash.GetHashCode();
                }
            }
        }

        public class CacheResult
        {
            public CacheKey Key;
            public int Usage;
            public DateTime Created;
            public ChakraPatcher Patcher;
        }
    }
}