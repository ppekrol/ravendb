using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using JetBrains.Annotations;
using Raven.Server.Config;
using Sparrow.Json.Parsing;
using Sparrow.LowMemory;

namespace Raven.Server.Documents.Patch
{
    internal sealed class ScriptRunnerCache : ILowMemoryHandler
    {
        internal static string PolyfillJs;

        private long _generation;

        private RavenConfiguration _configuration;

        private readonly ConcurrentDictionary<Key, Lazy<ScriptRunner>> _cache = new();

        public bool EnableClr;

        public readonly DocumentDatabase Database;

        public RavenConfiguration Configuration => _configuration;

        public long Generation => _generation;

        static ScriptRunnerCache()
        {
            using (Stream stream = typeof(ScriptRunnerCache).Assembly.GetManifestResourceStream("Raven.Server.Documents.Patch.Polyfill.js"))
            {
                using (var reader = new StreamReader(stream))
                {
                    PolyfillJs = reader.ReadToEnd();
                }
            }
        }

        public ScriptRunnerCache(DocumentDatabase database, [NotNull] RavenConfiguration configuration)
        {
            Database = database;
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));

            LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);
        }

        public int NumberOfCachedScripts
        {
            get
            {
                return _cache.Values
                    .Select(x => x.IsValueCreated ? x.Value : null)
                    .Where(x => x != null)
                    .Sum(x => x.NumberOfCachedScripts);
            }
        }

        public IEnumerable<DynamicJsonValue> GetDebugInfo(bool detailed = false)
        {
            foreach (var item in _cache)
            {
                yield return item.Value.Value.GetDebugInfo(detailed);
            }
        }

        internal abstract class Key
        {
            public abstract void GenerateScript(ScriptRunner runner);

            public abstract override bool Equals(object obj);

            public abstract override int GetHashCode();
        }

        public ScriptRunner.ReturnRun GetScriptRunner(Key key, bool readOnly, out ScriptRunner.SingleRun patchRun)
        {
            return GetScriptRunner(key, readOnly, ignoreValidationErrors: false, out patchRun);
        }

        public ScriptRunner.ReturnRun GetScriptRunner(Key key, bool readOnly, bool ignoreValidationErrors, out ScriptRunner.SingleRun patchRun)
        {
            if (key == null)
            {
                patchRun = null;
                return new ScriptRunner.ReturnRun();
            }
            var scriptRunner = GetScriptRunner(key);
            var returnRun = scriptRunner.GetRunner(ignoreValidationErrors, out patchRun);
            patchRun.ReadOnly = readOnly;
            return returnRun;
        }

        private ScriptRunner GetScriptRunner(Key script)
        {
            if (_cache.TryGetValue(script, out var lazy))
                return lazy.Value;

            return GetScriptRunnerUnlikely(script);
        }

        private ScriptRunner GetScriptRunnerUnlikely(Key script)
        {
            var value = new Lazy<ScriptRunner>(() =>
            {
                var runner = new ScriptRunner(this, EnableClr);
                script.GenerateScript(runner);
                runner.ScriptType = script.GetType().Name;
                return runner;
            });
            return _cache.GetOrAdd(script, value).Value;
        }

        public void UpdateConfiguration(RavenConfiguration configuration)
        {
            _configuration = configuration;
            Interlocked.Increment(ref _generation);
        }

        public void LowMemory(LowMemorySeverity lowMemorySeverity)
        {
            _cache.Clear();
        }

        public void LowMemoryOver()
        {
        }

        public void RunIdleOperations()
        {
            foreach (var (key, lazyRunner) in _cache)
            {
                if (lazyRunner.IsValueCreated == false)
                    continue;
                if (lazyRunner.Value.RunIdleOperations() == false)
                    _cache.TryRemove(key, out _);
            }
        }
    }
}
