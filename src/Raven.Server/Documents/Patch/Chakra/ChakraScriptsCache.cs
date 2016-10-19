﻿using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Scripting.JavaScript;
using Raven.Abstractions;
using Raven.Abstractions.Json.Linq;

namespace Raven.Server.Documents.Patch.Chakra
{
    [CLSCompliant(false)]
    public class ChakraScriptsCache
    {
        private class CachedResult
        {
            public int Usage;
            public DateTime Timestamp;
            public JavaScriptEngine Engine;
        }

        private const int CacheMaxSize = 512;

        private readonly Dictionary<ScriptedPatchRequestAndCustomFunctionsToken, CachedResult> _cache = new Dictionary<ScriptedPatchRequestAndCustomFunctionsToken, CachedResult>();

        private class ScriptedPatchRequestAndCustomFunctionsToken
        {
            private readonly PatchRequest request;
            private readonly string customFunctions;

            public ScriptedPatchRequestAndCustomFunctionsToken(PatchRequest request, string customFunctions)
            {
                this.request = request;
                this.customFunctions = customFunctions;
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(this, obj)) return true;
                var other = obj as ScriptedPatchRequestAndCustomFunctionsToken;
                if (ReferenceEquals(null, other)) return false;
                if (request.Equals(other.request))
                {
                    if (customFunctions == null && other.customFunctions == null)
                        return true;
                    if (customFunctions != null && other.customFunctions != null)
                        return RavenJTokenEqualityComparer.Default.Equals(customFunctions, other.customFunctions);
                }
                return false;
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return ((request?.GetHashCode() ?? 0) * 397) ^
                        (customFunctions != null ? RavenJTokenEqualityComparer.Default.GetHashCode(customFunctions) : 0);
                }
            }
        }

        public JavaScriptEngine GetEngine(Func<PatchRequest, JavaScriptEngine> createEngine, PatchRequest request, string customFunctions)
        {
            CachedResult value;
            var patchRequestAndCustomFunctionsTuple = new ScriptedPatchRequestAndCustomFunctionsToken(request, customFunctions);
            if (_cache.TryGetValue(patchRequestAndCustomFunctionsTuple, out value))
            {
                value.Usage++;
                return value.Engine;
            }
            var result = createEngine(request);

            if (string.IsNullOrWhiteSpace(customFunctions) == false)
            {
                var custom = result.EvaluateScriptText(string.Format(
                    @"var customFunctions = function() {{  var exports = {{ }}; {0};
                            return exports;
                        }}();
                        for(var customFunction in customFunctions) {{
                            this[customFunction] = customFunctions[customFunction];
                        }};", customFunctions));
            }

            var cachedResult = new CachedResult
            {
                Usage = 1,
                Timestamp = SystemTime.UtcNow,
                Engine = result
            };

            if (_cache.Count > CacheMaxSize)
            {
                foreach (var item in _cache.OrderBy(x => x.Value?.Usage)
                    .ThenByDescending(x => x.Value?.Timestamp)
                    .Take(CacheMaxSize / 10)
                    .Select(source => source.Key)
                    .ToList())
                {
                    _cache.Remove(item);
                }
            }
            _cache[patchRequestAndCustomFunctionsTuple] = cachedResult;


            return result;
        }
    }
}
