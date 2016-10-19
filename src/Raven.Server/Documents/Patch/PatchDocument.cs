using System;
using Raven.Server.Documents.Patch.Chakra;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Voron.Exceptions;
using Sparrow.Logging;

namespace Raven.Server.Documents.Patch
{
    public class PatchDocument
    {
        private static Logger _logger;

        private static readonly ChakraPatcherCache Cache = new ChakraPatcherCache();

        private readonly DocumentDatabase _database;

        public PatchDocument(DocumentDatabase database)
        {
            _database = database;
            _logger = LoggingSource.Instance.GetLogger<PatchDocument>(database.Name);
        }

        public virtual PatchResultData Apply(DocumentsOperationContext context, Document document, PatchRequest patchRequest)
        {
            if (document == null)
                return null;

            if (string.IsNullOrEmpty(patchRequest.Script))
                throw new InvalidOperationException("Patch script must be non-null and not empty");

            var patchResult = ApplySingleScript(context, document, patchRequest);

            return new PatchResultData
            {
                ModifiedDocument = patchResult.Document ?? document.Data,
                DebugInfo = patchResult.DebugInfo,
            };
        }

        public unsafe PatchResultData Apply(DocumentsOperationContext context,
            string documentKey,
            long? etag,
            PatchRequest patch,
            PatchRequest patchIfMissing,
            bool isTestOnly = false,
            bool skipPatchIfEtagMismatch = false)
        {
            var document = _database.DocumentsStorage.Get(context, documentKey);
            if (_logger.IsInfoEnabled)
                _logger.Info(string.Format("Preparing to apply patch on ({0}). Document found?: {1}.", documentKey, document != null));

            if (etag.HasValue && document != null && document.Etag != etag.Value)
            {
                System.Diagnostics.Debug.Assert(document.Etag > 0);

                if (skipPatchIfEtagMismatch)
                {
                    return new PatchResultData
                    {
                        PatchResult = PatchResult.Skipped
                    };
                }

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Got concurrent exception while tried to patch the following document: {documentKey}");
                throw new ConcurrencyException($"Could not patch document '{documentKey}' because non current etag was used")
                {
                    ActualETag = document.Etag,
                    ExpectedETag = etag.Value,
                };
            }

            var patchRequest = patch;
            if (document == null)
            {
                if (patchIfMissing == null)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info("Tried to patch a not exists document and patchIfMissing is null");

                    return new PatchResultData
                    {
                        PatchResult = PatchResult.DocumentDoesNotExists
                    };
                }
                patchRequest = patchIfMissing;
            }
            var patchResult = ApplySingleScript(context, document, patchRequest);

            var result = new PatchResultData
            {
                PatchResult = PatchResult.NotModified,
                OriginalDocument = document?.Data,
                DebugInfo = patchResult.DebugInfo,
            };

            if (patchResult.Document == null)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info($"After applying patch, modifiedDocument is null and document is null? {document == null}");

                result.PatchResult = PatchResult.Skipped;
                return result;
            }

            if (isTestOnly)
            {
                return new PatchResultData
                {
                    PatchResult = PatchResult.Tested,
                    OriginalDocument = document?.Data,
                    ModifiedDocument = patchResult.Document,
                    DebugActions = patchResult.DebugActions.GetDebugActions(),
                    DebugInfo = patchResult.DebugInfo,
                };
            }

            if (document == null)
            {
                _database.DocumentsStorage.Put(context, documentKey, null, patchResult.Document);
            }
            else
            {
                var isModified = document.Data.Size != patchResult.Document.Size;
                if (isModified == false) // optimization, if size different, no need to compute hash to check
                {
                    var originHash = Hashing.XXHash64.Calculate(document.Data.BasePointer, (ulong)document.Data.Size);
                    var modifiedHash = Hashing.XXHash64.Calculate(patchResult.Document.BasePointer, (ulong)patchResult.Document.Size);
                    isModified = originHash != modifiedHash;
                }

                if (isModified)
                {
                    _database.DocumentsStorage.Put(context, document.Key, document.Etag, patchResult.Document);
                    result.PatchResult = PatchResult.Patched;
                }
            }

            return result;
        }

        private ChakraPatcher.Result ApplySingleScript(DocumentsOperationContext context, Document document, PatchRequest patch)
        {
            ChakraPatcherCache.CacheResult cacheResult;

            try
            {
                cacheResult = Cache.Get(patch, null);
            }
            catch (NotSupportedException e)
            {
                throw new ParseException("Could not parse script", e);
            }
            catch (Exception e)
            {
                throw new ParseException("Could not parse: " + Environment.NewLine + patch.Script, e);
            }

            ChakraPatcherOperationScope scope = null;
            try
            {
                scope = cacheResult.Patcher.Prepare(_database, context, patch, debugMode: false);

                //PrepareEngine(patch, document, scope, jintEngine);

                var patchedDocument = cacheResult.Patcher.Patch(document, context, scope);

                //CleanupEngine(patch, jintEngine, scope);

                cacheResult.Patcher.OutputLog(scope);
                //if (scope.DebugMode)
                //    scope.DebugInfo.Add(string.Format("Statements executed: {0}", jintEngine.StatementsCount));

                return new ChakraPatcher.Result
                {
                    Document = patchedDocument,
                    DebugActions = scope.DebugActions,
                    DebugInfo = scope.DebugInfo
                };
            }
            catch (ConcurrencyException)
            {
                throw;
            }
            catch (Exception errorEx)
            {
                if (scope != null)
                    cacheResult.Patcher.OutputLog(scope);

                var errorMsg = "Unable to execute JavaScript: " + Environment.NewLine + patch.Script + Environment.NewLine;
                var error = errorEx as ChakraException;
                if (error != null)
                    errorMsg += Environment.NewLine + "Error: " + Environment.NewLine + string.Join(Environment.NewLine, error.Message);

                if (scope?.DebugInfo.Items.Count != 0)
                    errorMsg += Environment.NewLine + "Debug information: " + Environment.NewLine + string.Join(Environment.NewLine, scope.DebugInfo.Items);

                throw new InvalidOperationException(errorMsg, errorEx);
            }
            finally
            {
                scope?.Dispose();

                Cache.Return(cacheResult);
            }
        }
    }
}