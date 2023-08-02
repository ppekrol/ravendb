﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Raven.Client.Http;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Sharding.Executors;
using Sparrow.Platform;
using Sparrow.Server.Utils;

namespace Raven.Server.Documents
{
    internal static class ComputeHttpEtags
    {
        public static unsafe string ComputeEtagForDocuments(
            List<Document> documents,
            List<Document> includes,
            IncludeCountersCommand includeCounters,
            IncludeTimeSeriesCommand includeTimeSeries,
            IncludeCompareExchangeValuesCommand includeCompareExchangeValues)
        {
            // This method is efficient because we aren't materializing any values
            // except the change vector, which we need
            if (documents.Count == 1 &&
                (includes == null || includes.Count == 0) &&
                includeCounters == null &&
                includeTimeSeries == null &&
                includeCompareExchangeValues == null)
                return documents[0]?.ChangeVector ?? string.Empty;

            var size = Sodium.crypto_generichash_bytes();
            Debug.Assert((int)size == 32);
            var cryptoGenerichashStatebytes = (int)Sodium.crypto_generichash_statebytes();
            byte* state = stackalloc byte[cryptoGenerichashStatebytes];
            if (Sodium.crypto_generichash_init(state, null, UIntPtr.Zero, size) != 0)
                ThrowFailToInitHash();

            HashNumber(state, documents.Count);
            foreach (var doc in documents)
            {
                HashChangeVector(state, doc?.ChangeVector);
            }

            if (includes != null)
            {
                HashNumber(state, includes.Count);
                foreach (var doc in includes)
                {
                    if (doc is IncludeDocumentsCommand.ConflictDocument)
                        continue;

                    HashChangeVector(state, doc.ChangeVector);
                }
            }

            if (includeCounters != null)
            {
                foreach (var countersResult in includeCounters.Results)
                {
                    HashNumber(state, countersResult.Value.Count);
                    foreach (var counterDetail in countersResult.Value)
                    {
                        HashNumber(state, counterDetail?.Etag ?? 0);
                    }
                }
            }

            if (includeTimeSeries != null)
            {
                foreach (var tsIncludesForDocument in includeTimeSeries.Results)
                {
                    foreach (var kvp in tsIncludesForDocument.Value)
                    {
                        HashNumber(state, tsIncludesForDocument.Value.Count);

                        foreach (var rangeResult in kvp.Value)
                        {
                            HashNumber(state, rangeResult.Entries?.Length ?? 0);
                            HashChangeVector(state, rangeResult.Hash);
                        }
                    }
                }
            }

            if (includeCompareExchangeValues != null)
            {
                if (includeCompareExchangeValues.Results == null || includeCompareExchangeValues.Results.Count == 0)
                    HashNumber(state, 0);
                else
                {
                    HashNumber(state, includeCompareExchangeValues.Results.Count);

                    foreach (var compareExchangeValueInclude in includeCompareExchangeValues.Results)
                        HashNumber(state, compareExchangeValueInclude.Value.Index);
                }
            }

            return FinalizeHash(size, state);
        }

        public static unsafe string ComputeEtagForRevisions(List<Document> revisions)
        {
            // This method is efficient because we aren't materializing any values
            // except the change vector, which we need
            if (revisions.Count == 1)
                return revisions[0]?.ChangeVector ?? string.Empty;

            var size = Sodium.crypto_generichash_bytes();
            Debug.Assert((int)size == 32);
            var cryptoGenerichashStatebytes = (int)Sodium.crypto_generichash_statebytes();
            byte* state = stackalloc byte[cryptoGenerichashStatebytes];
            if (Sodium.crypto_generichash_init(state, null, UIntPtr.Zero, size) != 0)
                ThrowFailToInitHash();

            HashNumber(state, revisions.Count);
            foreach (var doc in revisions)
            {
                HashChangeVector(state, doc?.ChangeVector);
            }

            return FinalizeHash(size, state);
        }

        public static unsafe string CombineEtags(IEnumerable<string> etags)
        {
            var size = Sodium.crypto_generichash_bytes();
            Debug.Assert((int)size == 32);
            var cryptoGenericHashStateBytes = (int)Sodium.crypto_generichash_statebytes();
            byte* state = stackalloc byte[cryptoGenericHashStateBytes];
            if (Sodium.crypto_generichash_init(state, null, UIntPtr.Zero, size) != 0)
                ThrowFailToInitHash();

            var index = 0;
            string first = null;
            foreach (var etag in etags)
            {
                switch (index)
                {
                    case 0:
                        first = etag;
                        break;
                    case 1:
                        HashChangeVector(state, first);
                        goto default;
                    default:
                        HashChangeVector(state, etag);
                        break;
                }

                index++;
            }

            // if we have a result only from a single shard, simply return without hashing
            if (index == 1)
                return first;

            byte* final = stackalloc byte[(int)size];
            if (Sodium.crypto_generichash_final(state, final, size) != 0)
                ThrowFailedToFinalizeHash();

            var str = new string(' ', 49);
            fixed (char* p = str)
            {
                p[0] = 'H';
                p[1] = 'a';
                p[2] = 's';
                p[3] = 'h';
                p[4] = '-';
                var len = Base64.ConvertToBase64Array(p + 5, final, 0, 32);
                Debug.Assert(len == 44);
            }

            return str;
        }

        public static string CombineEtags<T>(Dictionary<int, ShardExecutionResult<T>> cmds) => CombineEtags(EnumerateEtags(cmds));

        public static IEnumerable<string> EnumerateEtags<T>(Dictionary<int, ShardExecutionResult<T>> cmds)
        {
            foreach (var shardInfo in cmds.Values)
            {
                var cmd = shardInfo.Command;
                string etag = cmd.Etag;
                if (etag != null)
                    yield return etag;
            }
        }

        internal static unsafe void HashChangeVector(byte* state, string changeVector)
        {
            if (changeVector == null)
            {
                HashNumber(state, 0);
                return;
            }
            fixed (char* pCV = changeVector)
            {
                if (Sodium.crypto_generichash_update(state, (byte*)pCV, (ulong)(sizeof(char) * changeVector.Length)) != 0)
                    ThrowFailedToUpdateHash();
            }
        }

        private static unsafe void HashNumber(byte* state, int num)
        {
            if (Sodium.crypto_generichash_update(state, (byte*)&num, sizeof(int)) != 0)
                ThrowFailedToUpdateHash();
        }

        public static unsafe void HashNumber(byte* state, long num)
        {
            if (Sodium.crypto_generichash_update(state, (byte*)&num, sizeof(long)) != 0)
                ThrowFailedToUpdateHash();
        }

        internal static unsafe string FinalizeHash(UIntPtr size, byte* state)
        {
            byte* final = stackalloc byte[(int)size];
            if (Sodium.crypto_generichash_final(state, final, size) != 0)
                ThrowFailedToFinalizeHash();

            var str = new string(' ', 49);
            fixed (char* p = str)
            {
                p[0] = 'H';
                p[1] = 'a';
                p[2] = 's';
                p[3] = 'h';
                p[4] = '-';
                var len = Base64.ConvertToBase64Array(p + 5, final, 0, 32);
                Debug.Assert(len == 44);
            }

            return str;
        }

        [DoesNotReturn]
        private static void ThrowFailedToFinalizeHash()
        {
            throw new InvalidOperationException("Failed to finalize generic hash");
        }

        [DoesNotReturn]
        internal static void ThrowFailToInitHash()
        {
            throw new InvalidOperationException("Failed to initiate generic hash");
        }

        [DoesNotReturn]
        private static void ThrowFailedToUpdateHash()
        {
            throw new InvalidOperationException("Failed to update generic hash");
        }
    }
}
