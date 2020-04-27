using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Server;

namespace Raven.Server.Documents
{
    public static class AttachmentsStorageHelper
    {
        public static async Task<string> CopyStreamToFileAndCalculateHash(DocumentsOperationContext context, Stream requestStream, Stream file, CancellationToken cancellationToken)
        {
            using (context.GetMemoryBuffer(out JsonOperationContext.MemoryBuffer buffer))
            using (context.GetMemoryBuffer(out JsonOperationContext.MemoryBuffer cryptoState))
            {
                if (cryptoState.Length < (int)Sodium.crypto_generichash_statebytes())
                    throw new InvalidOperationException("BUG: shouldn't happen, the size of a generic hash state was too large!");

                InitComputeHash(cryptoState);

                var bufferRead = 0;
                while (true)
                {
                    var count = await requestStream.ReadAsync(buffer.Memory.Slice(bufferRead), cancellationToken);
                    if (count == 0)
                        break;

                    bufferRead += count;

                    if (bufferRead == buffer.Length)
                    {
                        PartialComputeHash(cryptoState, buffer, bufferRead);
                        await file.WriteAsync(buffer.Memory, cancellationToken);
                        bufferRead = 0;
                    }
                }
                await file.WriteAsync(buffer.Memory.Slice(0, bufferRead), cancellationToken);
                PartialComputeHash(cryptoState, buffer, bufferRead);
                var hash = FinalizeGetHash(cryptoState, buffer);
                return hash;
            }
        }

        private static unsafe void InitComputeHash(JsonOperationContext.MemoryBuffer cryptoState)
        {
            var rc = Sodium.crypto_generichash_init(cryptoState.Pointer, null, UIntPtr.Zero, Sodium.crypto_generichash_bytes());
            if (rc != 0)
                throw new InvalidOperationException("Unable to hash attachment: " + rc);
        }

        private static unsafe string FinalizeGetHash(JsonOperationContext.MemoryBuffer cryptoState, JsonOperationContext.MemoryBuffer buffer)
        {
            var size = Sodium.crypto_generichash_bytes();
            var rc = Sodium.crypto_generichash_final(cryptoState.Pointer, buffer.Pointer, size);
            if (rc != 0)
                throw new InvalidOperationException("Unable to hash attachment: " + rc);

            return Convert.ToBase64String(buffer.Memory.Span.Slice((int)size));
        }

        private static unsafe void PartialComputeHash(JsonOperationContext.MemoryBuffer cryptoState, JsonOperationContext.MemoryBuffer buffer, int bufferRead)
        {
            var rc = Sodium.crypto_generichash_update(cryptoState.Pointer, buffer.Pointer, (ulong)bufferRead);
            if (rc != 0)
                throw new InvalidOperationException("Unable to hash attachment: " + rc);
        }
    }
}
