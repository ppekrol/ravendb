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
            using (context.GetMemoryBuffer(out MemoryBuffer buffer))
            using (context.GetMemoryBuffer(out MemoryBuffer cryptoState))
            {
                if (cryptoState.Base.Length < (int)Sodium.crypto_generichash_statebytes())
                    throw new InvalidOperationException("BUG: shouldn't happen, the size of a generic hash state was too large!");

                InitComputeHash(cryptoState);

                var bufferRead = 0;
                while (true)
                {
                    var count = await requestStream.ReadAsync(buffer.Base.Memory.Slice(bufferRead), cancellationToken);
                    if (count == 0)
                        break;

                    bufferRead += count;

                    if (bufferRead == buffer.Base.Length)
                    {
                        PartialComputeHash(cryptoState, buffer, bufferRead);
                        await file.WriteAsync(buffer.Base.Memory, cancellationToken);
                        bufferRead = 0;
                    }
                }
                await file.WriteAsync(buffer.Base.Memory.Slice(0, bufferRead), cancellationToken);
                PartialComputeHash(cryptoState, buffer, bufferRead);
                var hash = FinalizeGetHash(cryptoState, buffer);
                return hash;
            }
        }

        private static unsafe void InitComputeHash(MemoryBuffer cryptoState)
        {
            var rc = Sodium.crypto_generichash_init(cryptoState.Base.Pointer, null, UIntPtr.Zero, Sodium.crypto_generichash_bytes());
            if (rc != 0)
                throw new InvalidOperationException("Unable to hash attachment: " + rc);
        }

        private static unsafe string FinalizeGetHash(MemoryBuffer cryptoState, MemoryBuffer buffer)
        {
            var size = Sodium.crypto_generichash_bytes();
            var rc = Sodium.crypto_generichash_final(cryptoState.Base.Pointer, buffer.Base.Pointer, size);
            if (rc != 0)
                throw new InvalidOperationException("Unable to hash attachment: " + rc);

            return Convert.ToBase64String(buffer.Base.Memory.Span.Slice((int)size));
        }

        private static unsafe void PartialComputeHash(MemoryBuffer cryptoState, MemoryBuffer buffer, int bufferRead)
        {
            var rc = Sodium.crypto_generichash_update(cryptoState.Base.Pointer, buffer.Base.Pointer, (ulong)bufferRead);
            if (rc != 0)
                throw new InvalidOperationException("Unable to hash attachment: " + rc);
        }
    }
}
