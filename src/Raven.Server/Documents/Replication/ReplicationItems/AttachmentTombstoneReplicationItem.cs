﻿using System.IO;
using Raven.Client.Util;
using Raven.Server.Documents.Replication.Stats;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Voron;

namespace Raven.Server.Documents.Replication.ReplicationItems
{
    internal sealed class AttachmentTombstoneReplicationItem : ReplicationBatchItem
    {
        public Slice Key;
        public DocumentFlags Flags;

        public override DynamicJsonValue ToDebugJson()
        {
            var djv = base.ToDebugJson();
            djv[nameof(Key)] = CompoundKeyHelper.ExtractDocumentId(Key);
            return djv;
        }

        public override long AssertChangeVectorSize()
        {
            return sizeof(byte) + // type
                   sizeof(int) + // # of change vectors
                   Encodings.Utf8.GetByteCount(ChangeVector) +
                   sizeof(short) + // transaction marker
                   sizeof(long) + // last modified
                   sizeof(int) + // size of key
                   Key.Size;
        }

        public override long Size => 0;

        public override unsafe void Write(Slice changeVector, Stream stream, byte[] tempBuffer, OutgoingReplicationStatsScope stats)
        {
            fixed (byte* pTemp = tempBuffer)
            {
                if (AssertChangeVectorSize() > tempBuffer.Length)
                    ThrowTooManyChangeVectorEntries(this, Key.ToString());

                var tempBufferPos = WriteCommon(changeVector, pTemp);

                *(long*)(pTemp + tempBufferPos) = LastModifiedTicks;
                tempBufferPos += sizeof(long);

                *(int*)(pTemp + tempBufferPos) = Key.Size;
                tempBufferPos += sizeof(int);

                Memory.Copy(pTemp + tempBufferPos, Key.Content.Ptr, Key.Size);
                tempBufferPos += Key.Size;

                stream.Write(tempBuffer, 0, tempBufferPos);

                stats.RecordAttachmentTombstoneOutput();
            }
        }

        public override unsafe void Read(JsonOperationContext context, ByteStringContext allocator, IncomingReplicationStatsScope stats)
        {
            using (stats.For(ReplicationOperation.Incoming.TombstoneRead))
            {
                stats.RecordAttachmentTombstoneRead();
                LastModifiedTicks = *(long*)Reader.ReadExactly(sizeof(long));

                var size = *(int*)Reader.ReadExactly(sizeof(int));
                ToDispose(Slice.From(allocator, Reader.ReadExactly(size), size, ByteStringType.Immutable, out Key));
            }
        }

        protected override ReplicationBatchItem CloneInternal(JsonOperationContext context, ByteStringContext allocator)
        {
            var item = new AttachmentTombstoneReplicationItem();
            item.Key = Key.Clone(allocator);

            item.ToDispose(new DisposableAction(() =>
            {
                item.Key.Release(allocator);
            }));

            return item;
        }

        protected override void InnerDispose()
        {
        }
    }
}
