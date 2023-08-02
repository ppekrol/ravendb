﻿using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Text;
using Raven.Client.Documents.Commands.Batches;
using Raven.Server.Documents.Handlers.Batches;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Batches;

internal sealed class BufferedCommand
{
    public MemoryStream CommandStream;
    public bool IsIdentity;
    public bool IsServerSideIdentity;
    public bool IsNullOrEmptyId;
    public bool IsBatchPatch;

    // for identities we should replace the id and the change vector
    public int IdStartPosition;
    public int ChangeVectorPosition;
    public int IdLength;
    public bool AddQuotes;

    public bool ModifyIdentityStreamRequired => IsServerSideIdentity || IsIdentity || IsNullOrEmptyId;

    public MemoryStream ModifyIdentityStream(BatchRequestParser.CommandData cmd)
    {
        if (cmd.Type != CommandType.PUT)
            throw new InvalidOperationException($"Expected command of type 'PUT', but got {cmd.Type}");

        if (ModifyIdentityStreamRequired == false)
            throw new InvalidOperationException("Must be an identity");

        var id = cmd.Id;
        if (AddQuotes)
            id = "\"" + id + "\"";

        var modifier = IdentityCommandModifier.Create(IdStartPosition, IdLength, ChangeVectorPosition, id);
        return modifier.Rewrite(CommandStream);
    }

    public MemoryStream ModifyBatchPatchStream(List<(string Id, string ChangeVector)> list)
    {
        if (IsBatchPatch == false)
            throw new InvalidOperationException("Must be batch patch");

        var modifier = new PatchCommandModifier(IdStartPosition, IdLength, list);
        return modifier.Rewrite(CommandStream);
    }

    internal interface IItemModifier
    {
        public void Validate();
        public int GetPosition();
        public int GetLength();
        public byte[] NewValue();
    }

    internal sealed class PatchModifier : IItemModifier
    {
        public List<(string Id, string ChangeVector)> List;
        public int IdsStartPosition;
        public int IdsLength;

        public void Validate()
        {
            if (List == null || List.Count == 0)
                BufferedCommandModifier.ThrowArgumentMustBePositive("Ids");

            if (IdsStartPosition <= 0)
                BufferedCommandModifier.ThrowArgumentMustBePositive("Ids position");

            if (IdsLength <= 0)
                BufferedCommandModifier.ThrowArgumentMustBePositive("Ids length");
        }

        public int GetPosition() => IdsStartPosition;

        public int GetLength() => IdsLength;

        public byte[] NewValue()
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(ctx))
            {
                builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                builder.StartArrayDocument();

                builder.StartWriteArray();
                foreach (var item in List)
                {
                    builder.StartWriteObject();
                    builder.WritePropertyName(nameof(ICommandData.Id));
                    builder.WriteValue(item.Id);
                    if (item.ChangeVector != null)
                    {
                        builder.WritePropertyName(nameof(ICommandData.ChangeVector));
                        builder.WriteValue(item.ChangeVector);
                    }
                    builder.WriteObjectEnd();
                }
                builder.WriteArrayEnd();
                builder.FinalizeDocument();

                var reader = builder.CreateArrayReader();
                return Encoding.UTF8.GetBytes(reader.ToString());
            }
        }
    }

    internal sealed class ChangeVectorModifier : IItemModifier
    {
        public int ChangeVectorPosition;

        public void Validate()
        {
            if (ChangeVectorPosition <= 0)
                BufferedCommandModifier.ThrowArgumentMustBePositive("Change vector position");
        }

        public int GetPosition() => ChangeVectorPosition;
        public int GetLength() => 4; // null
        public byte[] NewValue() => Empty;

        private static readonly byte[] Empty = Encoding.UTF8.GetBytes("\"\"");
    }

    internal sealed class IdModifier : IItemModifier
    {
        public int IdStartPosition;
        public int IdLength;

        public string NewId;

        public void Validate()
        {
            if (IdStartPosition <= 0)
                BufferedCommandModifier.ThrowArgumentMustBePositive("Id position");

            if (IdLength < 0) // can be zero, if requested ID is 'string.Empty'
                BufferedCommandModifier.ThrowArgumentMustBePositive("Id length");
        }

        public int GetPosition() => IdStartPosition;
        public int GetLength() => IdLength;
        public byte[] NewValue() => Encoding.UTF8.GetBytes(NewId);
    }

    internal sealed class PatchCommandModifier : BufferedCommandModifier
    {
        public PatchCommandModifier(int idsStartPosition, int idsLength, List<(string Id, string ChangeVector)> list)
        {
            Items = new IItemModifier[1];
            Items[0] = new PatchModifier
            {
                List = list,
                IdsLength = idsLength,
                IdsStartPosition = idsStartPosition
            };
        }
    }

    internal sealed class IdentityCommandModifier : BufferedCommandModifier
    {
        public static IdentityCommandModifier Create(int idStartPosition, int idLength, int changeVectorPosition, string newId)
        {
            return changeVectorPosition == 0 ? 
                new IdentityCommandModifier(idStartPosition, idLength, newId) : 
                new IdentityCommandModifier(idStartPosition, idLength, changeVectorPosition, newId);
        }

        private IdentityCommandModifier(int idStartPosition, int idLength, int changeVectorPosition, string newId)
        {
            Items = new IItemModifier[2];

            var idModifier = new IdModifier
            {
                IdLength = idLength,
                IdStartPosition = idStartPosition,
                NewId = newId
            };
            var cvModifier = new ChangeVectorModifier
            {
                ChangeVectorPosition = changeVectorPosition
            };

            if (changeVectorPosition < idStartPosition)
            {
                Items[0] = cvModifier;
                Items[1] = idModifier;
            }
            else
            {
                Items[1] = cvModifier;
                Items[0] = idModifier;
            }
        }

        private IdentityCommandModifier(int idStartPosition, int idLength, string newId)
        {
            Items = new IItemModifier[1];

            var idModifier = new IdModifier
            {
                IdLength = idLength,
                IdStartPosition = idStartPosition,
                NewId = newId
            };
           
            Items[0] = idModifier;
        }
    }

    internal abstract class BufferedCommandModifier
    {
        protected IItemModifier[] Items;


        public MemoryStream Rewrite(MemoryStream source)
        {
            EnsureInitialized();

            var offset = 0;
            var dest = new MemoryStream();
            try
            {
                source.Position = 0;

                var sourceBuffer = source.GetBuffer();

                foreach (var item in Items)
                {
                    offset = WriteRemaining(item.GetPosition());
                    dest.Write(item.NewValue());
                    offset += item.GetLength();
                }

                // copy the rest
                source.Position = offset;
                source.CopyTo(dest);

                int WriteRemaining(int upto)
                {
                    var remaining = upto - offset;
                    if (remaining < 0)
                        throw new InvalidOperationException();

                    if (remaining > 0)
                    {
                        dest.Write(sourceBuffer, offset, remaining);
                        offset += remaining;
                    }

                    return offset;
                }
            }
            catch
            {
                dest.Dispose();
                throw;
            }

            return dest;
        }

        private void EnsureInitialized()
        {
            if (Items == null || Items.Length == 0)
                throw new InvalidOperationException();

            foreach (var item in Items)
            {
                item.Validate();
            }
        }

        [DoesNotReturn]
        public static void ThrowArgumentMustBePositive(string name)
        {
            throw new ArgumentException($"{name} must be positive");
        }
    }
}
