﻿using System;
using System.IO;
using Raven.Client.Documents.Commands.Batches;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.Documents.Handlers.Batches.Commands;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.BulkInsert;

internal sealed class ShardedBatchCommandData : IDisposable, IBatchCommandData
{
    private readonly JsonOperationContext _ctx;

    public ShardedBatchCommandData(JsonOperationContext ctx)
    {
        _ctx = ctx;
        Stream = ctx.CheckoutMemoryStream();
    }

    public MemoryStream Stream { get; }

    public BatchRequestParser.CommandData Data { get; set; }

    public CommandType Type => Data.Type;

    public string Id => Data.Id;

    public MergedBatchCommand.AttachmentStream AttachmentStream { get; set; }

    public long ContentLength => Data.ContentLength;

    public void Dispose()
    {
        _ctx.ReturnMemoryStream(Stream);
    }
}
