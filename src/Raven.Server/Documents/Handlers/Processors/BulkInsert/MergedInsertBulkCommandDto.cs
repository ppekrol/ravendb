﻿using System.Linq;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.Processors.BulkInsert;

internal sealed class MergedInsertBulkCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedInsertBulkCommand>
{
    public BatchRequestParser.CommandData[] Commands { get; set; }

    public MergedInsertBulkCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
    {
        return new MergedInsertBulkCommand
        {
            NumberOfCommands = Commands.Length,
            TotalSize = Commands.Sum(c => c.Document.Size),
            Commands = Commands,
            Database = database,
            Logger = LoggingSource.Instance.GetLogger<MergedInsertBulkCommandDto>(database.Name)
        };
    }
}
