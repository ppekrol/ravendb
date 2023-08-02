using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.TransactionMerger.Commands;

internal interface IReplayableCommandDto<TOperationContext, TTransaction, out TCommand> 
    where TCommand : MergedTransactionCommand<TOperationContext, TTransaction>
    where TOperationContext : TransactionOperationContext<TTransaction>
    where TTransaction : RavenTransaction
{
    TCommand ToCommand(TOperationContext context, DocumentDatabase database);
}
