using System.Collections.Generic;

namespace Raven.Server.ServerWide.Commands.Subscriptions
{
    internal sealed class PutSubscriptionBatchCommand : PutSubscriptionBatchCommandBase<PutSubscriptionCommand>
    {

        public PutSubscriptionBatchCommand()
        {
        }

        public PutSubscriptionBatchCommand(List<PutSubscriptionCommand> commands, string uniqueRequestId) : base(commands, uniqueRequestId)
        {
        }
    }
}
