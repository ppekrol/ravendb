using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations;

namespace Raven.Server.Documents.Changes;

internal interface IDocumentsChanges
{
    void RaiseNotifications(TopologyChange topologyChange);
    void RaiseNotifications(OperationStatusChange operationStatusChange);
}
