using System;
using Raven.Client.Documents.Replication;

namespace Raven.Server.Documents.Replication.Outgoing
{
    internal interface IAbstractOutgoingReplicationHandler : IDisposable, IReportOutgoingReplicationPerformance
    {
        public ReplicationNode Node { get; }
        public long LastSentDocumentEtag { get; }
        public string LastAcceptedChangeVector { get; set; }
        public ReplicationNode Destination { get; }
        public bool IsConnectionDisposed { get; }
        public string GetNode();
        public void Start();
    }
}
