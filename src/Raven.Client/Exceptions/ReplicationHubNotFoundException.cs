using System;

namespace Raven.Client.Exceptions
{
    public sealed class ReplicationHubNotFoundException : RavenException
    {
        public ReplicationHubNotFoundException()
        {
        }

        public ReplicationHubNotFoundException(string message) : base(message)
        {
        }

        public ReplicationHubNotFoundException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
