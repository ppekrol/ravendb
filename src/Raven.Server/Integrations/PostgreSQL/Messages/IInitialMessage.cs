using System.Collections.Generic;

namespace Raven.Server.Integrations.PostgreSQL.Messages
{
    internal interface IInitialMessage
    {
    }

    internal sealed class StartupMessage : IInitialMessage
    {
        public ProtocolVersion ProtocolVersion;
        public Dictionary<string, string> ClientOptions;
    }

    internal sealed class Cancel : IInitialMessage
    {
        public int ProcessId;
        public int SessionId;
    }

    internal sealed class SSLRequest : IInitialMessage
    {
    }
}
