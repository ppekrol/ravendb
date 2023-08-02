using System.Collections.Generic;

namespace Raven.Server.Documents.Commands
{
    internal sealed class WaitForIndexNotificationRequest
    {
        public List<long> RaftCommandIndexes { get; set; }
    }
}
