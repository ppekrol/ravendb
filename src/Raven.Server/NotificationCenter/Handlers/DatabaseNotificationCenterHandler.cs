﻿using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.Handlers.Processors;
using Raven.Server.Routing;

namespace Raven.Server.NotificationCenter.Handlers
{
    internal sealed class DatabaseNotificationCenterHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/notification-center/watch", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, SkipUsagesCount = true)]
        public async Task Watch()
        {
            using (var processor = new DatabaseNotificationCenterHandlerProcessorForWatch(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/notification-center/dismiss", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task Dismiss()
        {
            using (var processor = new DatabaseNotificationCenterHandlerProcessorForDismiss(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/notification-center/postpone", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task Postpone()
        {
            using (var processor = new DatabaseNotificationCenterHandlerProcessorForPostpone(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/notification-center/stats", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Stats()
        {
            using (var processor = new DatabaseNotificationCenterHandlerProcessorForStats(this))
                await processor.ExecuteAsync();
        }
    }
}
