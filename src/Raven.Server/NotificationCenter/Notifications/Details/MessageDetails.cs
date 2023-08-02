﻿using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    internal sealed class MessageDetails : INotificationDetails
    {
        public string Message { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                [nameof(Message)] = Message
            };
        }
    }
}