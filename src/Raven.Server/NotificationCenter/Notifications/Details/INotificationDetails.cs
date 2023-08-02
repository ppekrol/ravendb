using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    internal interface INotificationDetails
    {
        DynamicJsonValue ToJson();
    }
}