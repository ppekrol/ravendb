using System.Threading.Tasks;

namespace Raven.Server.NotificationCenter
{
    internal interface IWebsocketWriter
    {
        Task WriteToWebSocket<TNotification>(TNotification notification);
    }
}