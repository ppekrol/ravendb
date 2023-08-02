using System;
using Sparrow.Json;

namespace Raven.Server.NotificationCenter
{
    internal sealed class NotificationTableValue
    {
        public BlittableJsonReaderObject Json;

        public DateTime CreatedAt;

        public DateTime? PostponedUntil;
    }
}