using System;

namespace Raven.Server.Exceptions
{
    internal sealed class SerializationNestedLevelTooDeepException : Exception
    {
        public SerializationNestedLevelTooDeepException(string message) : base(message)
        {
        }
    }
}
