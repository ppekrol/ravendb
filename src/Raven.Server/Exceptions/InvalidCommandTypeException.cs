using System;

namespace Raven.Server.Exceptions
{
    internal sealed class InvalidCommandTypeException : Exception
    {
        public InvalidCommandTypeException(string msg): base(msg) { }
    }
}
