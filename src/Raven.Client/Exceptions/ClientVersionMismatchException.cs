﻿using System;

namespace Raven.Client.Exceptions
{
    public sealed class ClientVersionMismatchException : RavenException
    {
        public ClientVersionMismatchException()
        {
        }

        public ClientVersionMismatchException(string message) : base(message)
        {
        }

        public ClientVersionMismatchException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
