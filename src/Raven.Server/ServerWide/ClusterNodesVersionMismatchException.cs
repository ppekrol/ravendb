﻿using System;
using Raven.Client.Exceptions;

namespace Raven.Server.ServerWide
{
    internal sealed class ClusterNodesVersionMismatchException : RavenException
    {
        public ClusterNodesVersionMismatchException()
        {
        }

        public ClusterNodesVersionMismatchException(string message) : base(message)
        {
        }

        public ClusterNodesVersionMismatchException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
