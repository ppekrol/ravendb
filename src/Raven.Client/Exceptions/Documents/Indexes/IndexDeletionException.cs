﻿using System;

namespace Raven.Client.Exceptions.Documents.Indexes
{
    public sealed class IndexDeletionException : RavenException
    {
        public IndexDeletionException()
        {            
        }

        public IndexDeletionException(string message) : base(message)
        {
        }

        public IndexDeletionException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
