﻿using System;

namespace Raven.Server.Exceptions
{
    internal sealed class IndexDisposingException : Exception
    {

        public IndexDisposingException()
        {
        }

        public IndexDisposingException(string message) : base(message)
        {
        }

        public IndexDisposingException(string message, Exception inner) : base(message, inner)
        {
        }
    }

    internal sealed class IndexOpenException : Exception
    {
        public IndexOpenException(string message)
            : base(message)
        {
        }

        public IndexOpenException(string message, Exception e)
            : base(message, e)
        {
        }
    }
}
