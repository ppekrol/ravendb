﻿using System;

namespace Raven.Server.Documents.Indexes.MapReduce.Exceptions
{
    internal sealed class ExcessiveNumberOfReduceErrorsException : Exception
    {
        public ExcessiveNumberOfReduceErrorsException()
        {
        }

        public ExcessiveNumberOfReduceErrorsException(string message) : base(message)
        {
        }

        public ExcessiveNumberOfReduceErrorsException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
