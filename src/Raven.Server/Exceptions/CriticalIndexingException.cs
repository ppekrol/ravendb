﻿using System;

namespace Raven.Server.Exceptions
{
    interal class CriticalIndexingException : Exception
    {
        public CriticalIndexingException(Exception e)
            : base(e.Message, e)
        {
        }

        public CriticalIndexingException(string message, Exception e)
            : base(message, e)
        {
        }
    }
}
