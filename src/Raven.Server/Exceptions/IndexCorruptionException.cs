using System;

namespace Raven.Server.Exceptions
{
    internal sealed class IndexCorruptionException : Exception
    {
        public IndexCorruptionException(Exception e)
            : base(e.Message, e)
        {
        }
    }
}