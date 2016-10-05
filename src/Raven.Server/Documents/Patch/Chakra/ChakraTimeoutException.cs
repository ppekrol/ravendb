using System;

namespace Raven.Server.Documents.Patch.Chakra
{
    public class ChakraTimeoutException : Exception
    {
        public ChakraTimeoutException(TimeSpan timeout, Exception e)
            : base($"Script timed-out. Waited for: {timeout.TotalSeconds}.", e)
        {
        }
    }
}