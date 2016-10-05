using System;

namespace Raven.Server.Documents.Patch.Chakra
{
    public class ChakraException : Exception
    {
        public ChakraException(string message)
            : base(message)
        {
        }
    }
}