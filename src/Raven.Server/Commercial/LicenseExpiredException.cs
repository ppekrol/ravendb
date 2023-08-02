using System;

namespace Raven.Server.Commercial
{
    internal sealed class LicenseExpiredException : Exception
    {
        public LicenseExpiredException()
        {
        }

        public LicenseExpiredException(string message) : base(message)
        {
        }

        public LicenseExpiredException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}