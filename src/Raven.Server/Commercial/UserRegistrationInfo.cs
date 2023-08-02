using Raven.Client.ServerWide.Operations;

namespace Raven.Server.Commercial
{
    internal sealed class UserRegistrationInfo
    {
        public string FirstName { get; set; }

        public string LastName { get; set; }

        public string Email { get; set; }

        public string Company { get; set; }

        public BuildNumber BuildInfo { get; set; }
    }
}
