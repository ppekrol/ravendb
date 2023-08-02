namespace Raven.Server.Commercial
{
    internal sealed class RSAPublicParameters
    {
        public RSAKeyValue RsaKeyValue { get; set; }

        internal sealed class RSAKeyValue
        {
            public byte[] Modulus { get; set; }

            public byte[] Exponent { get; set; }
        }
    }
}