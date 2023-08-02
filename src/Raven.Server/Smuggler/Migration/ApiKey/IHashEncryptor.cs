using System;
using System.IO;

namespace Raven.Server.Smuggler.Migration.ApiKey
{
    internal interface IHashEncryptor : IDisposable
    {
        int StorageHashSize { get; }

        byte[] ComputeForOAuth(byte[] bytes);

        byte[] Compute16(byte[] bytes);

        byte[] Compute16(Stream stream);

        byte[] Compute16(byte[] bytes, int offset, int size);

        byte[] Compute20(byte[] bytes);

        byte[] Compute20(byte[] bytes, int offset, int size);
    }
}
