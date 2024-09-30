using Sparrow.Platform.Posix;
using System.Diagnostics;
using System;
using System.Buffers;
using System.IO;
using Sparrow.Platform;

namespace Sparrow.Server.Platform.Posix;

internal static class SmapsFactory
{
    public const int BufferSize = 4096;

    public static SmapsReaderType DefaultSmapsReaderType = SmapsReaderType.Smaps;

    static SmapsFactory()
    {
        var envSmapsReaderType = Environment.GetEnvironmentVariable("RAVEN_SMAPS_READER_TYPE");

        if (PlatformDetails.RunningOnLinux == false)
            return;

        if (string.IsNullOrWhiteSpace(envSmapsReaderType) == false && Enum.TryParse<SmapsReaderType>(envSmapsReaderType, ignoreCase: true, out var smapsReaderType))
        {
            DefaultSmapsReaderType = smapsReaderType;
            return;
        }

        using (var process = Process.GetCurrentProcess())
        {
            if (File.Exists(SmapsRollupReader.GetSmapsPath(process.Id)))
                DefaultSmapsReaderType = SmapsReaderType.SmapsRollup;
        }
    }

    public static IDisposable CreateSmapsReader(out ISmapsReader smapsReader) => CreateSmapsReader(DefaultSmapsReaderType, out smapsReader);

    public static IDisposable CreateSmapsReader(SmapsReaderType type, out ISmapsReader smapsReader)
    {
        var buffers = new[]
        {
            ArrayPool<byte>.Shared.Rent(BufferSize),
            ArrayPool<byte>.Shared.Rent(BufferSize)
        };

        smapsReader = CreateSmapsReader(type, buffers);

        return new ReleaseSmapsReader(buffers);
    }

    private static ISmapsReader CreateSmapsReader(SmapsReaderType type, byte[][] smapsBuffer)
    {
        switch (type)
        {
            case SmapsReaderType.Smaps:
                return new SmapsReader(smapsBuffer);
            case SmapsReaderType.SmapsRollup:
                return new SmapsRollupReader(smapsBuffer);
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    public struct ReleaseSmapsReader : IDisposable
    {
        private readonly byte[][] _smapsBuffer;

        public ReleaseSmapsReader(byte[][] smapsBuffer)
        {
            _smapsBuffer = smapsBuffer;
        }

        public void Dispose()
        {
            ArrayPool<byte>.Shared.Return(_smapsBuffer[0]);
            ArrayPool<byte>.Shared.Return(_smapsBuffer[1]);
        }
    }
}
