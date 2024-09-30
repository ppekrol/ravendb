using System;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Platform.Posix;
using Sparrow.Server.Platform.Posix;

namespace Sparrow.Server.LowMemory
{
    public class LowMemoryMonitor : AbstractLowMemoryMonitor
    {
        private IDisposable _releaseSmapsReader;

        private readonly ISmapsReader _smapsReader;

        public LowMemoryMonitor()
        {
            if (PlatformDetails.RunningOnLinux)
                _releaseSmapsReader = SmapsFactory.CreateSmapsReader(out _smapsReader);
        }

        public override MemoryInfoResult GetMemoryInfoOnce()
        {
            return MemoryInformation.GetMemoryInformationUsingOneTimeSmapsReader();
        }

        public override MemoryInfoResult GetMemoryInfo(bool extended = false)
        {
            return MemoryInformation.GetMemoryInfo(extended ? _smapsReader : null, extended: extended);
        }

        public override bool IsEarlyOutOfMemory(MemoryInfoResult memInfo, out Size commitChargeThreshold)
        {
            return MemoryInformation.IsEarlyOutOfMemory(memInfo, out commitChargeThreshold);
        }

        public override DirtyMemoryState GetDirtyMemoryState()
        {
            return MemoryInformation.GetDirtyMemoryState();
        }

        public override void AssertNotAboutToRunOutOfMemory()
        {
            MemoryInformation.AssertNotAboutToRunOutOfMemory();
        }

        public override void Dispose()
        {
            _releaseSmapsReader?.Dispose();
            _releaseSmapsReader = null;
        }
    }
}
