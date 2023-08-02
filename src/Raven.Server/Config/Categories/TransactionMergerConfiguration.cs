using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Sparrow;
using Sparrow.LowMemory;
using Sparrow.Platform;

namespace Raven.Server.Config.Categories
{
    [ConfigurationCategory(ConfigurationCategoryType.TransactionMerger)]
    internal sealed class TransactionMergerConfiguration : ConfigurationCategory
    {
        public TransactionMergerConfiguration(bool forceUsing32BitsPager)
        {
            if (PlatformDetails.Is32Bits || forceUsing32BitsPager)
            {
                MaxTxSize = new Size(4, SizeUnit.Megabytes);
                return;
            }

            MaxTxSize = Size.Min(
                new Size(512, SizeUnit.Megabytes),
                MemoryInformation.TotalPhysicalMemory / 10);
        }

        [Description("EXPERT: Time to wait after the previous async commit is completed before checking for the tx size")]
        [DefaultValue(0)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("TransactionMerger.MaxTimeToWaitForPreviousTxInMs", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting MaxTimeToWaitForPreviousTx { get; set; }

        [Description("EXPERT: Time to wait for the previous async commit transaction before rejecting the request due to long duration IO")]
        [DefaultValue(5000)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("TransactionMerger.MaxTimeToWaitForPreviousTxBeforeRejectingInMs", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting MaxTimeToWaitForPreviousTxBeforeRejecting { get; set; }

        [Description("EXPERT: Maximum size for the merged transaction")]
        [DefaultValue(DefaultValueSetInConstructor)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("TransactionMerger.MaxTxSizeInMb", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public Size MaxTxSize { get; set; }
    }
}
