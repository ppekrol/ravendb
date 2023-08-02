namespace Raven.Server.Smuggler.Migration
{
    internal sealed class ExportDataV35
    {
        public string DownloadOptions { get; set; }

        public long ProgressTaskId { get; set; }
    }

    internal sealed class ExportDataV3
    {
        public string SmugglerOptions { get; set; }
    }
}
