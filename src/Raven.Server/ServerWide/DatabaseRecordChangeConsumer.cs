using Raven.Client.ServerWide;

namespace Raven.Server.ServerWide
{
    /// <summary>
    /// Tracks which version of the database record sections a feature has already handled,
    /// so it is notified only when one of the sections it depends on was changed.
    /// It is accessed only under the lock of the database record state change.
    /// </summary>
    public sealed class DatabaseRecordChangeConsumer
    {
        public readonly string Name;

        public readonly DatabaseRecordSections Sections;

        private long? _processedVersion;

        public DatabaseRecordChangeConsumer(string name, DatabaseRecordSections sections)
        {
            Name = name;
            Sections = sections;
        }

        public bool ShouldProcess(DatabaseRecord record, bool forced, out long version)
        {
            version = record.ClusterState.GetVersion(Sections);

            if (forced || _processedVersion == null)
                return true;

            return version > _processedVersion.Value;
        }

        public void MarkProcessed(long version)
        {
            _processedVersion = version;
        }
    }
}
