using System.Collections.Generic;

namespace Raven.Server.Documents.PeriodicBackup.Retention
{
    internal sealed class GetFoldersResult
    {
        public List<string> List { get; set; }

        public bool HasMore { get; set; }
    }

    internal sealed class GetBackupFolderFilesResult
    {
        public string FirstFile { get; set; }

        public string LastFile { get; set; }
    }
}
