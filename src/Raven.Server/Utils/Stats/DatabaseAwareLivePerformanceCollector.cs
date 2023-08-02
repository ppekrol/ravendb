﻿using Raven.Server.Documents;

namespace Raven.Server.Utils.Stats
{
    internal abstract class DatabaseAwareLivePerformanceCollector<T> : LivePerformanceCollector<T>
    {
        protected readonly DocumentDatabase Database;

        protected DatabaseAwareLivePerformanceCollector(DocumentDatabase database): base(database.DatabaseShutdown, database.Name)
        {
            Database = database;
        }
        
    }
}
