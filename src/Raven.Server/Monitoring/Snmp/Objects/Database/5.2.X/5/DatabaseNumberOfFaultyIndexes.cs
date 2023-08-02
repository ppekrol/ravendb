// -----------------------------------------------------------------------
//  <copyright file="DatabaseOpenedCount.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes.Errors;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    internal sealed class DatabaseNumberOfFaultyIndexes : DatabaseScalarObjectBase<Integer32>
    {
        public DatabaseNumberOfFaultyIndexes(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.NumberOfFaultyIndexes, index)
        {
        }

        protected override Integer32 GetData(DocumentDatabase database)
        {
            return new Integer32(GetCount(database));
        }

        internal static int GetCount(DocumentDatabase database)
        {
            return database
                .IndexStore
                .GetIndexes()
                .Count(x => x is FaultyInMemoryIndex);
        }
    }
}
