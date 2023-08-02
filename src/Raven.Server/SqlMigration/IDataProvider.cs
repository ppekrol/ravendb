using System;
using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration
{
    internal interface IDataProvider<out T> : IDisposable
    {
        T Provide(DynamicJsonValue specialColumns);
    }
    
}
