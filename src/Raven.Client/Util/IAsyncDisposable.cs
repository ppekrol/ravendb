// -----------------------------------------------------------------------
//  <copyright file="IAsyncDisposable.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;

namespace Raven.Client.Util
{
#if NETSTANDARD2_0 || NETCOREAPP2_1
    internal interface IAsyncDisposable
    {
        ValueTask DisposeAsync();
    }
#endif
}
