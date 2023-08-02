//-----------------------------------------------------------------------
// <copyright file="Reference.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Server.Utils
{
    /// <summary>
    /// A reference that can be used with lambda expression
    /// to pass a value out.
    /// </summary>
    internal sealed class Reference<T>
    {
        /// <summary>
        /// Gets or sets the value.
        /// </summary>
        /// <value>The value.</value>
        public T Value { get; set; }
    }
}
