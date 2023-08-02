﻿using Jint;
using Jint.Native.Object;
using Jint.Runtime.Descriptors;

namespace Raven.Server.Documents.Indexes.Static.JavaScript
{
    internal abstract class ObjectInstanceBase : ObjectInstance
    {
        protected static readonly PropertyDescriptor ImplicitNull = new PropertyDescriptor(DynamicJsNull.ImplicitNull, writable: false, enumerable: false, configurable: false);

        protected ObjectInstanceBase(Engine engine) : base(engine)
        {
        }
    }
}
