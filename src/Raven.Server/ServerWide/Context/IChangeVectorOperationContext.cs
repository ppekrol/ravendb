﻿using System.Collections.Generic;
using Raven.Server.Utils;

namespace Raven.Server.ServerWide.Context;

internal interface IChangeVectorOperationContext
{
    ChangeVector GetChangeVector(string changeVector, bool throwOnRecursion = false);

    ChangeVector GetChangeVector(string version, string order);
}
