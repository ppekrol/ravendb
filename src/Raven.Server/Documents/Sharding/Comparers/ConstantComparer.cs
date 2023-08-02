﻿using System;
using System.Collections.Generic;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Comparers;

internal sealed class ConstantComparer : IComparer<BlittableJsonReaderObject>
{
    public static readonly ConstantComparer Instance = new();

    private ConstantComparer()
    {
    }

    public int Compare(BlittableJsonReaderObject x, BlittableJsonReaderObject y) => 0;
}
