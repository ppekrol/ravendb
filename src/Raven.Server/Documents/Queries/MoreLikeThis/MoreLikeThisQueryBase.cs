﻿using Sparrow.Json;

namespace Raven.Server.Documents.Queries.MoreLikeThis;

internal abstract class MoreLikeThisQueryBase
{
    public string BaseDocument { get; set; }

    public BlittableJsonReaderObject Options { get; set; }
}
