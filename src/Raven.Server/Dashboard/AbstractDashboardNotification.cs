﻿using System;
using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard
{
    public delegate bool CanAccessDatabase(string databaseName, bool requiresWrite);

    internal abstract class AbstractDashboardNotification
    {
        // marker interface
    }
}
