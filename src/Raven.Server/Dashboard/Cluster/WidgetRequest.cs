// -----------------------------------------------------------------------
//  <copyright file="WidgetRequest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

namespace Raven.Server.Dashboard.Cluster
{
    internal sealed class WidgetRequest
    {
        public string Command { get; set; }
        public int Id { get; set; }
        public ClusterDashboardNotificationType Type { get; set; }
        public object Config { get; set; }
    }
}
