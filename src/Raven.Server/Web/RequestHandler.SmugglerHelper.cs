﻿using System.Text;
using Raven.Client.Documents.Changes;
using Raven.Server.Documents.Queries;

namespace Raven.Server.Web
{
    internal abstract partial class RequestHandler
    {
        internal void TrafficWatchQuery(IndexQueryServerSide indexQuery)
        {
            TrafficWatchQuery(indexQuery, TrafficWatchChangeType.Queries);
        }

        internal void TrafficWatchStreamQuery(IndexQueryServerSide indexQuery)
        {
            TrafficWatchQuery(indexQuery, TrafficWatchChangeType.Streams);
        }

        private void TrafficWatchQuery(IndexQueryServerSide indexQuery, TrafficWatchChangeType type)
        {
            var sb = new StringBuilder();

            // append stringBuilder with the query
            sb.Append(indexQuery.Query);
            // if query got parameters append with parameters
            if (indexQuery.QueryParameters != null && indexQuery.QueryParameters.Count > 0)
                sb.AppendLine().Append(indexQuery.QueryParameters);

            AddStringToHttpContext(sb.ToString(), type);
        }
    }
}
