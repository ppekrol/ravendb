﻿using System;
using System.Collections.Generic;
using System.Net.Http;
using Microsoft.AspNetCore.Http;
using Raven.Client.Http;
using Raven.Server.Documents.Commands;
using Raven.Server.Documents.Sharding.Executors;

namespace Raven.Server.Documents.Sharding.Operations
{
    internal readonly struct WaitForIndexNotificationOperation : IShardedOperation
    {
        private readonly List<long> _indexes;

        public WaitForIndexNotificationOperation(List<long> indexes)
        {
            _indexes = indexes;
        }

        public WaitForIndexNotificationOperation(long index) : this(new List<long>(capacity: 1) { index })
        {
        }

        public void ModifyHeaders(HttpRequestMessage request)
        {
            // we don't care here for any headers
        }

        public HttpRequest HttpRequest => null;
        public object Combine(Dictionary<int, ShardExecutionResult<object>> results) => throw new NotImplementedException();

        public RavenCommand<object> CreateCommandForShard(int shardNumber) => new WaitForIndexNotificationCommand(_indexes);
    }
}
