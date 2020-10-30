﻿// -----------------------------------------------------------------------
//  <copyright file="AdminCpuCreditsHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.System
{
    public class AdminCpuCreditsHandler : RequestHandler
    {
        [RavenAction("/admin/cpu-credits", "POST", AuthorizationStatus.ClusterAdmin)]
        public Task UpdateCpuCredits()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (var newCredits = ctx.ReadForDisk(RequestBodyStream(), "cpu-credits"))
            {
                var updated = JsonDeserializationServer.CpuCredits(newCredits);
                Server.CpuCreditsBalance.RemainingCpuCredits = updated.RemainingCredits;
                return Task.CompletedTask;
            }
        }

        [RavenAction("/debug/cpu-credits", "GET", AuthorizationStatus.ValidUser, IsDebugInformationEndpoint = true)]
        public Task GetCpuCredits()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var json = Server.CpuCreditsBalance.ToJson();
                writer.WriteObjectAsync(context.ReadObject(json, "cpu/credits"));
                return Task.CompletedTask;
            }
        }

        public class CpuCredits
        {
            public double RemainingCredits;
            
            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(RemainingCredits)] = RemainingCredits
                };
            }
        }
    }
}
