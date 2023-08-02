﻿using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Stats
{
    internal sealed class EtlTaskStats : IDynamicJson
    {
        public string TaskName { get; set; }

        public EtlProcessTransformationStats[] Stats { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(TaskName)] = TaskName,
                [nameof(Stats)] = new DynamicJsonArray(Stats)
            };
        }
    }

    internal class EtlProcessTransformationStats : IDynamicJson
    {
        public string TransformationName { get; set; }

        public EtlProcessStatistics Statistics { get; set; }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(TransformationName)] = TransformationName,
                [nameof(Statistics)] = Statistics.ToJson()
            };
        }
    }
}
