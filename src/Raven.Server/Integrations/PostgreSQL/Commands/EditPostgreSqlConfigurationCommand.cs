﻿using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.Integrations;
using Raven.Client.ServerWide.Operations.Integrations.PostgreSQL;
using Raven.Server.ServerWide.Commands;
using Sparrow.Json.Parsing;

namespace Raven.Server.Integrations.PostgreSQL.Commands
{
    internal sealed class EditPostgreSqlConfigurationCommand : UpdateDatabaseCommand
    {
        public PostgreSqlConfiguration Configuration;

        public EditPostgreSqlConfigurationCommand()
        {
            // for deserialization
        }

        public EditPostgreSqlConfigurationCommand(
            PostgreSqlConfiguration configuration,
            string databaseName,
            string uniqueRequestId)
            : base(databaseName, uniqueRequestId)
        {
            Configuration = configuration;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.Integrations ??= new IntegrationConfigurations();

            record.Integrations.PostgreSql = Configuration;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = Configuration.ToJson();
        }
    }
}
