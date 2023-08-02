﻿using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    internal sealed class EditRevisionsForConflictsConfigurationCommand : UpdateDatabaseCommand
    {
        public RevisionsCollectionConfiguration Configuration { get; private set; }

        public EditRevisionsForConflictsConfigurationCommand()
        {
        }

        public EditRevisionsForConflictsConfigurationCommand(RevisionsCollectionConfiguration configuration, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            Configuration = configuration;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.RevisionsForConflicts = Configuration;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }
    }
}
