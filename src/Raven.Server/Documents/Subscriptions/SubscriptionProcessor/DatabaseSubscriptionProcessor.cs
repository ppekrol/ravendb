﻿using System;
using System.Collections.Generic;
using Jint;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime;
using Raven.Client;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Logging;

namespace Raven.Server.Documents.Subscriptions.SubscriptionProcessor
{
    public abstract class DatabaseSubscriptionProcessor<T> : DatabaseSubscriptionProcessor
    {
        protected Logger Logger;
        protected SubscriptionFetcher<T> Fetcher;

        protected DatabaseSubscriptionProcessor(ServerStore server, DocumentDatabase database, SubscriptionConnection connection) :
            base(server, database, connection)
        {
            Logger = LoggingSource.Instance.GetLogger<DatabaseSubscriptionProcessor<T>>(Database.Name);
        }

        public override IDisposable InitializeForNewBatch(ClusterOperationContext clusterContext, out SubscriptionIncludeCommands includesCommands)
        {
            var release = base.InitializeForNewBatch(clusterContext, out includesCommands);

            try
            {
                Fetcher = CreateFetcher();
                Fetcher.Initialize(clusterContext, DocsContext, Active);
                return release;
            }
            catch
            {
                release.Dispose();
                throw;
            }
        }

        protected (Document Doc, Exception Exception) GetBatchItem(T item)
        {
            if (ShouldSend(item, out var reason, out var exception, out var result))
            {
                if (IncludesCmd != null && Run != null)
                    IncludesCmd.AddRange(Run.Includes, result.Id);

                if (result.Data != null)
                    Fetcher.MarkDocumentSent();

                return (result, null);
            }

            if (Logger.IsInfoEnabled) 
                Logger.Info(reason, exception);

            if (exception != null)
            {
                if (result.Data != null)
                    Fetcher.MarkDocumentSent();

                return (result, exception);
            }

            result.Data = null;
            return (result, null);
        }

        protected abstract SubscriptionFetcher<T> CreateFetcher();

        protected abstract bool ShouldSend(T item, out string reason, out Exception exception, out Document result);
    }

    public abstract class DatabaseSubscriptionProcessor : SubscriptionProcessor
    {
        protected readonly Size MaximumAllowedMemory;
        protected readonly IJavaScriptOptions _jsOptions;
        protected readonly DocumentDatabase Database;
        protected DocumentsOperationContext DocsContext;
        protected SubscriptionConnectionsState SubscriptionConnectionsState;
        protected HashSet<long> Active;

        public SubscriptionPatchDocument Patch;
        protected ISingleRun Run;
        private ReturnRun? _returnRun;

        protected DatabaseSubscriptionProcessor(ServerStore server, DocumentDatabase database, SubscriptionConnection connection) : base(server, connection)
        {
            Database = database;
            MaximumAllowedMemory = new Size((Database.Is32Bits ? 4 : 32) * Voron.Global.Constants.Size.Megabyte, SizeUnit.Bytes);
            _jsOptions = database.Configuration.JavaScript;
        }
        
        public override void InitializeProcessor()
        {
            base.InitializeProcessor();
            
            SubscriptionConnectionsState = Database.SubscriptionStorage.Subscriptions[Connection.SubscriptionId];
            Active = SubscriptionConnectionsState.GetActiveBatches();
        }

        public override IDisposable InitializeForNewBatch(
            ClusterOperationContext clusterContext,
            out SubscriptionIncludeCommands includesCommands)
        {
            var release = Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocsContext);
            try
            {
                DocsContext.OpenReadTransaction();
                base.InitializeForNewBatch(clusterContext, out includesCommands);
                return release;
            }
            catch
            {
                release.Dispose();
                throw;
            }
        }
        

        protected override SubscriptionIncludeCommands CreateIncludeCommands()
        {
            var includeCommands = new SubscriptionIncludeCommands();

            if (Connection.SupportedFeatures.Subscription.Includes)
                includeCommands.IncludeDocumentsCommand = new IncludeDocumentsCommand(Database.DocumentsStorage, DocsContext, Connection.Subscription.Includes,
                    isProjection: string.IsNullOrWhiteSpace(Connection.Subscription.Script) == false);
            if (Connection.SupportedFeatures.Subscription.CounterIncludes && Connection.Subscription.CounterIncludes != null)
                includeCommands.IncludeCountersCommand = new IncludeCountersCommand(Database, DocsContext, Connection.Subscription.CounterIncludes);
            if (Connection.SupportedFeatures.Subscription.TimeSeriesIncludes && Connection.Subscription.TimeSeriesIncludes != null)
                includeCommands.IncludeTimeSeriesCommand = new IncludeTimeSeriesCommand(DocsContext, Connection.Subscription.TimeSeriesIncludes.TimeSeries);

            return includeCommands;
        }

        protected void InitializeScript()
        {
            if (Patch == null)
                return;

            if (_returnRun != null)
                return; // already init

            _returnRun = Database.Scripts.GetScriptRunner(Patch, readOnly: true, out Run);
        }

        private protected class ProjectionMetadataModifier : IResultModifier
        {
            public static readonly ProjectionMetadataModifier Instance = new ProjectionMetadataModifier();

            private ProjectionMetadataModifier()
            {
            }

            public void Modify<T>(T json, IJsEngineHandle<T> engine) where T : struct, IJsHandle<T>
            {
                using (var jsMetadata = json.GetProperty(Constants.Documents.Metadata.Key))
                {
                    if (!jsMetadata.IsObject)
                    {
                        using (var jsMetadataNew = engine.CreateObject())
                            jsMetadata.Set(jsMetadataNew);
                        json.SetProperty(Constants.Documents.Metadata.Key, jsMetadata.Clone(), throwOnError: false);
                    }

                    jsMetadata.SetProperty(Constants.Documents.Metadata.Projection, engine.CreateValue(true), throwOnError: false);
                }
            }
        }

        public abstract long GetLastItemEtag(DocumentsOperationContext context, string collection);

        public override void Dispose()
        {
            base.Dispose();
            _returnRun?.Dispose();
        }
    }
}
