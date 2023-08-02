// -----------------------------------------------------------------------
//  <copyright file="RavenStorageClient.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Threading;
using Raven.Server.Utils;

namespace Raven.Server.Documents.PeriodicBackup
{
    internal abstract class RavenStorageClient : IDisposable
    {
        private readonly List<RavenHttpClient> _clients = new();
        protected readonly CancellationToken CancellationToken;
        protected readonly Progress Progress;
        protected const int MaxRetriesForMultiPartUpload = 5;

        protected RavenStorageClient(Progress progress, CancellationToken? cancellationToken)
        {
            Debug.Assert(progress == null || (progress.UploadProgress != null && progress.OnUploadProgress != null));

            Progress = progress;
            CancellationToken = cancellationToken ?? CancellationToken.None;
        }

        protected RavenHttpClient GetClient(TimeSpan? timeout = null)
        {
            var handler = new HttpClientHandler
            {
                AutomaticDecompression = System.Net.DecompressionMethods.None
            };

            var client = new RavenHttpClient(handler)
            {
                Timeout = timeout ?? TimeSpan.FromSeconds(120)
            };

            _clients.Add(client);

            return client;
        }

        public virtual void Dispose()
        {
            var exceptions = new List<Exception>();

            foreach (var client in _clients)
            {
                try
                {
                    client.Dispose();
                }
                catch (Exception e)
                {
                    exceptions.Add(e);
                }
            }

            if (exceptions.Count > 0)
                throw new AggregateException(exceptions);
        }

        internal sealed class Blob : IDisposable
        {
            private IDisposable _toDispose;

            public Blob(Stream data, IDictionary<string, string> metadata, IDisposable toDispose = null)
            {
                Data = data ?? throw new ArgumentNullException(nameof(data));
                Metadata = metadata;
                _toDispose = toDispose;
            }

            public Stream Data { get; }

            public IDictionary<string, string> Metadata { get; }

            public void Dispose()
            {
                _toDispose?.Dispose();
                _toDispose = null;
            }
        }

        internal sealed class ListBlobResult
        {
            public IEnumerable<BlobProperties> List { get; set; }

            public string ContinuationToken { get; set; }
        }

        internal sealed class BlobProperties
        {
            public string Name { get; set; }
        }
    }
}
