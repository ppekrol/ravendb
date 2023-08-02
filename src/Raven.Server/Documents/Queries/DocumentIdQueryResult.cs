﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.Queries
{
    internal sealed class DocumentIdQueryResult : DocumentQueryResult
    {
        private readonly DeterminateProgress _progress;
        private readonly Action<DeterminateProgress> _onProgress;
        private readonly OperationCancelToken _token;

        public readonly Queue<string> DocumentIds = new Queue<string>();

        public DocumentIdQueryResult(DeterminateProgress progress, Action<DeterminateProgress> onProgress, long? indexDefinitionRaftIndex, OperationCancelToken token) : base(indexDefinitionRaftIndex)
        {
            _progress = progress;
            _onProgress = onProgress;
            _token = token;
        }

        public override ValueTask AddResultAsync(Document result, CancellationToken token)
        {
            using (result)
            {
                _token.Delay();
                DocumentIds.Enqueue(result.Id);

                _progress.Total++;

                if (_progress.Total % 10_000 == 0)
                {
                    _onProgress.Invoke(_progress);
                }
            }

            return default;
        }
    }
}
