﻿using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.TransactionsRecording
{
    public class StartTransactionsRecordingOperation : IMaintenanceOperation
    {
        private readonly string _filePath;

        public StartTransactionsRecordingOperation(string filePath)
        {
            _filePath = filePath;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new StartTransactionsRecordingCommand(_filePath);
        }

        private class StartTransactionsRecordingCommand : RavenCommand
        {
            private readonly string _filePath;

            public StartTransactionsRecordingCommand(string filePath)
            {
                _filePath = filePath;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/transactions/start-recording";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(stream =>
                    {
                        var jsonReaderObject = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(
                                new Parameters { File = _filePath },
                                ctx
                            );
                        ctx.Write(stream, jsonReaderObject);
                    })
                };
                return request;
            }
        }

        public class Parameters
        {
            public string File { get; set; }
        }
    }
}
