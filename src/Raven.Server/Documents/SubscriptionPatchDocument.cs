using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents
{
    public class SubscriptionPatchDocument : PatchDocument
    {
        private readonly PatchRequest _patchRequest;

        public SubscriptionPatchDocument(DocumentDatabase database, string filterJavaScript) : base(database)
        {
            _patchRequest = new PatchRequest
            {
                Script = filterJavaScript
            };

        }

        //protected override void CustomizeEngine(Engine engine, PatcherOperationScope scope)
        //{
        //    // override to make it "no-op"
        //}

        public bool MatchCriteria(DocumentsOperationContext context, Document document)
        {
            return false;
            //var actualPatchResult = ApplySingleScript(context, document, false, _patchRequest).ActualPatchResult;
            //return actualPatchResult.AsBoolean();
        }
    }
}
