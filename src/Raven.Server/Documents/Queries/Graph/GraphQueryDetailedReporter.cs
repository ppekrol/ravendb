using System.Collections.Generic;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Queries.Graph
{
    public class GraphQueryDetailedReporter : QueryPlanVisitor
    {
        private AsyncBlittableJsonTextWriter _writer;
        private DocumentsOperationContext _ctx;

        public GraphQueryDetailedReporter(AsyncBlittableJsonTextWriter writer, DocumentsOperationContext ctx)
        {
            _writer = writer;
            _ctx = ctx;
        }

        public override void VisitQueryQueryStep(QueryQueryStep qqs)
        {
            _writer.WriteStartObjectAsync();
            _writer.WritePropertyNameAsync("Type");
            _writer.WriteStringAsync("QueryQueryStep");
            _writer.WriteCommaAsync();
            _writer.WritePropertyNameAsync("Query");
            _writer.WriteStringAsync(qqs.Query.ToString());
            _writer.WriteCommaAsync();            
            WriteIntermidiateResults(qqs.IntermediateResults);
            _writer.WriteEndObjectAsync();
        }

        private void WriteIntermidiateResults(List<GraphQueryRunner.Match> matches)
        {
            _writer.WritePropertyNameAsync("Results");
            _writer.WriteStartArrayAsync();
            var first = true;
            foreach (var match in matches)
            {
                if (first == false)
                {
                    _writer.WriteCommaAsync();
                }

                first = false;
                var djv = new DynamicJsonValue();
                match.PopulateVertices(djv);
                _writer.WriteObjectAsync(_ctx.ReadObject(djv, null));
            }

            _writer.WriteEndArrayAsync();
        }

        public override void VisitEdgeQueryStep(EdgeQueryStep eqs)
        {
            _writer.WriteStartObjectAsync();
            _writer.WritePropertyNameAsync("Type");
            _writer.WriteStringAsync("EdgeQueryStep");
            _writer.WriteCommaAsync();
            _writer.WritePropertyNameAsync("Left");
            Visit(eqs.Left);
            _writer.WriteCommaAsync();
            _writer.WritePropertyNameAsync("Right");
            Visit(eqs.Right);
            _writer.WriteEndObjectAsync();
        }

        public override void VisitCollectionDestinationQueryStep(CollectionDestinationQueryStep cdqs)
        {
            _writer.WriteStartObjectAsync();
            _writer.WritePropertyNameAsync("Type");
            _writer.WriteStringAsync("CollectionDestinationQueryStep");
            _writer.WriteCommaAsync();
            _writer.WritePropertyNameAsync("Collection");
            _writer.WriteStringAsync(cdqs.CollectionName);
            _writer.WriteCommaAsync();
            WriteIntermidiateResults(cdqs.IntermediateResults);
            _writer.WriteEndObjectAsync();
        }

        public override void VisitIntersectionQueryStepExcept(IntersectionQueryStep<Except> iqse)
        {
            _writer.WriteStartObjectAsync();
            _writer.WritePropertyNameAsync("Type");
            _writer.WriteStringAsync("IntersectionQueryStep<Except>");
            _writer.WriteCommaAsync();
            _writer.WritePropertyNameAsync("Left");
            Visit(iqse.Left);
            _writer.WriteCommaAsync();
            _writer.WritePropertyNameAsync("Right");
            Visit(iqse.Right);
            _writer.WriteCommaAsync();
            WriteIntermidiateResults(iqse.IntermediateResults);
            _writer.WriteEndObjectAsync();
        }

        public override void VisitIntersectionQueryStepUnion(IntersectionQueryStep<Union> iqsu)
        {
            _writer.WriteStartObjectAsync();
            _writer.WritePropertyNameAsync("Type");
            _writer.WriteStringAsync("IntersectionQueryStep<Except>");
            _writer.WriteCommaAsync();
            _writer.WritePropertyNameAsync("Left");
            Visit(iqsu.Left);
            _writer.WriteCommaAsync();
            _writer.WritePropertyNameAsync("Right");
            Visit(iqsu.Right);
            _writer.WriteCommaAsync();
            WriteIntermidiateResults(iqsu.IntermediateResults);
            _writer.WriteEndObjectAsync();
        }

        public override void VisitIntersectionQueryStepIntersection(IntersectionQueryStep<Intersection> iqsi)
        {
            _writer.WriteStartObjectAsync();
            _writer.WritePropertyNameAsync("Type");
            _writer.WriteStringAsync("IntersectionQueryStep<Except>");
            _writer.WriteCommaAsync();
            _writer.WritePropertyNameAsync("Left");
            Visit(iqsi.Left);
            _writer.WriteCommaAsync();
            _writer.WritePropertyNameAsync("Right");
            Visit(iqsi.Right);
            _writer.WriteCommaAsync();
            WriteIntermidiateResults(iqsi.IntermediateResults);
            _writer.WriteEndObjectAsync();
        }

        public override void VisitRecursionQueryStep(RecursionQueryStep rqs)
        {
            _writer.WriteStartObjectAsync();
            _writer.WritePropertyNameAsync("Type");
            _writer.WriteStringAsync("RecursionQueryStep");
            _writer.WriteCommaAsync();
            _writer.WritePropertyNameAsync("Left");
            Visit(rqs.Left);
            _writer.WriteCommaAsync();
            _writer.WritePropertyNameAsync("Steps");
            _writer.WriteStartArrayAsync();
            var first = true;
            foreach (var step in rqs.Steps)
            {
                if (first == false)
                {
                    _writer.WriteCommaAsync();
                }

                first = false;
                Visit(step.Right);
            }
            _writer.WriteEndArrayAsync();
            _writer.WriteCommaAsync();
            Visit(rqs.GetNextStep());
            WriteIntermidiateResults(rqs.IntermediateResults);
            _writer.WriteEndObjectAsync();
        }

        public override void VisitEdgeMatcher(EdgeQueryStep.EdgeMatcher em)
        {
            _writer.WritePropertyNameAsync("Next");
            Visit(em._parent.Right);
            _writer.WriteCommaAsync();
        }
    }
}
