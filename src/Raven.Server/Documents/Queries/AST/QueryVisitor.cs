using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Sparrow;

namespace Raven.Server.Documents.Queries.AST
{
    internal abstract class QueryVisitor
    {
        public void Visit(Query q)
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();

            if (q.DeclaredFunctions != null)
            {
                VisitDeclaredFunctions(q.DeclaredFunctions);
            }

            if (q.From.From != null)
                VisitFromClause(q.From.From, q.From.Alias, q.From.Filter, q.From.Index);

            if (q.GroupBy != null)
            {
                VisitGroupByExpression(q.GroupBy);
            }

            if (q.Where != null)
            {
                VisitWhereClause(q.Where);
            }

            if (q.Filter != null)
            {
                VisitFilterClause(q.Filter);
            }

            if (q.OrderBy != null)
            {
                VisitOrderBy(q.OrderBy);
            }

            if (q.Load != null)
            {
                VisitLoad(q.Load);
            }

            if (q.Select != null && q.Select.Count > 0)
            {
                VisitSelect(q.Select, q.IsDistinct);
            }
            else if (q.Select == null && q.IsDistinct)
            {
                VisitSelectDistinct();
            }

            if (q.SelectFunctionBody.FunctionText != null)
            {
                VisitSelectFunctionBody(q.SelectFunctionBody.FunctionText);
            }

            if (q.UpdateBody != null)
            {
                VisitUpdate(q.UpdateBody);
            }

            if (q.Include != null)
            {
                VisitInclude(q.Include);
            }

            if (q.Limit != null)
            {
                VisitLimit(q.Limit);
            }

            if (q.Offset != null)
            {
                VisitOffset(q.Offset);
            }
        }

        public virtual void VisitFilterClause(QueryExpression filter)
        {
            VisitExpression(filter);
        }

        public virtual void VisitLimit(ValueExpression limit)
        {

        }

        public virtual void VisitOffset(ValueExpression offset)
        {

        }

        public virtual void VisitInclude(List<QueryExpression> includes)
        {
            foreach (var queryExpression in includes)
            {
                VisitExpression(queryExpression);
            }
        }

        public virtual void VisitUpdate(StringSegment update)
        {

        }

        public virtual void VisitSelectFunctionBody(StringSegment func)
        {

        }

        public virtual void VisitNegatedExpression(NegatedExpression expr)
        {
            VisitExpression(expr.Expression);
        }

        public virtual void VisitSelect(List<(QueryExpression Expression, StringSegment? Alias)> select, bool isDistinct)
        {
            foreach (var s in select)
            {
                VisitExpression(s.Expression);
            }
        }

        public virtual void VisitSelectDistinct()
        {

        }

        public virtual void VisitLoad(List<(QueryExpression Expression, StringSegment? Alias)> load)
        {
            foreach (var l in load)
            {
                VisitExpression(l.Expression);
            }
        }

        public virtual void VisitOrderBy(List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending)> orderBy)
        {
            foreach (var tuple in orderBy)
            {
                VisitExpression(tuple.Expression);
            }
        }

        public virtual void VisitDeclaredFunctions(Dictionary<string, DeclaredFunction> declaredFunctions)
        {
            foreach (var kvp in declaredFunctions)
            {
                VisitDeclaredFunction(kvp.Value.FunctionText);
            }
        }

        public virtual void VisitWhereClause(QueryExpression where)
        {
            VisitExpression(where);
        }

        protected virtual void VisitBinaryExpression(BinaryExpression @where)
        {
            switch (@where.Operator)
            {
                case OperatorType.Equal:
                case OperatorType.NotEqual:
                case OperatorType.LessThan:
                case OperatorType.GreaterThan:
                case OperatorType.LessThanEqual:
                case OperatorType.GreaterThanEqual:
                    VisitSimpleWhereExpression(@where);
                    break;
                case OperatorType.And:
                case OperatorType.Or:
                    VisitCompoundWhereExpression(@where);
                    break;
                default:
                    ThrowInvalidOperationType(@where);
                    break;
            }
        }

        public virtual void VisitCompoundWhereExpression(BinaryExpression @where)
        {
            VisitExpression(where.Left);
            VisitExpression(where.Right);
        }

        public void VisitExpression(QueryExpression expr)
        {
            switch (expr.Type)
            {
                case ExpressionType.Field:
                    VisitField((FieldExpression)expr);
                    break;
                case ExpressionType.Between:
                    VisitBetween((BetweenExpression)expr);
                    break;
                case ExpressionType.Binary:
                    VisitBinaryExpression((BinaryExpression)expr);
                    break;
                case ExpressionType.In:
                    VisitIn((InExpression)expr);
                    break;
                case ExpressionType.Value:
                    VisitValue((ValueExpression)expr);
                    break;
                case ExpressionType.Method:
                    VisitMethod((MethodExpression)expr);
                    break;
                case ExpressionType.True:
                    VisitTrue();
                    break;
                case ExpressionType.Negated:
                    VisitNegatedExpression((NegatedExpression)expr);
                    break;
                default:
                    GetValueThrowInvalidExprType(expr);
                    break;
            }
        }

        public virtual void VisitMethod(MethodExpression expr)
        {
            foreach (var expression in expr.Arguments)
            {
                VisitExpression(expression);
            }
        }

        public virtual void VisitValue(ValueExpression expr)
        {

        }

        public virtual void VisitIn(InExpression expr)
        {
            foreach (var value in expr.Values)
            {
                VisitExpression(value);
            }
        }

        public virtual void VisitBetween(BetweenExpression expr)
        {
            VisitExpression(expr.Source);
            VisitExpression(expr.Min);
            VisitExpression(expr.Max);
        }

        public virtual void VisitField(FieldExpression field)
        {

        }

        public virtual void VisitTrue()
        {

        }

        [DoesNotReturn]
        private static void GetValueThrowInvalidExprType(QueryExpression expr)
        {
            throw new ArgumentOutOfRangeException(expr.Type.ToString());
        }

        [DoesNotReturn]
        protected static void ThrowInvalidOperationType(BinaryExpression @where)
        {
            throw new ArgumentOutOfRangeException(@where.Operator.ToString());
        }

        public virtual void VisitSimpleWhereExpression(BinaryExpression expr)
        {
        }

        public virtual void VisitGroupByExpression(List<(QueryExpression Expression, StringSegment? Alias)> expressions)
        {

        }

        public virtual void VisitFromClause(FieldExpression from, StringSegment? alias, QueryExpression filter, bool index)
        {

        }

        public virtual void VisitDeclaredFunction(string func)
        {

        }
    }
}
