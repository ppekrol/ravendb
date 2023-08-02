﻿using System;
using System.Collections.Generic;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    internal abstract class CollectionNameRetrieverBase : CSharpSyntaxRewriter
    {
        public HashSet<Collection> Collections { get; protected set; }

        protected sealed class MethodSyntaxRewriter : CollectionNameRetrieverBase
        {
            private readonly string _nodePrefix;
            private readonly string _itemName;

            public MethodSyntaxRewriter(string nodePrefix, string itemName)
            {
                _nodePrefix = nodePrefix ?? throw new ArgumentNullException(nameof(nodePrefix));
                _itemName = itemName ?? throw new ArgumentNullException(nameof(itemName));
            }

            public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
            {
                if (Collections != null)
                    return node;

                var nodeToCheck = CollectionNameRetriever.UnwrapNode(node);

                var nodeAsString = nodeToCheck.Expression.ToString();
                if (nodeAsString.StartsWith(_nodePrefix, StringComparison.OrdinalIgnoreCase) == false)
                    return node;

                var nodeParts = nodeAsString.Split(new[] { "." }, StringSplitOptions.RemoveEmptyEntries);
                var nodePartsLength = nodeParts.Length;
                if (nodePartsLength < 2 || nodePartsLength > 4)
                    throw new NotImplementedException("Not supported syntax exception. This might be a bug.");

                var toRemove = 0;

                if (nodePartsLength == 2) // from ts in timeSeries.SelectMany
                {
                    if (nodeToCheck.Expression is MemberAccessExpressionSyntax expr)
                    {
                        if (expr.Expression is ElementAccessExpressionSyntax timeSeriesNameIndexer // from ts in timeSeries[@""][@""].SelectMany
                            && timeSeriesNameIndexer.Expression is ElementAccessExpressionSyntax collectionNameIndexer1)
                        {
                            Collections = new HashSet<Collection>
                            {
                                { new Collection(ExtractName(collectionNameIndexer1, "collection"), ExtractName(timeSeriesNameIndexer, _itemName)) }
                            };
                        }
                        else if (expr.Expression is ElementAccessExpressionSyntax collectionNameIndexer2)
                        {
                            Collections = new HashSet<Collection>
                            {
                                { new Collection(ExtractName(collectionNameIndexer2, "collection"), null) }
                            };
                        }

                        toRemove = expr.Expression.ToString().Length - _nodePrefix.Length;
                    }
                    else
                    {
                        return node;
                    }
                }
                else if (nodePartsLength == 4) // from ts in timeSeries.Companies.HeartRate.SelectMany
                {
                    var collectionName = nodeParts[1];
                    toRemove += collectionName.Length + 1;

                    var timeSeriesName = nodeParts[2];
                    toRemove += timeSeriesName.Length + 1;

                    Collections = new HashSet<Collection>
                    {
                        { new Collection(collectionName, timeSeriesName) }
                    };
                }
                else if (nodePartsLength == 3) // from ts in timeSeries.Companies.SelectMany
                {
                    if (nodeToCheck.Expression is MemberAccessExpressionSyntax expr)
                    {
                        if (expr.Expression is ElementAccessExpressionSyntax timeSeriesNameIndexer
                            && timeSeriesNameIndexer.Expression is MemberAccessExpressionSyntax collectionName1) // from ts in timeSeries.Companies[@""].SelectMany
                        {
                            Collections = new HashSet<Collection>
                            {
                                { new Collection(ExtractName(collectionName1), ExtractName(timeSeriesNameIndexer, _itemName)) }
                            };

                            toRemove = expr.Expression.ToString().Length - _nodePrefix.Length;
                        }
                        else if (expr.Expression is MemberAccessExpressionSyntax timeSeriesName
                            && timeSeriesName.Expression is ElementAccessExpressionSyntax collectionNameIndexer) // from ts in timeSeries[@""].HeartRate.SelectMany
                        {
                            Collections = new HashSet<Collection>
                            {
                                { new Collection(ExtractName(collectionNameIndexer, "collection"), ExtractName(timeSeriesName)) }
                            };

                            toRemove = expr.Expression.ToString().Length - _nodePrefix.Length;
                        }
                        else
                        {
                            var collectionName2 = nodeParts[1];
                            toRemove += collectionName2.Length + 1;

                            Collections = new HashSet<Collection>
                            {
                                { new Collection(collectionName2, null) }
                            };
                        }
                    }
                }

                if (nodeToCheck != node)
                    nodeAsString = node.Expression.ToString();

                // removing collection name: "timeSeries.Users.HeartRate.Select" => ".Select"
                nodeAsString = nodeAsString.Remove(0, toRemove + _nodePrefix.Length);
                nodeAsString = _nodePrefix + nodeAsString; // .Select => timeSeries.Select (normalizing timeSeries which could be lowercased)

                var newExpression = SyntaxFactory.ParseExpression(nodeAsString);
                return node.WithExpression(newExpression);
            }
        }

        protected sealed class QuerySyntaxRewriter : CollectionNameRetrieverBase
        {
            private readonly string _nodePrefix;
            private readonly string _itemName;

            public QuerySyntaxRewriter(string nodePrefix, string itemName)
            {
                _nodePrefix = nodePrefix ?? throw new ArgumentNullException(nameof(nodePrefix));
                _itemName = itemName ?? throw new ArgumentNullException(nameof(itemName));
            }

            public override SyntaxNode VisitFromClause(FromClauseSyntax node)
            {
                if (Collections != null)
                    return node;

                var nodeAsString = node.Expression.ToString();
                if (nodeAsString.StartsWith(_nodePrefix) == false)
                {
                    if (nodeAsString.StartsWith(_nodePrefix, StringComparison.OrdinalIgnoreCase) == false)
                        return node;

                    nodeAsString = nodeAsString.Substring(_nodePrefix.Length);
                    nodeAsString = _nodePrefix + nodeAsString;

                    var newExpression = SyntaxFactory.ParseExpression(nodeAsString); // normalizing timeSeries which could be lowercased
                    node = node.WithExpression(newExpression);
                }

                if (node.Expression is IdentifierNameSyntax) // from ts in timeSeries
                    return node;

                if (node.Expression is MemberAccessExpressionSyntax timeSeriesExpression)
                {
                    if (timeSeriesExpression.Expression is MemberAccessExpressionSyntax collectionExpression) // from ts in timeSeries.Companies.HeartRate
                    {
                        var timeSeriesIdentifier = collectionExpression.Expression as IdentifierNameSyntax;
                        if (string.Equals(timeSeriesIdentifier?.Identifier.Text, _nodePrefix, StringComparison.OrdinalIgnoreCase) == false)
                            return node;

                        Collections = new HashSet<Collection>
                        {
                            { new Collection(collectionExpression.Name.Identifier.Text, timeSeriesExpression.Name.Identifier.Text) }
                        };

                        return node.WithExpression(collectionExpression.Expression);
                    }
                    else if (timeSeriesExpression.Expression is IdentifierNameSyntax identifierNameSyntax) // from ts in timeSeries.Companies
                    {
                        Collections = new HashSet<Collection>
                        {
                            { new Collection(timeSeriesExpression.Name.Identifier.Text, null) }
                        };

                        return node.WithExpression(identifierNameSyntax);
                    }
                    else if (timeSeriesExpression.Expression is ElementAccessExpressionSyntax collectionNameIndexer) // from ts in timeSeries[@""].HeartRate
                    {
                        Collections = new HashSet<Collection>
                        {
                            { new Collection(ExtractName(collectionNameIndexer, "collection"), ExtractName(timeSeriesExpression)) }
                        };

                        return node.WithExpression(collectionNameIndexer.Expression);
                    }
                }
                else if (node.Expression is ElementAccessExpressionSyntax indexer)
                {
                    if (indexer.Expression is ElementAccessExpressionSyntax collectionNameIndexer) // from ts in timeSeries[@""][@""]
                    {
                        Collections = new HashSet<Collection>
                        {
                            { new Collection(ExtractName(collectionNameIndexer, "collection"), ExtractName(indexer, _itemName)) }
                        };

                        return node.WithExpression(collectionNameIndexer.Expression);
                    }
                    else if (indexer.Expression is MemberAccessExpressionSyntax collectionName) // from ts in timeSeries.Companies[@""]
                    {
                        Collections = new HashSet<Collection>
                        {
                            { new Collection(ExtractName(collectionName), ExtractName(indexer, _itemName)) }
                        };

                        return node.WithExpression(collectionName.Expression);
                    }
                    else if (indexer.Expression is IdentifierNameSyntax) // from ts in timeSeries[@""]
                    {
                        Collections = new HashSet<Collection>
                        {
                            { new Collection(ExtractName(indexer, "collection"), null) }
                        };

                        return node.WithExpression(indexer.Expression);
                    }
                }
                else if (node.Expression is InvocationExpressionSyntax invocation) // from ts in timeSeries.Companies.HeartRate.Where(x => true)
                {
                    var methodSyntax = new MethodSyntaxRewriter(_nodePrefix, _itemName);
                    var newExpression = (ExpressionSyntax)methodSyntax.VisitInvocationExpression(invocation);
                    Collections = methodSyntax.Collections;

                    return node.WithExpression(newExpression);
                }

                throw new NotImplementedException("Not supported syntax exception. This might be a bug.");
            }
        }

        private static string ExtractName(ElementAccessExpressionSyntax indexer, string name)
        {
            if (indexer.ArgumentList.Arguments.Count != 1)
                throw new NotSupportedException($"You can only pass one {name} name to the indexer.");

            if (indexer.ArgumentList.Arguments[0].Expression is LiteralExpressionSyntax les)
                return les.Token.ValueText;

            throw new NotSupportedException($"Could not exptract {name} name from: {indexer}");
        }

        private static string ExtractName(MemberAccessExpressionSyntax member)
        {
            return member.Name.Identifier.ValueText;
        }

        internal sealed class Collection
        {
            public readonly string CollectionName;

            public readonly string ItemName;

            public Collection(string collectionName, string itemName)
            {
                CollectionName = collectionName;
                ItemName = itemName;
            }

            public override bool Equals(object obj)
            {
                return obj is Collection collection &&
                       string.Equals(CollectionName, collection.CollectionName, StringComparison.OrdinalIgnoreCase) &&
                       string.Equals(ItemName, collection.ItemName, StringComparison.OrdinalIgnoreCase);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    var hashCode = CollectionName != null ? StringComparer.OrdinalIgnoreCase.GetHashCode(CollectionName) : 0;
                    hashCode = hashCode * 397 ^ (ItemName != null ? StringComparer.OrdinalIgnoreCase.GetHashCode(ItemName) : 0);
                    return hashCode;
                }
            }
        }
    }
}
