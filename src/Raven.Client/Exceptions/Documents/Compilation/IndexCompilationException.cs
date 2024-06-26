using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Raven.Client.Exceptions.Compilation;
using Raven.Client.Extensions;
using Sparrow.Json.Parsing;

namespace Raven.Client.Exceptions.Documents.Compilation
{
    public sealed class IndexCompilationException : CompilationException
    {
        public IndexCompilationException()
        {
        }

        public IndexCompilationException(string message)
            : base(message)
        {
        }

        public IndexCompilationException(string message, Exception inner)
            : base(message, inner)
        {
        }

        /// <summary>
        /// Indicates which property caused error (Maps, Reduce).
        /// </summary>
        public string IndexDefinitionProperty;

        /// <summary>
        /// Value of a problematic property.
        /// </summary>
        public string ProblematicText;

        public string Code;

        public List<IndexCompilationDiagnostic> Diagnostics;

        public override string ToString()
        {
            return this.ExceptionToString(description =>
            {
                description.AppendLine();

                if (IndexDefinitionProperty != null)
                    description
                        .AppendFormat("IndexDefinitionProperty='{0}'", IndexDefinitionProperty)
                        .AppendLine();

                if (ProblematicText != null)
                    description
                        .AppendFormat("ProblematicText='{0}'", ProblematicText)
                        .AppendLine();

                if (Diagnostics != null)
                {
                    foreach (var diagnostic in Diagnostics)
                        description
                            .AppendFormat("Diagnostic='{0}'", diagnostic)
                            .AppendLine();
                }

                if (Code != null)
                    description
                        .AppendFormat("Code='{0}{1}{2}'", Environment.NewLine, Code, Environment.NewLine)
                        .AppendLine();
            });
        }

#if !NETSTANDARD2_0
        [DoesNotReturn]
#endif
        internal static void Throw(string name, string code, List<IndexCompilationDiagnostic> diagnostics)
        {
            throw new IndexCompilationException($"Failed to compile index '{name}'.")
            {
                Code = code,
                Diagnostics = diagnostics
            };
        }

        internal void FillJson(DynamicJsonValue json)
        {
            json[nameof(IndexDefinitionProperty)] = IndexDefinitionProperty;
            json[nameof(ProblematicText)] = ProblematicText;
            json[nameof(Code)] = Code;

            DynamicJsonArray diagnosticArray = null;
            if (Diagnostics != null)
            {
                diagnosticArray = new DynamicJsonArray();
                foreach (var diagnostic in Diagnostics)
                    diagnosticArray.Add(diagnostic.ToJson());
            }

            json[nameof(Diagnostics)] = diagnosticArray;
        }
    }

    public class IndexCompilationDiagnostic : CompilationDiagnostic
    {
        public IndexCompilationDiagnostic(string id, string message, CompilationDiagnosticSeverity severity, CompilationDiagnosticLocation location)
            : base(id, message, severity, location)
        {
        }
    }
}
