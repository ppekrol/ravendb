using System;
using Sparrow.Json.Parsing;

namespace Raven.Client.Exceptions.Compilation
{
    public abstract class CompilationException : RavenException
    {
        protected CompilationException()
        {
        }

        protected CompilationException(string message)
            : base(message)
        {
        }

        protected CompilationException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }

    public abstract class CompilationDiagnostic
    {
        public string Id { get; set; }

        public string Message { get; set; }

        public CompilationDiagnosticSeverity Severity { get; set; }

        public CompilationDiagnosticLocation Location { get; set; }

        protected CompilationDiagnostic(string id, string message, CompilationDiagnosticSeverity severity, CompilationDiagnosticLocation location)
        {
            Id = id;
            Message = message;
            Severity = severity;
            Location = location;
        }

        public override string ToString()
        {
            return $"{Location} {Severity} {Id}: {Message}";
        }

        internal DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(Message)] = Message,
                [nameof(Severity)] = Severity,
                [nameof(Location)] = Location?.ToJson()
            };
        }
    }

    public class CompilationDiagnosticLocation
    {
        public int StartLine { get; set; }
        public int StartCharacter { get; set; }

        public CompilationDiagnosticLocation(int startLine, int startCharacter)
        {
            StartLine = startLine;
            StartCharacter = startCharacter;
        }

        public override string ToString()
        {
            return $"({StartLine}, {StartCharacter})";
        }

        internal DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(StartLine)] = StartLine,
                [nameof(StartCharacter)] = StartCharacter
            };
        }
    }

    public enum CompilationDiagnosticSeverity
    {
        /// <summary>
        /// Something that is an issue, as determined by some authority,
        /// but is not surfaced through normal means.
        /// There may be different mechanisms that act on these issues.
        /// </summary>
        Hidden = 0,

        /// <summary>
        /// Information that does not indicate a problem (i.e. not prescriptive).
        /// </summary>
        Info = 1,

        /// <summary>
        /// Something suspicious but allowed.
        /// </summary>
        Warning = 2,

        /// <summary>
        /// Something not allowed by the rules of the language or other authority.
        /// </summary>
        Error = 3,
    }
}
