using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CliWrap;

namespace Sparrow.Server.Platform
{
    public static class RavenProcess
    {
        public static ProcessResult<string> ExecuteWithString(string command, string arguments = null, int? executionTimeoutInMs = null)
        {
            return ExecuteWithStringAsync(command, arguments, ToTimeSpan(executionTimeoutInMs)).Result;
        }

        public static ProcessResult<string> ExecuteWithString(string command, string arguments = null, TimeSpan? executionTimeout = null)
        {
            return ExecuteWithStringAsync(command, arguments, executionTimeout).Result;
        }

        public static Task<ProcessResult<string>> ExecuteWithStringAsync(string command, string arguments = null, int? executionTimeoutInMs = null)
        {
            return ExecuteWithStringAsync(command, arguments, ToTimeSpan(executionTimeoutInMs));
        }

        public static async Task<ProcessResult<string>> ExecuteWithStringAsync(string command, string arguments = null, TimeSpan? executionTimeout = null)
        {
            var outputBuilder = new StringBuilder();
            var errorBuilder = new StringBuilder();

            var result = await ExecuteInternal(PipeTarget.ToStringBuilder(outputBuilder), PipeTarget.ToStringBuilder(errorBuilder), command, arguments, executionTimeout).ConfigureAwait(false);

            return new ProcessResult<string>(outputBuilder.ToString(), errorBuilder.ToString(), result);
        }

        public static ProcessResult<MemoryStream> ExecuteWithStream(string command, string arguments = null, int? executionTimeoutInMs = null)
        {
            return ExecuteWithStream(command, arguments, ToTimeSpan(executionTimeoutInMs));
        }

        public static ProcessResult<MemoryStream> ExecuteWithStream(string command, string arguments = null, TimeSpan? executionTimeout = null)
        {
            return ExecuteWithStreamAsync(command, arguments, executionTimeout).Result;
        }

        public static Task<ProcessResult<MemoryStream>> ExecuteWithStreamAsync(string command, string arguments = null, int? executionTimeoutInMs = null)
        {
            return ExecuteWithStreamAsync(command, arguments, ToTimeSpan(executionTimeoutInMs));
        }

        public static async Task<ProcessResult<MemoryStream>> ExecuteWithStreamAsync(string command, string arguments = null, TimeSpan? executionTimeout = null)
        {
            var outputStream = new MemoryStream();
            var errorBuilder = new StringBuilder();

            var result = await ExecuteInternal(PipeTarget.ToStream(outputStream), PipeTarget.ToStringBuilder(errorBuilder), command, arguments, executionTimeout).ConfigureAwait(false);

            return new ProcessResult<MemoryStream>(outputStream, errorBuilder.ToString(), result);
        }

        private static async Task<CommandResult> ExecuteInternal(PipeTarget outputTarget, PipeTarget errorTarget, string command, string arguments, TimeSpan? executionTimeout)
        {
            if (string.IsNullOrWhiteSpace(command))
                throw new ArgumentException($"'{nameof(command)}' cannot be null or whitespace", nameof(command));

            using (var cts = new CancellationTokenSource(executionTimeout ?? TimeSpan.MaxValue))
            {
                using (var processTask = Cli.Wrap(command)
                    .WithArguments(arguments)
                    .WithStandardOutputPipe(outputTarget)
                    .WithStandardErrorPipe(errorTarget)
                    .ExecuteAsync(cts.Token))
                {
                    return await processTask.Task.ConfigureAwait(false);
                }
            }
        }

        private static TimeSpan? ToTimeSpan(int? milliseconds)
        {
            if (milliseconds.HasValue == false)
                return null;

            return TimeSpan.FromMilliseconds(milliseconds.Value);
        }

        public class ProcessResult<TResult> : CommandResult
        {
            public ProcessResult(TResult standardOutput, string errorOutput, CommandResult commandResult)
                : base(commandResult.ExitCode, commandResult.StartTime, commandResult.ExitTime)
            {
                StandardOutput = standardOutput;
                ErrorOutput = errorOutput;
            }

            public TResult StandardOutput { get; }

            public string ErrorOutput { get; }
        }
    }
}
