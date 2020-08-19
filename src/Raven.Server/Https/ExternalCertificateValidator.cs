using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Http;
using Sparrow.Logging;
using Sparrow.Server.Platform;
using Sparrow.Utils;

namespace Raven.Server.Https
{
    public class ExternalCertificateValidator
    {
        private readonly RavenServer _server;
        private readonly Logger _logger;

        private ConcurrentDictionary<Key, Task<CachedValue>> _externalCertificateValidationCallbackCache;

        public ExternalCertificateValidator(RavenServer server, Logger logger)
        {
            _server = server;
            _logger = logger;
        }

        public void Initialize()
        {
            if (string.IsNullOrEmpty(_server.Configuration.Security.CertificateValidationExec))
                return;

            _externalCertificateValidationCallbackCache = new ConcurrentDictionary<Key, Task<CachedValue>>();

            RequestExecutor.RemoteCertificateValidationCallback += (sender, cert, chain, errors) => ExternalCertificateValidationCallback(sender, cert, chain, errors, _logger);
        }

        public void ClearCache()
        {
            _externalCertificateValidationCallbackCache?.Clear();
        }

        private CachedValue CheckExternalCertificateValidation(string senderHostname, X509Certificate certificate, X509Chain chain, SslPolicyErrors sslPolicyErrors, Logger log)
        {
            var base64Cert = Convert.ToBase64String(certificate.Export(X509ContentType.Cert));

            var timeout = _server.Configuration.Security.CertificateValidationExecTimeout.AsTimeSpan;

            var args = $"{_server.Configuration.Security.CertificateValidationExecArguments ?? string.Empty} " +
                       $"{CommandLineArgumentEscaper.EscapeSingleArg(senderHostname)} " +
                       $"{CommandLineArgumentEscaper.EscapeSingleArg(base64Cert)} " +
                       $"{CommandLineArgumentEscaper.EscapeSingleArg(sslPolicyErrors.ToString())}";

            var result = RavenProcess.ExecuteWithString(_server.Configuration.Security.CertificateValidationExec, args, timeout);

            // Can have exit code 0 (success) but still get errors. We log the errors anyway.
            if (log.IsOperationsEnabled)
                log.Operations($"Executing '{_server.Configuration.Security.CertificateValidationExec} {args}' took {result.RunTime.TotalMilliseconds:#,#;;0} ms. Exit code: {result.ExitCode}{Environment.NewLine}Output: {result.StandardOutput}{Environment.NewLine}Errors: {result.ErrorOutput}{Environment.NewLine}");

            if (result.ExitCode != 0)
            {
                throw new InvalidOperationException(
                    $"Command or executable '{_server.Configuration.Security.CertificateValidationExec} {args}' failed. Exit code: {result.ExitCode}{Environment.NewLine}Output: {result.StandardOutput}{Environment.NewLine}Errors: {result.ErrorOutput}{Environment.NewLine}");
            }

            if (bool.TryParse(result.StandardOutput, out bool boolResult) == false)
            {
                throw new InvalidOperationException(
                    $"Cannot parse to boolean the result of Command or executable '{_server.Configuration.Security.CertificateValidationExec} {args}'. Exit code: {result.ExitCode}{Environment.NewLine}Output: {result.StandardOutput}{Environment.NewLine}Errors: {result.ErrorOutput}{Environment.NewLine}");
            }

            return new CachedValue { Valid = boolResult, Until = boolResult ? DateTime.UtcNow.AddMinutes(15) : DateTime.UtcNow.AddSeconds(30) };
        }

        public bool ExternalCertificateValidationCallback(object sender, X509Certificate certificate, X509Chain chain, SslPolicyErrors sslPolicyErrors, Logger log)
        {
            var senderHostname = RequestExecutor.ConvertSenderObjectToHostname(sender);

            var cacheKey = new Key(senderHostname, certificate.GetCertHashString(), sslPolicyErrors);

            Task<CachedValue> task;
            if (_externalCertificateValidationCallbackCache.TryGetValue(cacheKey, out var existingTask) == false)
            {
                task = new Task<CachedValue>(() => CheckExternalCertificateValidation(senderHostname, certificate, chain, sslPolicyErrors, log));
                existingTask = _externalCertificateValidationCallbackCache.GetOrAdd(cacheKey, task);
                if (existingTask == task)
                {
                    task.Start();

                    if (_externalCertificateValidationCallbackCache.Count > 50)
                    {
                        foreach (var item in _externalCertificateValidationCallbackCache.Where(x => x.Value.IsCompleted).OrderBy(x => x.Value.Result.Until).Take(25))
                        {
                            _externalCertificateValidationCallbackCache.TryRemove(item.Key, out _);
                        }
                    }
                }
            }

            CachedValue cachedValue;
            try
            {
                cachedValue = existingTask.Result;
            }
            catch
            {
                _externalCertificateValidationCallbackCache.TryRemove(cacheKey, out _);
                throw;
            }

            if (_server.Time.GetUtcNow() < cachedValue.Until)
                return cachedValue.Valid;

            var cachedValueNext = cachedValue.Next;
            if (cachedValueNext != null)
                return ReturnTaskValue(cachedValueNext);

            task = new Task<CachedValue>(() =>
                CheckExternalCertificateValidation(senderHostname, certificate, chain, sslPolicyErrors, log));

            var nextTask = Interlocked.CompareExchange(ref cachedValue.Next, task, null);
            if (nextTask != null)
                return ReturnTaskValue(nextTask);

            task.ContinueWith(done =>
            {
                _externalCertificateValidationCallbackCache.TryUpdate(cacheKey, done, existingTask);
            }, TaskContinuationOptions.OnlyOnRanToCompletion);

            task.Start();

            return cachedValue.Valid; // we are computing this, but may take some time, let's use cached value for now

            bool ReturnTaskValue(Task<CachedValue> task)
            {
                if (task.IsCompletedSuccessfully)
                    return task.Result.Valid;

                // not done yet? return the cached value
                return cachedValue.Valid;
            }
        }

        private class CachedValue
        {
            public DateTime Until;
            public bool Valid;

            public Task<CachedValue> Next;
        }

        private class Key
        {
            public readonly string Host;
            public readonly string Cert;
            public readonly SslPolicyErrors Errors;

            public Key(string host, string cert, SslPolicyErrors errors)
            {
                Host = host;
                Cert = cert;
                Errors = errors;
            }

            protected bool Equals(Key other)
            {
                return Host == other.Host && Cert == other.Cert && Errors == other.Errors;
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj))
                    return false;
                if (ReferenceEquals(this, obj))
                    return true;
                if (obj.GetType() != this.GetType())
                    return false;
                return Equals((Key)obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    var hashCode = (Host != null ? Host.GetHashCode() : 0);
                    hashCode = (hashCode * 397) ^ (Cert != null ? Cert.GetHashCode() : 0);
                    hashCode = (hashCode * 397) ^ (int)Errors;
                    return hashCode;
                }
            }
        }
    }
}
