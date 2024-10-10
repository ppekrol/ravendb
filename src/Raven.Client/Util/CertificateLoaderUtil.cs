#if NET9_0_OR_GREATER2
#define FEATURE_X509CERTIFICATELOADER_SUPPORT
#endif

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ConstrainedExecution;
using System.Security.Cryptography.X509Certificates;
using Raven.Client.Logging;
using Sparrow.Logging;

namespace Raven.Client.Util;

internal static class CertificateLoaderUtil
{
    private static readonly IRavenLogger Logger = RavenLogManager.Instance.GetLoggerForClient(typeof(CertificateLoaderUtil));

    private static bool FirstTime = true;

    public static X509KeyStorageFlags FlagsForExport => X509KeyStorageFlags.Exportable;

    public static X509KeyStorageFlags FlagsForPersist => X509KeyStorageFlags.Exportable | X509KeyStorageFlags.PersistKeySet;

    public static void ImportWithPrivateKey(X509Certificate2Collection collection, byte[] rawData, string password, X509KeyStorageFlags? flags)
    {
        DebugAssertDoesntContainKeySet(flags);
        var f = AddUserKeySet(flags);

        Exception exception = null;
        try
        {
            ImportCertificate(collection, rawData, password, f);
        }
        catch (Exception e)
        {
            exception = e;
            f = AddMachineKeySet(flags);
            ImportCertificate(collection, rawData, password, f);
        }

        LogIfNeeded(nameof(ImportWithPrivateKey), f, exception);
        return;

        static void ImportCertificate(X509Certificate2Collection collection, byte[] data, string password, X509KeyStorageFlags keyStorageFlags)
        {
#if FEATURE_X509CERTIFICATELOADER_SUPPORT
            collection.Add(X509CertificateLoader.LoadPkcs12(data, password, keyStorageFlags));
#else
            collection.Import(data, password, keyStorageFlags);
#endif
        }
    }

    public static void ImportWithoutPrivateKey(X509Certificate2Collection collection, byte[] rawData)
    {
#if FEATURE_X509CERTIFICATELOADER_SUPPORT
        collection.Add(X509CertificateLoader.LoadCertificate(rawData));
#else
        collection.Import(rawData);
#endif
    }

    internal static X509Certificate2 CreateCertificateWithPrivateKey(byte[] rawData, string password, X509KeyStorageFlags? flags)
    {
#if FEATURE_X509CERTIFICATELOADER_SUPPORT
        return CreateCertificate(f => X509CertificateLoader.LoadPkcs12(rawData, password, f), flags);
#else
        return CreateCertificate(f => new X509Certificate2(rawData, password, f), flags);
#endif
    }

    internal static X509Certificate2 CreateCertificateWithoutPrivateKey(byte[] rawData)
    {
#if FEATURE_X509CERTIFICATELOADER_SUPPORT
        return CreateCertificate(f => X509CertificateLoader.LoadCertificate(rawData), flags: null);
#else
        return CreateCertificate(f => new X509Certificate2(rawData), flags: null);
#endif
    }

    internal static X509Certificate2 CreateCertificateWithPrivateKey(string fileName, string password, X509KeyStorageFlags? flags)
    {
#if FEATURE_X509CERTIFICATELOADER_SUPPORT
        return CreateCertificate(f => X509CertificateLoader.LoadPkcs12FromFile(fileName, password, f), flags);
#else
        return CreateCertificate(f => new X509Certificate2(fileName, password, f), flags);
#endif
    }

    internal static X509Certificate2 CreateCertificateWithoutPrivateKey(string fileName)
    {
#if FEATURE_X509CERTIFICATELOADER_SUPPORT
        return CreateCertificate(f => X509CertificateLoader.LoadCertificateFromFile(fileName), flags: null);
#else
        return CreateCertificate(f => new X509Certificate2(fileName), flags: null);
#endif
    }

    private static X509Certificate2 CreateCertificate(Func<X509KeyStorageFlags, X509Certificate2> creator, X509KeyStorageFlags? flags)
    {
        DebugAssertDoesntContainKeySet(flags);
        var f = AddUserKeySet(flags);

        Exception exception = null;
        X509Certificate2 certificate;
        try
        {
            certificate = creator(f);
        }
        catch (Exception e)
        {
            exception = e;
            f = AddMachineKeySet(flags);
            certificate = creator(f);
        }

        LogIfNeeded(nameof(CreateCertificate), f, exception);

        CertificateCleaner.RegisterForDisposalDuringFinalization(certificate);

        return certificate;
    }

    private static X509KeyStorageFlags AddUserKeySet(X509KeyStorageFlags? flag)
    {
        return (flag ?? X509KeyStorageFlags.DefaultKeySet) | X509KeyStorageFlags.UserKeySet;
    }

    private static X509KeyStorageFlags AddMachineKeySet(X509KeyStorageFlags? flag)
    {
        return (flag ?? X509KeyStorageFlags.DefaultKeySet) | X509KeyStorageFlags.MachineKeySet;
    }

    [Conditional("DEBUG")]
    private static void DebugAssertDoesntContainKeySet(X509KeyStorageFlags? flags)
    {
        const X509KeyStorageFlags keyStorageFlags =
#if NETCOREAPP3_1_OR_GREATER 
            X509KeyStorageFlags.EphemeralKeySet |
#endif
            X509KeyStorageFlags.UserKeySet |
            X509KeyStorageFlags.MachineKeySet;

        Debug.Assert(flags.HasValue == false || (flags.Value & keyStorageFlags) == 0);
    }

    private static void LogIfNeeded(string method, X509KeyStorageFlags flags, Exception exception)
    {
        if (FirstTime)
        {
            FirstTime = false;
            if (Logger.IsWarnEnabled)
                Logger.Warn(CreateMsg(), exception);
        }
        else
        {
            if (Logger.IsDebugEnabled)
                Logger.Debug(CreateMsg(), exception);
        }

        string CreateMsg()
        {
            return $"{nameof(CertificateLoaderUtil)}.{method} - Flags used {flags}";
        }
    }

    private sealed class CertificateCleaner : CriticalFinalizerObject
    {
        private X509Certificate2 _certificate;
        private static readonly ConditionalWeakTable<X509Certificate2, CertificateCleaner> AssociateLifetimes = new();

        public static void RegisterForDisposalDuringFinalization(X509Certificate2 cert)
        {
            var cleaner = AssociateLifetimes.GetOrCreateValue(cert);
            cleaner!._certificate = cert;
        }

        ~CertificateCleaner() => _certificate?.Dispose();
    }
}
