using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Quic;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using FastTests;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Raven.Client.Documents;
using Raven.Client.Exceptions.Security;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_17220 : RavenTestBase
{
    private static readonly Version Http3 = new Version(3, 0);

    // - the HTTP/3 client connects only to the first resolved address of a host name (no fallback to the other ones like with TCP)
    // - HTTP/3 requires an explicit port, with a dynamic one Kestrel binds the UDP socket to a different port than the TCP one
    private static string ServerUrl() => $"https://127.0.0.1:{GetReservedPort()}";

    public RavenDB_17220(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Security | RavenTestCategory.Certificates)]
    public async Task CanUseHttp3WithSecuredServer()
    {
        SkipIfQuicIsNotSupported();

        var certificates = Certificates.SetupServerAuthentication(Http3Settings(), ServerUrl());
        Assert.Equal(HttpProtocols.Http1AndHttp2AndHttp3, Server.EffectiveHttpProtocols);

        var dbName = GetDatabaseName();
        var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
        var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
        {
            [dbName] = DatabaseAccess.ReadWrite
        });

        var responseVersions = new ConcurrentBag<Version>();

        using (var store = GetDocumentStore(new Options
        {
            AdminCertificate = adminCert,
            ClientCertificate = userCert,
            ModifyDatabaseName = _ => dbName,
            ModifyDocumentStore = s =>
            {
                s.Conventions.HttpVersion = Http3;
                s.Conventions.HttpVersionPolicy = HttpVersionPolicy.RequestVersionExact;
                s.OnSucceedRequest += (_, args) => responseVersions.Add(args.Response.Version);
            }
        }))
        {
            // changes api uses web sockets, which are not supported over HTTP/3, so it has to fall back to HTTP/2
            var mre = new AsyncManualResetEvent();
            var changes = await store.Changes().EnsureConnectedNow();
            var forDocument = changes.ForDocument("users/1");
            forDocument.Subscribe(_ => mre.Set());
            await forDocument.EnsureSubscribedNow();

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Grisha" }, "users/1");
                session.Advanced.Attachments.Store("users/1", "file.bin", new MemoryStream(new byte[] { 1, 2, 3 }));
                await session.SaveChangesAsync();
            }

            Assert.True(await mre.WaitAsync(TimeSpan.FromSeconds(15)));

            await using (var bulk = store.BulkInsert())
            {
                for (var i = 0; i < 100; i++)
                    await bulk.StoreAsync(new User { Name = $"user-{i}" }, $"bulk/{i}");
            }

            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>("users/1");
                Assert.Equal("Grisha", user.Name);

                using (var attachment = await session.Advanced.Attachments.GetAsync("users/1", "file.bin"))
                using (var ms = new MemoryStream())
                {
                    await attachment.Stream.CopyToAsync(ms);
                    Assert.Equal(new byte[] { 1, 2, 3 }, ms.ToArray());
                }

                var count = await session.Query<User>().Customize(x => x.WaitForNonStaleResults()).CountAsync(x => x.Name.StartsWith("user-"));
                Assert.Equal(100, count);

                var streamed = 0;
                await using (var stream = await session.Advanced.StreamAsync<User>(startsWith: "bulk/"))
                {
                    while (await stream.MoveNextAsync())
                        streamed++;
                }

                Assert.Equal(100, streamed);
            }
        }

        Assert.NotEmpty(responseVersions);
        Assert.All(responseVersions, v => Assert.Equal(Http3, v));
    }

    [RavenFact(RavenTestCategory.Security | RavenTestCategory.Certificates)]
    public void AuthorizationIsEnforcedOverHttp3()
    {
        SkipIfQuicIsNotSupported();

        var certificates = Certificates.SetupServerAuthentication(Http3Settings(), ServerUrl());
        var dbName = GetDatabaseName();
        var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
        var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
        {
            [dbName] = DatabaseAccess.ReadWrite
        });

        using (var store = GetDocumentStore(new Options
        {
            AdminCertificate = adminCert,
            ClientCertificate = userCert,
            ModifyDatabaseName = _ => dbName,
            ModifyDocumentStore = UseHttp3
        }))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new User(), "users/1");
                session.SaveChanges();
            }

            // no access to other databases
            using (var session = store.OpenSession(GetDatabaseName()))
                Assert.Throws<AuthorizationException>(() => session.Load<User>("users/1"));
        }

        // not registered in the cluster
        using (var store = GetDocumentStore(new Options
        {
            AdminCertificate = adminCert,
            ClientCertificate = certificates.ClientCertificate3.Value,
            ModifyDatabaseName = _ => dbName,
            CreateDatabase = false,
            ModifyDocumentStore = UseHttp3
        }))
        {
            using (var session = store.OpenSession())
                Assert.Throws<AuthorizationException>(() => session.Load<User>("users/1"));
        }
    }

    [RavenFact(RavenTestCategory.Security | RavenTestCategory.Certificates)]
    public async Task CertificateRegisteredAfterHttp3ConnectionWasEstablishedIsAccepted()
    {
        SkipIfQuicIsNotSupported();

        var certificates = Certificates.SetupServerAuthentication(Http3Settings(), ServerUrl());
        var dbName = GetDatabaseName();
        var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

        using (var store = GetDocumentStore(new Options
        {
            AdminCertificate = adminCert,
            ClientCertificate = adminCert,
            ModifyDatabaseName = _ => dbName
        }))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new User { Name = "Grisha" }, "users/1");
                session.SaveChanges();
            }

            // the same client is used for both requests, so the second one reuses the QUIC connection which was authenticated
            // before the certificate got registered, and it has to be re-authenticated by the request router
            using var client = CreateHttpClient(certificates.ClientCertificate3.Value);

            using (var response = await SendAsync())
            {
                Assert.Equal(Http3, response.Version);
                Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
            }

            Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate3.Value, new Dictionary<string, DatabaseAccess>
            {
                [dbName] = DatabaseAccess.ReadWrite
            });

            using (var response = await SendAsync())
            {
                Assert.Equal(Http3, response.Version);
                Assert.Equal(HttpStatusCode.OK, response.StatusCode);
                Assert.Contains("Grisha", await response.Content.ReadAsStringAsync());
            }

            Task<HttpResponseMessage> SendAsync() => client.SendAsync(new HttpRequestMessage(HttpMethod.Get, $"{Server.WebUrl}/databases/{dbName}/docs?id=users/1")
            {
                Version = Http3,
                VersionPolicy = HttpVersionPolicy.RequestVersionExact
            });
        }
    }

    [RavenFact(RavenTestCategory.Security | RavenTestCategory.Certificates)]
    public async Task RequestWithoutClientCertificateOverHttp3IsRejected()
    {
        SkipIfQuicIsNotSupported();

        Certificates.SetupServerAuthentication(Http3Settings(), ServerUrl());

        using (var client = CreateHttpClient(clientCertificate: null))
        using (var response = await client.SendAsync(new HttpRequestMessage(HttpMethod.Get, $"{Server.WebUrl}/databases")
               {
                   Version = Http3,
                   VersionPolicy = HttpVersionPolicy.RequestVersionExact
               }))
        {
            Assert.Equal(Http3, response.Version);
            Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        }
    }

    [RavenTheory(RavenTestCategory.Security | RavenTestCategory.Certificates)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Http3IsAdvertisedViaAltSvcHeader(bool disableAltSvcHeader)
    {
        SkipIfQuicIsNotSupported();

        var settings = Http3Settings();
        settings[RavenConfiguration.GetKey(x => x.Http.Http3DisableAltSvcHeader)] = disableAltSvcHeader.ToString();
        Certificates.SetupServerAuthentication(settings, ServerUrl());

        // Kestrel adds the header to every HTTP/1.1 and HTTP/2 response, so the status code doesn't matter here
        using (var client = CreateHttpClient(clientCertificate: null))
        using (var response = await client.SendAsync(new HttpRequestMessage(HttpMethod.Get, $"{Server.WebUrl}/build/version")
               {
                   Version = HttpVersion.Version20,
                   VersionPolicy = HttpVersionPolicy.RequestVersionExact
               }))
        {
            var hasHttp3AltSvc = response.Headers.TryGetValues("Alt-Svc", out var values) && values.Any(v => v.Contains("h3=", StringComparison.OrdinalIgnoreCase));
            Assert.Equal(disableAltSvcHeader == false, hasHttp3AltSvc);
        }
    }

    [RavenFact(RavenTestCategory.Configuration)]
    public void Http3IsDisabledOnUnsecuredServer()
    {
        using var server = GetNewServer(new ServerCreationOptions
        {
            CustomSettings = Http3Settings()
        });

        Assert.Equal(HttpProtocols.Http1AndHttp2, server.EffectiveHttpProtocols);

        using (var store = GetDocumentStore(new Options { Server = server }))
        using (var session = store.OpenSession())
        {
            session.Store(new User(), "users/1");
            session.SaveChanges();
        }
    }

    [RavenFact(RavenTestCategory.Security | RavenTestCategory.Certificates)]
    public void Http3IsDisabledOnSecuredServerWithDynamicPort()
    {
        SkipIfQuicIsNotSupported();

        Certificates.SetupServerAuthentication(Http3Settings(), "https://127.0.0.1:0");

        Assert.Equal(HttpProtocols.Http1AndHttp2, Server.EffectiveHttpProtocols);
    }

    [RavenFact(RavenTestCategory.Configuration)]
    public void Http3OnlyOnUnsecuredServerFailsToStart()
    {
        var e = Assert.ThrowsAny<Exception>(() => GetNewServer(new ServerCreationOptions
        {
            CustomSettings = new Dictionary<string, string> { [RavenConfiguration.GetKey(x => x.Http.Protocols)] = HttpProtocols.Http3.ToString() }
        }));

        Assert.Contains("HTTP/3 requires the server to be secured", e.ToString());
    }

    private static Dictionary<string, string> Http3Settings() => new()
    {
        [RavenConfiguration.GetKey(x => x.Http.Protocols)] = HttpProtocols.Http1AndHttp2AndHttp3.ToString()
    };

    private static void UseHttp3(DocumentStore store)
    {
        store.Conventions.HttpVersion = Http3;
        store.Conventions.HttpVersionPolicy = HttpVersionPolicy.RequestVersionExact;
    }

    private static HttpClient CreateHttpClient(X509Certificate2 clientCertificate)
    {
        var handler = new SocketsHttpHandler();
        handler.SslOptions.RemoteCertificateValidationCallback = (_, _, _, _) => true;
        if (clientCertificate != null)
            handler.SslOptions.ClientCertificates = new X509CertificateCollection { clientCertificate };

        return new HttpClient(handler);
    }

    private static void SkipIfQuicIsNotSupported()
    {
#pragma warning disable CA1416
        if (QuicListener.IsSupported == false || QuicConnection.IsSupported == false)
#pragma warning restore CA1416
            Assert.Skip("QUIC (HTTP/3) is not supported on this machine.");
    }
}
