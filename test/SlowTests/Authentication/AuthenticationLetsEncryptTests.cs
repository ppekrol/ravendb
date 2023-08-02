﻿// -----------------------------------------------------------------------
//  <copyright file="CanAuthenticate.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Commercial;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Utils;
using SlowTests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Server;
using Tests.Infrastructure;
using xRetry;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Authentication
{
    public partial class AuthenticationLetsEncryptTests : ClusterTestBase
    {
        public AuthenticationLetsEncryptTests(ITestOutputHelper output) : base(output)
        {
        }

        [RetryFact(delayBetweenRetriesMs: 1000)]
        public async Task CanGetLetsEncryptCertificateAndRenewIt()
        {
            var settingPath = Path.Combine(NewDataPath(forceCreateDir: true), "settings.json");
            var defaultSettingsPath = new PathSetting("settings.default.json").FullPath;
            File.Copy(defaultSettingsPath, settingPath, true);

            UseNewLocalServer(customConfigPath: settingPath);

            // Use this when testing against pebble
            //var acmeStaging = "https://localhost:14000/";

            var acmeStaging = "https://acme-staging-v02.api.letsencrypt.org/";

            Server.Configuration.Core.AcmeUrl = acmeStaging;
            Server.ServerStore.Configuration.Core.SetupMode = SetupMode.Initial;

            var domain = "RavenClusterTest" + Environment.MachineName.Replace("-", "");
            string email;
            string rootDomain;

            await Server.ServerStore.EnsureNotPassiveAsync();
            var license = Server.ServerStore.LoadLicense();

            using (var store = GetDocumentStore())
            using (var commands = store.Commands())
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var command = new ClaimDomainCommand(store.Conventions, context, new ClaimDomainInfo
                {
                    Domain = domain,
                    License = license
                });

                await commands.RequestExecutor.ExecuteAsync(command, commands.Context);

                Assert.True(command.Result.RootDomains.Length > 0);
                rootDomain = command.Result.RootDomains[0];
                email = command.Result.Email;
            }

            var setupInfo = new SetupInfo
            {
                Domain = domain,
                RootDomain = rootDomain,
                ZipOnly = false, // N/A here
                RegisterClientCert = false, // N/A here
                Password = null,
                Certificate = null,
                LocalNodeTag = "A",
                License = license,
                Email = email,
                NodeSetupInfos = new Dictionary<string, NodeInfo>()
                {
                    ["A"] = new NodeInfo
                    {
                        Port = GetAvailablePort(),
                        TcpPort = GetAvailablePort(),
                        Addresses = new List<string>
                        {
                            "127.0.0.1"
                        }
                    }
                }
            };

            X509Certificate2 serverCert;
            byte[] serverCertBytes;
            string firstServerCertThumbprint;
            BlittableJsonReaderObject settingsJsonObject;

            using (var store = GetDocumentStore())
            using (var commands = store.Commands())
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var command = new SetupLetsEncryptCommand(store.Conventions, context, setupInfo);

                await commands.RequestExecutor.ExecuteAsync(command, commands.Context);

                Assert.True(command.Result.Length > 0);

                var zipBytes = command.Result;

                try
                {
                    settingsJsonObject = SetupManager.ExtractCertificatesAndSettingsJsonFromZip(zipBytes, "A", context, out serverCertBytes, out serverCert, out _, out _, out _, out _);
                    firstServerCertThumbprint = serverCert.Thumbprint;
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Unable to extract setup information from the zip file.", e);
                }

                // Finished the setup wizard, need to restart the server.
                // Since cannot restart we'll create a new server loaded with the new certificate and settings and use the server cert to connect to it

                settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Security.CertificatePassword), out string certPassword);
                settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Security.CertificateLetsEncryptEmail), out string letsEncryptEmail);
                settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.PublicServerUrl), out string publicServerUrl);
                settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.ServerUrls), out string serverUrl);
                settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.SetupMode), out SetupMode setupMode);
                settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.ExternalIp), out string externalIp);

                var tempFileName = GetTempFileName();
                File.WriteAllBytes(tempFileName, serverCertBytes);

                IDictionary<string, string> customSettings = new ConcurrentDictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Security.CertificatePath)] = tempFileName,
                    [RavenConfiguration.GetKey(x => x.Security.CertificateLetsEncryptEmail)] = letsEncryptEmail,
                    [RavenConfiguration.GetKey(x => x.Security.CertificatePassword)] = certPassword,
                    [RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)] = publicServerUrl,
                    [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = serverUrl,
                    [RavenConfiguration.GetKey(x => x.Core.SetupMode)] = setupMode.ToString(),
                    [RavenConfiguration.GetKey(x => x.Core.ExternalIp)] = externalIp,
                    [RavenConfiguration.GetKey(x => x.Core.AcmeUrl)] = acmeStaging
                };

                DoNotReuseServer(customSettings);
            }

            Server.Dispose();

            UseNewLocalServer();

            // Note: because we use a staging lets encrypt cert, the chain is not trusted.
            // It only works because in the TestBase ctor we do:
            // RequestExecutor.ServerCertificateCustomValidationCallback += (msg, cert, chain, errors) => true;

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = serverCert,
                ClientCertificate = serverCert
            }))
            using (var commands = store.Commands())
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await Server.ServerStore.EnsureNotPassiveAsync();
                Assert.Equal(firstServerCertThumbprint, Server.Certificate.Certificate.Thumbprint);

                Server.Time.UtcDateTime = () => DateTime.UtcNow.AddDays(80);

                var mre = new ManualResetEventSlim();

                Server.ServerCertificateChanged += (sender, args) => mre.Set();

                var command = new ForceRenewCertCommand(store.Conventions, context);

                await commands.RequestExecutor.ExecuteAsync(command, commands.Context);

                Assert.True(command.Result.Success, "ForceRenewCertCommand returned false");

                var result = mre.Wait(Debugger.IsAttached ? TimeSpan.FromMinutes(10) : TimeSpan.FromMinutes(4));

                if (result == false && Server.RefreshTask.IsCompleted)
                {
                    if (Server.RefreshTask.IsFaulted || Server.RefreshTask.IsCanceled)
                    {
                        Assert.True(result,
                            $"Refresh task failed to complete successfully. Exception: {Server.RefreshTask.Exception}");
                    }

                    Assert.True(result, "Refresh task completed successfully, waited too long for the cluster cert to be replaced");
                }

                Assert.True(result, "Refresh task didn't complete. Waited too long for the cluster cert to be replaced");

                Assert.NotEqual(firstServerCertThumbprint, Server.Certificate.Certificate.Thumbprint);
            }
        }

        [RavenFact(RavenTestCategory.Certificates | RavenTestCategory.Sharding)]
        public async Task CertificateReplaceSharded()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var clusterSize = 3;
            var (leader, nodes, serverCert) = await CreateLetsEncryptCluster(clusterSize);
            Assert.Equal(serverCert.Thumbprint, nodes[0].Certificate.Certificate.Thumbprint);
            var databaseName = GetDatabaseName();

            var options = Sharding.GetOptionsForCluster(leader, clusterSize, shardReplicationFactor: 1, orchestratorReplicationFactor: 1);
            options.ClientCertificate = serverCert;
            options.AdminCertificate = serverCert;
            options.ModifyDatabaseName = _ => databaseName;
            options.RunInMemory = false;
            options.DeleteDatabaseOnDispose = false;
            options.ModifyDocumentStore = s => s.Conventions.DisposeCertificate = false;

            X509Certificate2 newCert;

            using (var store = Sharding.GetDocumentStore(options))
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                foreach (var topology in record.Sharding.Shards.Values)
                {
                    Assert.Equal(1, topology.Members.Count);
                }

                var requestExecutor = store.GetRequestExecutor(databaseName);

                var replaceTasks = new Dictionary<string, AsyncManualResetEvent>();
                foreach (var node in nodes)
                {
                    replaceTasks.Add(node.ServerStore.NodeTag, new AsyncManualResetEvent());
                }

                foreach (var server in nodes)
                {
                    server.ServerCertificateChanged += (sender, args) => replaceTasks[server.ServerStore.NodeTag].Set();
                }

                //trigger cert refresh
                await requestExecutor.HttpClient.PostAsync($"{nodes[0].WebUrl}/admin/certificates/letsencrypt/force-renew", null);

                await Task.WhenAll(replaceTasks.Values.Select(x => x.WaitAsync()).ToArray());

                //make sure all cluster nodes have the new server cert
                foreach (var node in nodes)
                {
                    Assert.NotEqual(serverCert.Thumbprint, node.Certificate.Certificate.Thumbprint);
                }

                newCert = nodes[0].Certificate.Certificate;
            }

            using (var store = new DocumentStore()
            {
                Certificate = newCert,
                Database = databaseName,
                Urls = new string[] { leader.WebUrl },
                Conventions =
                {
                    DisposeCertificate = false
                }
            }.Initialize())
            {
                //try a request that will not use shard executors
                await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));

                //try a request that will use shard executors
                await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation());

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "users/1");
                    await session.SaveChangesAsync();
                }
            }
        }

        internal async Task<(RavenServer Leader, List<RavenServer> Nodes, X509Certificate2 Cert)> CreateLetsEncryptCluster(int clutserSize)
        {
            var settingPath = Path.Combine(NewDataPath(forceCreateDir: true), "settings.json");
            var defaultSettingsPath = new PathSetting("settings.default.json").FullPath;
            File.Copy(defaultSettingsPath, settingPath, true);

            UseNewLocalServer(customConfigPath: settingPath);

            var acmeStaging = "https://acme-staging-v02.api.letsencrypt.org/";

            Server.Configuration.Core.AcmeUrl = acmeStaging;
            Server.ServerStore.Configuration.Core.SetupMode = SetupMode.Initial;

            var domain = "RavenClusterTest" + Environment.MachineName.Replace("-", "");
            string email;
            string rootDomain;

            await Server.ServerStore.EnsureNotPassiveAsync();
            var license = Server.ServerStore.LoadLicense();

            using (var store = GetDocumentStore())
            using (var commands = store.Commands())
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var command = new AuthenticationLetsEncryptTests.ClaimDomainCommand(store.Conventions, context, new ClaimDomainInfo
                {
                    Domain = domain,
                    License = license
                });

                await commands.RequestExecutor.ExecuteAsync(command, commands.Context);

                Assert.True(command.Result.RootDomains.Length > 0);
                rootDomain = command.Result.RootDomains[0];
                email = command.Result.Email;
            }

            var nodeSetupInfos = new Dictionary<string, NodeInfo>();
            char nodeTag = 'A';
            for (int i = 1; i <= clutserSize; i++)
            {
                var tcpListener = new TcpListener(IPAddress.Loopback, 0);
                tcpListener.Start();
                var port = ((IPEndPoint)tcpListener.LocalEndpoint).Port;
                tcpListener.Stop();
                var setupNodeInfo = new NodeInfo
                {
                    Port = port,
                    Addresses = new List<string> { $"127.0.0.{i}" }
                };
                nodeSetupInfos.Add(nodeTag.ToString(), setupNodeInfo);
                nodeTag++;
            }

            var setupInfo = new SetupInfo
            {
                Domain = domain,
                RootDomain = rootDomain,
                RegisterClientCert = false,
                Password = null,
                Certificate = null,
                LocalNodeTag = "A",
                License = license,
                Email = email,
                NodeSetupInfos = nodeSetupInfos
            };

            X509Certificate2 serverCert = default;
            byte[] serverCertBytes;
            BlittableJsonReaderObject settingsJsonObject;
            var customSettings = new List<IDictionary<string, string>>();

            using (var store = GetDocumentStore())
            using (var commands = store.Commands())
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var command = new AuthenticationLetsEncryptTests.SetupLetsEncryptCommand(store.Conventions, context, setupInfo);

                await commands.RequestExecutor.ExecuteAsync(command, commands.Context);

                Assert.True(command.Result.Length > 0);

                var zipBytes = command.Result;

                foreach (var node in setupInfo.NodeSetupInfos)
                {
                    try
                    {
                        var tag = node.Key;
                        settingsJsonObject = SetupManager.ExtractCertificatesAndSettingsJsonFromZip(zipBytes, tag, context, out serverCertBytes, out serverCert, out _, out _, out _, out _);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("Unable to extract setup information from the zip file.", e);
                    }

                    settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Security.CertificatePassword), out string certPassword);
                    settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Security.CertificateLetsEncryptEmail), out string letsEncryptEmail);
                    settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.PublicServerUrl), out string publicServerUrl);
                    settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.ServerUrls), out string serverUrl);
                    settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.SetupMode), out SetupMode setupMode);
                    settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.ExternalIp), out string externalIp);

                    var tempFileName = GetTempFileName();
                    await File.WriteAllBytesAsync(tempFileName, serverCertBytes);

                    var settings = new Dictionary<string, string>
                    {
                        [RavenConfiguration.GetKey(x => x.Security.CertificatePath)] = tempFileName,
                        [RavenConfiguration.GetKey(x => x.Security.CertificateLetsEncryptEmail)] = letsEncryptEmail,
                        [RavenConfiguration.GetKey(x => x.Security.CertificatePassword)] = certPassword,
                        [RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)] = publicServerUrl,
                        [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = serverUrl,
                        [RavenConfiguration.GetKey(x => x.Core.SetupMode)] = setupMode.ToString(),
                        [RavenConfiguration.GetKey(x => x.Core.ExternalIp)] = externalIp,
                        [RavenConfiguration.GetKey(x => x.Core.AcmeUrl)] = acmeStaging
                    };
                    customSettings.Add(settings);
                }
            }

            Server.Dispose();

            var cluster = await CreateRaftClusterInternalAsync(clutserSize, customSettingsList: customSettings, leaderIndex: 0, useSsl: true);
            return (cluster.Leader, cluster.Nodes, serverCert);
        }
    }
}
