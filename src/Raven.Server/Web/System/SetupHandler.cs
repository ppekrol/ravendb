using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Conventions;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Security;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Client.Util;
using Raven.Server.Commercial;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using StudioConfiguration = Raven.Client.Documents.Operations.Configuration.StudioConfiguration;

namespace Raven.Server.Web.System
{
    public class SetupHandler : RequestHandler
    {
        [RavenAction("/setup/alive", "GET", AuthorizationStatus.UnauthenticatedClients, CorsMode = CorsMode.Public)]
        public Task ServerAlive()
        {
            return NoContent();
        }

        [RavenAction("/setup/dns-n-cert", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public async Task DnsCertBridge()
        {
            AssertOnlyInSetupMode();
            var action = GetQueryStringValueAndAssertIfSingleAndNotEmpty("action"); // Action can be: claim | user-domains | check-availability

            using (var reader = new StreamReader(RequestBodyStream()))
            {
                var payload = await reader.ReadToEndAsync();
                var content = new StringContent(payload, Encoding.UTF8, "application/json");

                try
                {
                    string error = null;
                    object result = null;
                    string responseString = null;
                    string errorMessage = null;

                    try
                    {
                        var response = await ApiHttpClient.Instance.PostAsync("/api/v1/dns-n-cert/" + action, content).ConfigureAwait(false);

                        HttpContext.Response.StatusCode = (int)response.StatusCode;
                        responseString = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

                        if ((int)response.StatusCode >= 500 && (int)response.StatusCode <= 599)
                        {
                            error = responseString;
                            errorMessage = GeneralDomainRegistrationError;
                        }
                        else
                        {
                            result = JsonConvert.DeserializeObject<JObject>(responseString);
                            if (result != null)
                            {
                                if (((JObject)result).TryGetValue(nameof(ExceptionDispatcher.ExceptionSchema.Error), out var err))
                                    error = err.ToString();

                                if (((JObject)result).TryGetValue(nameof(ExceptionDispatcher.ExceptionSchema.Message), out var msg))
                                    errorMessage = msg.ToString();
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        result = responseString;
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                        error = e.ToString();
                        errorMessage = DomainRegistrationServiceUnreachableError;
                    }

                    await using (var streamWriter = new StreamWriter(ResponseBodyStream()))
                    {
                        if (error != null)
                        {
                            new JsonSerializer().Serialize(streamWriter, new
                            {
                                Message = errorMessage,
                                Response = result,
                                Error = error,
                                Type = typeof(RavenException).FullName
                            });

                            await streamWriter.FlushAsync();
                        }
                        else
                        {
                            await streamWriter.WriteAsync(responseString);
                        }

                        await streamWriter.FlushAsync();
                    }
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException(GeneralDomainRegistrationError, e);
                }
            }
        }

        [RavenAction("/setup/user-domains", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public async Task UserDomains()
        {
            AssertOnlyInSetupMode();

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var json = context.Read(RequestBodyStream(), "license activation");
                var licenseInfo = JsonDeserializationServer.LicenseInfo(json);

                var content = new StringContent(JsonConvert.SerializeObject(licenseInfo), Encoding.UTF8, "application/json");
                try
                {
                    string error = null;
                    object result = null;
                    string responseString = null;
                    string errorMessage = null;

                    try
                    {
                        var response = await ApiHttpClient.Instance.PostAsync("/api/v1/dns-n-cert/user-domains", content).ConfigureAwait(false);

                        HttpContext.Response.StatusCode = (int)response.StatusCode;
                        responseString = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

                        if (response.IsSuccessStatusCode == false)
                        {
                            error = responseString;
                            errorMessage = GeneralDomainRegistrationError;
                        }
                        else
                        {
                            result = JsonConvert.DeserializeObject<JObject>(responseString);
                        }
                    }
                    catch (Exception e)
                    {
                        result = responseString;
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                        error = e.ToString();
                        errorMessage = DomainRegistrationServiceUnreachableError;
                    }

                    if (error != null)
                    {
                        JToken errorJToken = null;
                        if (responseString != null)
                        {
                            JsonConvert.DeserializeObject<JObject>(responseString).TryGetValue("Error", out errorJToken);
                        }

                        await using (var streamWriter = new StreamWriter(ResponseBodyStream()))
                        {
                            new JsonSerializer().Serialize(streamWriter, new
                            {
                                Message = errorMessage,
                                Response = result,
                                Error = errorJToken ?? error
                            });

                            await streamWriter.FlushAsync();
                        }

                        return;
                    }

                    var results = JsonConvert.DeserializeObject<UserDomainsResult>(responseString);

                    var fullResult = new UserDomainsAndLicenseInfo
                    {
                        UserDomainsWithIps = new UserDomainsWithIps
                        {
                            Emails = results.Emails,
                            RootDomains = results.RootDomains,
                            Domains = new Dictionary<string, List<SubDomainAndIps>>()
                        }
                    };

                    foreach (var domain in results.Domains)
                    {
                        var list = new List<SubDomainAndIps>();
                        foreach (var subDomain in domain.Value)
                        {
                            try
                            {
                                list.Add(new SubDomainAndIps
                                {
                                    SubDomain = subDomain,
                                    // The ip list will be populated on the next call (/setup/populate-ips), when we know which root domain the user selected
                                });
                            }
                            catch (Exception)
                            {
                                continue;
                            }
                        }

                        fullResult.UserDomainsWithIps.Domains.Add(domain.Key, list);
                    }

                    var licenseStatus = await SetupManager
                        .GetUpdatedLicenseStatus(ServerStore, licenseInfo.License)
                        .ConfigureAwait(false);
                    fullResult.MaxClusterSize = licenseStatus.MaxClusterSize;
                    fullResult.LicenseType = licenseStatus.Type;

                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        var blittable = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.ToBlittable(fullResult, context);
                        await context.WriteAsync(writer, blittable);
                    }
                }
                catch (LicenseExpiredException)
                {
                    throw;
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException(GeneralDomainRegistrationError, e);
                }
            }
        }

        [RavenAction("/setup/populate-ips", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public async Task PopulateIps()
        {
            AssertOnlyInSetupMode();
            var rootDomain = GetQueryStringValueAndAssertIfSingleAndNotEmpty("rootDomain");

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var userDomainsWithIpsJson = context.ReadForMemory(RequestBodyStream(), "setup-secured"))
            {
                var userDomainsWithIps = JsonDeserializationServer.UserDomainsWithIps(userDomainsWithIpsJson);

                foreach (var domain in userDomainsWithIps.Domains)
                {
                    foreach (var subDomain in domain.Value)
                    {
                        try
                        {
                            subDomain.Ips = Dns.GetHostAddresses(subDomain.SubDomain + "." + rootDomain).Select(ip => ip.ToString()).ToList();
                        }
                        catch (Exception)
                        {
                            continue;
                        }
                    }
                }

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var blittable = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.ToBlittable(userDomainsWithIps, context);
                    await context.WriteAsync(writer, blittable);
                }
            }
        }

        [RavenAction("/setup/parameters", "GET", AuthorizationStatus.UnauthenticatedClients)]
        public async Task GetSetupParameters()
        {
            AssertOnlyInSetupMode();
            var setupParameters = await SetupParameters.Get(ServerStore);
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                await writer.WriteStartObjectAsync();
                await writer.WritePropertyNameAsync(nameof(SetupParameters.FixedServerPortNumber));

                if (setupParameters.FixedServerPortNumber.HasValue)
                {
                    await writer.WriteIntegerAsync(setupParameters.FixedServerPortNumber.Value);
                }
                else
                {
                    await writer.WriteNullAsync();
                }

                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(SetupParameters.IsDocker));
                await writer.WriteBoolAsync(setupParameters.IsDocker);
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(SetupParameters.IsAzure));
                await writer.WriteBoolAsync(setupParameters.IsAzure);
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(SetupParameters.IsAws));
                await writer.WriteBoolAsync(setupParameters.IsAws);
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(SetupParameters.RunningOnPosix));
                await writer.WriteBoolAsync(setupParameters.RunningOnPosix);
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(SetupParameters.RunningOnMacOsx));
                await writer.WriteBoolAsync(setupParameters.RunningOnMacOsx);

                await writer.WriteEndObjectAsync();
            }
        }

        [RavenAction("/setup/ips", "GET", AuthorizationStatus.UnauthenticatedClients)]
        public async Task GetIps()
        {
            AssertOnlyInSetupMode();

            NetworkInterface[] netInterfaces = null;
            try
            {
                netInterfaces = NetworkInterface.GetAllNetworkInterfaces();
            }
            catch (Exception)
            {
                // https://github.com/dotnet/corefx/issues/26476
                // If GetAllNetworkInterfaces is not supported, we'll just return the default: 127.0.0.1
            }

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var setupParameters = await SetupParameters.Get(ServerStore);

                await writer.WriteStartObjectAsync();
                await writer.WritePropertyNameAsync("MachineName");
                await writer.WriteStringAsync(Environment.MachineName);
                await writer.WriteCommaAsync();
                await writer.WritePropertyNameAsync("NetworkInterfaces");
                await writer.WriteStartArrayAsync();
                var first = true;

                List<string> ips;
                if (netInterfaces != null)
                {
                    foreach (var netInterface in netInterfaces)
                    {
                        ips = netInterface.GetIPProperties().UnicastAddresses
                            .Where(x =>
                            {
                                // filter 169.254.xxx.xxx out, they are not meaningful for binding
                                if (x.Address.AddressFamily != AddressFamily.InterNetwork)
                                    return false;
                                var addressBytes = x.Address.GetAddressBytes();

                                // filter 127.xxx.xxx.xxx out, in docker only
                                if (setupParameters.IsDocker && addressBytes[0] == 127)
                                    return false;

                                return addressBytes[0] != 169 || addressBytes[1] != 254;
                            })
                            .Select(addr => addr.Address.ToString())
                            .ToList();

                        // If there's a hostname in the server url, add it to the list
                        if (setupParameters.DockerHostname != null && ips.Contains(setupParameters.DockerHostname) == false)
                            ips.Add(setupParameters.DockerHostname);

                        if (first == false)
                            await writer.WriteCommaAsync();
                        first = false;

                        await writer.WriteStartObjectAsync();
                        await writer.WritePropertyNameAsync("Name");
                        await writer.WriteStringAsync(netInterface.Name);
                        await writer.WriteCommaAsync();
                        await writer.WritePropertyNameAsync("Description");
                        await writer.WriteStringAsync(netInterface.Description);
                        await writer.WriteCommaAsync();
                        await writer.WriteArrayAsync("Addresses", ips);
                        await writer.WriteEndObjectAsync();
                    }
                }
                else
                {
                    // https://github.com/dotnet/corefx/issues/26476
                    // If GetAllNetworkInterfaces is not supported, we'll just return the default: 127.0.0.1
                    ips = new List<string>
                    {
                        "127.0.0.1"
                    };
                    await writer.WriteStartObjectAsync();
                    await writer.WritePropertyNameAsync("Name");
                    await writer.WriteStringAsync("Loopback Interface");
                    await writer.WriteCommaAsync();
                    await writer.WritePropertyNameAsync("Description");
                    await writer.WriteStringAsync("Loopback Interface");
                    await writer.WriteCommaAsync();
                    await writer.WriteArrayAsync("Addresses", ips);
                    await writer.WriteEndObjectAsync();
                }

                await writer.WriteEndArrayAsync();
                await writer.WriteEndObjectAsync();
            }
        }

        [RavenAction("/setup/hosts", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public async Task GetHosts()
        {
            AssertOnlyInSetupMode();

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var certificateJson = context.ReadForMemory(RequestBodyStream(), "setup-certificate"))
            {
                var certDef = JsonDeserializationServer.CertificateDefinition(certificateJson);

                X509Certificate2 certificate = null;
                string cn;

                try
                {
                    certificate = certDef.Password == null
                        ? new X509Certificate2(Convert.FromBase64String(certDef.Certificate), (string)null, X509KeyStorageFlags.MachineKeySet)
                        : new X509Certificate2(Convert.FromBase64String(certDef.Certificate), certDef.Password, X509KeyStorageFlags.MachineKeySet);

                    cn = certificate.GetNameInfo(X509NameType.SimpleName, false);
                }
                catch (Exception e)
                {
                    throw new BadRequestException($"Failed to extract the CN property from the certificate {certificate?.FriendlyName}. Maybe the password is wrong?", e);
                }

                if (cn == null)
                {
                    throw new BadRequestException($"Failed to extract the CN property from the certificate. CN is null");
                }

                if (cn.LastIndexOf('*') > 0)
                {
                    throw new NotSupportedException("The wildcard CN name contains a '*' which is not at the first character of the string. It is not supported in the Setup Wizard, you can do a manual setup instead.");
                }

                try
                {
                    SecretProtection.ValidateKeyUsages("Setup Wizard", certificate);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Failed to load the uploaded certificate. Did you accidentally upload a client certificate?", e);
                }

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    await writer.WriteStartObjectAsync();
                    await writer.WritePropertyNameAsync("CN");
                    await writer.WriteStringAsync(cn);
                    await writer.WriteCommaAsync();
                    await writer.WritePropertyNameAsync("AlternativeNames");
                    await writer.WriteStartArrayAsync();

                    var first = true;
                    foreach (var value in SetupManager.GetCertificateAlternativeNames(certificate))
                    {
                        if (first == false)
                            await writer.WriteCommaAsync();
                        first = false;

                        await writer.WriteStringAsync(value);
                    }

                    await writer.WriteEndArrayAsync();

                    await writer.WriteEndObjectAsync();
                }
            }
        }

        [RavenAction("/setup/unsecured", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public async Task SetupUnsecured()
        {
            AssertOnlyInSetupMode();

            using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (var setupInfoJson = context.ReadForMemory(RequestBodyStream(), "setup-unsecured"))
            {
                // Making sure we don't have leftovers from previous setup
                try
                {
                    using (var tx = context.OpenWriteTransaction())
                    {
                        ServerStore.Engine.DeleteTopology(context);
                        tx.Commit();
                    }
                }
                catch (Exception)
                {
                    // ignored
                }

                var setupInfo = JsonDeserializationServer.UnsecuredSetupInfo(setupInfoJson);

                BlittableJsonReaderObject settingsJson;
                using (var fs = new FileStream(ServerStore.Configuration.ConfigPath, FileMode.Open, FileAccess.Read))
                {
                    settingsJson = context.ReadForMemory(fs, "settings-json");
                }

                settingsJson.Modifications = new DynamicJsonValue(settingsJson)
                {
                    [RavenConfiguration.GetKey(x => x.Licensing.EulaAccepted)] = true,
                    [RavenConfiguration.GetKey(x => x.Core.SetupMode)] = nameof(SetupMode.Unsecured),
                    [RavenConfiguration.GetKey(x => x.Security.UnsecuredAccessAllowed)] = nameof(UnsecuredAccessAddressRange.PublicNetwork)
                };

                if (setupInfo.Port == 0)
                    setupInfo.Port = 8080;

                settingsJson.Modifications[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = string.Join(";", setupInfo.Addresses.Select(ip => IpAddressToUrl(ip, setupInfo.Port)));

                if (setupInfo.TcpPort == 0)
                    setupInfo.TcpPort = 38888;

                settingsJson.Modifications[RavenConfiguration.GetKey(x => x.Core.TcpServerUrls)] = string.Join(";", setupInfo.Addresses.Select(ip => IpAddressToUrl(ip, setupInfo.TcpPort, "tcp")));

                if (setupInfo.EnableExperimentalFeatures)
                {
                    settingsJson.Modifications[RavenConfiguration.GetKey(x => x.Core.FeaturesAvailability)] = FeaturesAvailability.Experimental;
                }

                if (!string.IsNullOrEmpty(setupInfo.LocalNodeTag))
                {
                    await ServerStore.EnsureNotPassiveAsync(nodeTag: setupInfo.LocalNodeTag);
                }

                if (setupInfo.Environment != StudioConfiguration.StudioEnvironment.None)
                {
                    var res = await ServerStore.PutValueInClusterAsync(
                        new PutServerWideStudioConfigurationCommand(new ServerWideStudioConfiguration { Disabled = false, Environment = setupInfo.Environment },
                            RaftIdGenerator.DontCareId));
                    await ServerStore.Cluster.WaitForIndexNotification(res.Index);
                }

                var modifiedJsonObj = context.ReadObject(settingsJson, "modified-settings-json");

                var indentedJson = SetupManager.IndentJsonString(modifiedJsonObj.ToString());
                SetupManager.WriteSettingsJsonLocally(ServerStore.Configuration.ConfigPath, indentedJson);
            }

            NoContentStatus();
        }

        [RavenAction("/setup/secured", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public async Task SetupSecured()
        {
            AssertOnlyInSetupMode();

            var stream = TryGetRequestFromStream("Options") ?? RequestBodyStream();
            var operationCancelToken = new OperationCancelToken(ServerStore.ServerShutdown);
            var operationId = GetLongQueryString("operationId", false);

            if (operationId.HasValue == false)
                operationId = ServerStore.Operations.GetNextOperationId();

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var setupInfoJson = context.ReadForMemory(stream, "setup-secured"))
            {
                var setupInfo = JsonDeserializationServer.SetupInfo(setupInfoJson);

                var operationResult = await ServerStore.Operations.AddOperation(
                    null,
                    "Setting up RavenDB in secured mode.",
                    Documents.Operations.Operations.OperationType.Setup,
                    progress => SetupManager.SetupSecuredTask(progress, setupInfo, ServerStore, operationCancelToken.Token),
                    operationId.Value, token: operationCancelToken);

                var zip = ((SetupProgressAndResult)operationResult).SettingsZipFile;

                var nodeCert = setupInfo.Password == null
                    ? new X509Certificate2(Convert.FromBase64String(setupInfo.Certificate), (string)null, X509KeyStorageFlags.MachineKeySet)
                    : new X509Certificate2(Convert.FromBase64String(setupInfo.Certificate), setupInfo.Password, X509KeyStorageFlags.MachineKeySet);

                var cn = nodeCert.GetNameInfo(X509NameType.SimpleName, false);

                var contentDisposition = $"attachment; filename={cn}.Cluster.Settings.zip";
                HttpContext.Response.Headers["Content-Disposition"] = contentDisposition;
                HttpContext.Response.ContentType = "application/octet-stream";

                HttpContext.Response.Body.Write(zip, 0, zip.Length);
            }
        }

        [RavenAction("/setup/letsencrypt/agreement", "GET", AuthorizationStatus.UnauthenticatedClients)]
        public async Task SetupAgreement()
        {
            AssertOnlyInSetupMode();

            var email = GetQueryStringValueAndAssertIfSingleAndNotEmpty("email");

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var baseUri = new Uri("https://letsencrypt.org/");
                var uri = new Uri(baseUri, await SetupManager.LetsEncryptAgreement(email, ServerStore));

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    await writer.WriteStartObjectAsync();
                    await writer.WritePropertyNameAsync("Uri");
                    await writer.WriteStringAsync(uri.AbsoluteUri);
                    await writer.WriteEndObjectAsync();
                }
            }
        }

        [RavenAction("/setup/letsencrypt", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public async Task SetupLetsEncrypt()
        {
            AssertOnlyInSetupMode();

            var stream = TryGetRequestFromStream("Options") ?? RequestBodyStream();

            var operationCancelToken = new OperationCancelToken(ServerStore.ServerShutdown);
            var operationId = GetLongQueryString("operationId", false);

            if (operationId.HasValue == false)
                operationId = ServerStore.Operations.GetNextOperationId();

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var setupInfoJson = context.ReadForMemory(stream, "setup-lets-encrypt"))
            {
                var setupInfo = JsonDeserializationServer.SetupInfo(setupInfoJson);

                var operationResult = await ServerStore.Operations.AddOperation(
                    null, "Setting up RavenDB with a Let's Encrypt certificate",
                    Documents.Operations.Operations.OperationType.Setup,
                    progress => SetupManager.SetupLetsEncryptTask(progress, setupInfo, ServerStore, operationCancelToken.Token),
                    operationId.Value, token: operationCancelToken);

                var zip = ((SetupProgressAndResult)operationResult).SettingsZipFile;

                var contentDisposition = $"attachment; filename={setupInfo.Domain}.Cluster.Settings.zip";
                HttpContext.Response.Headers["Content-Disposition"] = contentDisposition;
                HttpContext.Response.ContentType = "application/octet-stream";

                HttpContext.Response.Body.Write(zip, 0, zip.Length);
            }
        }

        [RavenAction("/setup/continue/extract", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public async Task ExtractInfoFromZip()
        {
            AssertOnlyInSetupMode();

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var continueSetupInfoJson = context.ReadForMemory(RequestBodyStream(), "continue-setup-info"))
            {
                var continueSetupInfo = JsonDeserializationServer.ContinueSetupInfo(continueSetupInfoJson);
                byte[] zipBytes;
                try
                {
                    zipBytes = Convert.FromBase64String(continueSetupInfo.Zip);
                }
                catch (Exception e)
                {
                    throw new ArgumentException($"Unable to parse the {nameof(continueSetupInfo.Zip)} property, expected a Base64 value", e);
                }

                try
                {
                    var urlByTag = new Dictionary<string, string>();

                    using (var ms = new MemoryStream(zipBytes))
                    using (var archive = new ZipArchive(ms, ZipArchiveMode.Read, false))
                    {
                        foreach (var entry in archive.Entries)
                        {
                            if (entry.Name.Equals("settings.json") == false)
                                continue;

                            var tag = entry.FullName.Substring(0, entry.FullName.Length - "/settings.json".Length);

                            using (var settingsJson = context.ReadForMemory(entry.Open(), "settings-json"))
                                if (settingsJson.TryGet(nameof(ConfigurationNodeInfo.PublicServerUrl), out string publicServerUrl))
                                    urlByTag[tag] = publicServerUrl;
                        }
                    }

                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        await writer.WriteStartArrayAsync();
                        var first = true;

                        foreach (var node in urlByTag)
                        {
                            if (first == false)
                                await writer.WriteCommaAsync();

                            await writer.WriteStartObjectAsync();
                            await writer.WritePropertyNameAsync(nameof(ConfigurationNodeInfo.Tag));
                            await writer.WriteStringAsync(node.Key);
                            await writer.WriteCommaAsync();
                            await writer.WritePropertyNameAsync(nameof(ConfigurationNodeInfo.PublicServerUrl));
                            await writer.WriteStringAsync(node.Value);
                            await writer.WriteEndObjectAsync();

                            first = false;
                        }

                        await writer.WriteEndArrayAsync();
                    }
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Unable to extract setup information from the zip file.", e);
                }
            }
        }

        [RavenAction("/setup/continue", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public async Task ContinueClusterSetup()
        {
            AssertOnlyInSetupMode();

            var operationCancelToken = new OperationCancelToken(ServerStore.ServerShutdown);
            var operationId = GetLongQueryString("operationId", false);

            if (operationId.HasValue == false)
                operationId = ServerStore.Operations.GetNextOperationId();

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var continueSetupInfoJson = context.ReadForMemory(RequestBodyStream(), "continue-cluster-setup"))
            {
                var continueSetupInfo = JsonDeserializationServer.ContinueSetupInfo(continueSetupInfoJson);

                await ServerStore.Operations.AddOperation(
                    null, "Continue Cluster Setup.",
                    Documents.Operations.Operations.OperationType.Setup,
                    progress => SetupManager.ContinueClusterSetupTask(progress, continueSetupInfo, ServerStore, operationCancelToken.Token),
                    operationId.Value, token: operationCancelToken);
            }

            NoContentStatus();
        }

        [RavenAction("/setup/finish", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public Task SetupFinish()
        {
            AssertOnlyInSetupMode();

            Task.Run(async () =>
            {
                // we want to give the studio enough time to actually
                // get a valid response from the server before we reset
                await Task.Delay(250);

                Program.RestartServer();
            });

            return NoContent();
        }

        private void AssertOnlyInSetupMode()
        {
            if (ServerStore.Configuration.Core.SetupMode == SetupMode.Initial)
                return;

            throw new AuthorizationException("RavenDB has already been setup. Cannot use the /setup endpoints any longer.");
        }

        private static string IpAddressToUrl(string address, int port, string scheme = "http")
        {
            var url = scheme + "://" + address;
            if (port != 80)
                url += ":" + port;
            return url;
        }

        private static string GeneralDomainRegistrationError = "Registration error.";
        private static string DomainRegistrationServiceUnreachableError = $"Failed to contact {ApiHttpClient.ApiRavenDbNet}. Please try again later.";
    }

    public class LicenseInfo
    {
        public License License { get; set; }
    }

    public class ConfigurationNodeInfo
    {
        public string Tag { get; set; }
        public string PublicServerUrl { get; set; }
    }
}
