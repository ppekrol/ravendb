﻿using System;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_7636 : NoDisposalNeeded
    {
        public RavenDB_7636(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void GivenNoCertificateSpecifiedAndServerBoundOutsideOfUnsecureAccessAllowedShouldError()
        {
            try
            {
                GetConfiguration(
                    unsecuredAccessAddressRange: UnsecuredAccessAddressRange.Local,
                    serverUrl: "http://192.168.1.24");
            }
            catch (InvalidOperationException exception)
            {
                Assert.Equal($"Configured { RavenConfiguration.GetKey(x => x.Core.ServerUrls) } \"http://192.168.1.24\" is not within unsecured access address range. Use a server url within unsecure access address range ({ RavenConfiguration.GetKey(x => x.Security.UnsecuredAccessAllowed) }) or fill in certificate information.", exception.Message);
            }
        }

        internal RavenConfiguration GetConfiguration(
            string certPath = null, 
            UnsecuredAccessAddressRange unsecuredAccessAddressRange = UnsecuredAccessAddressRange.Local,
            string publicServerUrl = null, 
            string serverUrl = null)
        {
            var configuration = RavenConfiguration.CreateForServer(null);
            configuration.SetSetting(
                RavenConfiguration.GetKey(x => x.Core.ServerUrls), serverUrl);
            configuration.SetSetting(
                RavenConfiguration.GetKey(x => x.Core.PublicServerUrl), publicServerUrl);
            configuration.SetSetting(
                RavenConfiguration.GetKey(x => x.Security.CertificatePath), certPath);
            configuration.SetSetting(
                RavenConfiguration.GetKey(x => x.Security.UnsecuredAccessAllowed), Enum.GetName(typeof(UnsecuredAccessAddressRange), unsecuredAccessAddressRange));
            configuration.Initialize();

            return configuration;
        }
    }
}
