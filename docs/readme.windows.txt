Startup instructions for RavenDB on Windows 
===========================================

* RavenDB as a Console Application
Open Powershell
Type:
    .\run.ps1

* RavenDB as Service
Open Powershell
Type:
    .\setup-as-service.ps1

The above command is going to install 'RavenDB' service on your machine. Note it requires to be run as administrator. On the first run the server is going to start on http://127.0.0.1:8080, where you can finish the setup.

The script accepts optional parameters, e.g. to install the service without any questions:
    .\setup-as-service.ps1 -Port 8081 -DataDir D:\RavenData -ServiceAccount 'NT AUTHORITY\NetworkService' -AddFirewallRule -Unattended

To see all parameters, type:
    Get-Help .\setup-as-service.ps1 -Detailed

RavenDB requires the Microsoft Visual C++ Redistributable. Both scripts check that it is installed and print a download link if it is not.

You can view its status using the Get-Service Powershell cmdlet:

>  Get-Service -Name RavenDB

Status   Name               DisplayName
------   ----               -----------
Running  RavenDB            RavenDB

To manage service you can use Stop-Service and Start-Service cmdlets (requires administrator privileges).

* Setup
Open browser, if not opened automatically, at url printed in "Server available on: <url>"
Follow the web setup instructions at: https://docs.ravendb.net/7.2/start/installation/setup-wizard/overview

* Upgrading to a New Version
Follow the upgrade instructions available at: https://docs.ravendb.net/7.2/start/installation/upgrading-to-new-version

