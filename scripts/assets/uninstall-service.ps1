#Requires -Version 4.0
#Requires -RunAsAdministrator

<#
.SYNOPSIS
Stops and unregisters the RavenDB Server Windows service and removes its firewall rule. Data and settings are kept.

.PARAMETER ServiceName
Name of the Windows service. Default: RavenDB.
#>
param(
    [ValidateNotNullOrEmpty()]
    [string]$ServiceName = "RavenDB"
)

$ErrorActionPreference = "Stop";
$ProgressPreference = "SilentlyContinue";

function Get-ScriptDirectory
{
    $Invocation = (Get-Variable MyInvocation -Scope 1).Value;
    Split-Path $Invocation.MyCommand.Path;
}

function CheckLastExitCode {
    param ([int[]]$SuccessCodes = @(0), [scriptblock]$CleanupScript=$null)

    if ($SuccessCodes -notcontains $LastExitCode) {
        if ($CleanupScript) {
            "Executing cleanup script: $CleanupScript"
            &$CleanupScript
        }
        $msg = @"
EXE RETURNED EXIT CODE $LastExitCode
CALLSTACK:$(Get-PSCallStack | Out-String)
"@
        throw $msg
    }
}


$scriptDirectory = Get-ScriptDirectory;
$rvn = "rvn.exe";
$serverDir = Join-Path $scriptDirectory "Server"

# rvn registers the service under its name with whitespace and slashes replaced
$name = $ServiceName -replace '[/\s]', '_'

Push-Location $serverDir;

Try
{
    $existingService = Get-Service -Name $name # just checking if it's installed

    $serviceStatus = $existingService.Status
    write-host "Service '$name' status is: $serviceStatus."
    if (($serviceStatus -eq "Running") -or ($serviceStatus -eq "StartPending")) {
        write-host "Stopping '$name' service..."
        Stop-Service -Name $name
    }

    & ".\$rvn" windows-service unregister --service-name $ServiceName
    CheckLastExitCode

    $firewallRuleName = "RavenDB-Server-$ServiceName"
    if (Get-NetFirewallRule -Name $firewallRuleName -ErrorAction SilentlyContinue) {
        Remove-NetFirewallRule -Name $firewallRuleName
        write-host "Firewall rule '$firewallRuleName' removed."
    }
}
catch
{
    write-error $_.Exception
    exit 1
}
finally
{
    Pop-Location;
}

