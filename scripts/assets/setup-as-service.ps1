#Requires -Version 4.0
#Requires -RunAsAdministrator

<#
.SYNOPSIS
Registers RavenDB Server as a Windows service and starts it.

.DESCRIPTION
On the first run (Server\settings.json doesn't exist yet) the server is configured to listen on http://127.0.0.1:<Port>,
where you can finish the setup wizard. When the server is already configured, its settings are kept as they are.

.PARAMETER Port
Port the server listens on until the setup wizard is finished. Used only on the first run. Default: 8080.

.PARAMETER DataDir
Directory for the server data. Used only on the first run. Default: Server\RavenData.

.PARAMETER ServiceName
Name of the Windows service. Default: RavenDB.

.PARAMETER ServiceAccount
Account the service runs as, e.g. 'NT AUTHORITY\NetworkService' or 'DOMAIN\user'. Default: LocalService.
The account needs the 'Log on as a service' right.

.PARAMETER ServiceAccountPassword
Password of the service account. Not needed for LocalService, NetworkService, LocalSystem and managed service accounts.
If not given for another account, the script asks for it (unless -Unattended is used).

.PARAMETER AddFirewallRule
Adds an inbound Windows Firewall rule allowing connections to Raven.Server.exe on the Domain and Private network profiles.

.PARAMETER Unattended
Doesn't ask any questions and doesn't open the browser.

.PARAMETER SkipVcRuntimeCheck
Doesn't check whether the Microsoft Visual C++ Redistributable is installed.

.EXAMPLE
.\setup-as-service.ps1

.EXAMPLE
.\setup-as-service.ps1 -Port 8081 -DataDir D:\RavenData -ServiceAccount 'NT AUTHORITY\NetworkService' -AddFirewallRule -Unattended
#>
param(
    [ValidateRange(1, 65535)]
    [int]$Port = 8080,
    [string]$DataDir,
    [ValidateNotNullOrEmpty()]
    [string]$ServiceName = "RavenDB",
    [string]$ServiceAccount,
    [SecureString]$ServiceAccountPassword,
    [switch]$AddFirewallRule,
    [switch]$Unattended,
    [switch]$SkipVcRuntimeCheck
)

$ErrorActionPreference = "Stop";
$ProgressPreference = "SilentlyContinue";

function Get-ScriptDirectory
{
    $Invocation = (Get-Variable MyInvocation -Scope 1).Value;
    Split-Path $Invocation.MyCommand.Path;
}

function CheckPortIsClosed($port) {
    $result = Test-NetConnection -Port $port -ComputerName 127.0.0.1 -InformationLevel Quiet 3> $null
    return $result -eq $false
}

function Get-ExecutableArchitecture($path) {
    $stream = [System.IO.File]::OpenRead($path)
    try {
        $reader = New-Object System.IO.BinaryReader($stream)
        $stream.Position = 0x3C
        $stream.Position = $reader.ReadInt32() + 4
        switch ($reader.ReadUInt16()) {
            0x8664 { return "x64" }
            0x014C { return "x86" }
            0xAA64 { return "arm64" }
        }
        return $null
    }
    finally {
        $stream.Dispose()
    }
}

function Test-VcRuntimeInstalled($serverDir, $architecture) {
    foreach ($view in "Registry64", "Registry32") {
        $key = [Microsoft.Win32.RegistryKey]::OpenBaseKey("LocalMachine", $view).OpenSubKey("SOFTWARE\Microsoft\VisualStudio\14.0\VC\Runtimes\$architecture")
        if ($null -ne $key -and $key.GetValue("Installed") -eq 1) {
            return $true
        }
    }

    # the runtime can also be present without the registry entry (e.g. deployed with merge modules or copied next to the server)
    $systemDir = Join-Path $env:windir "System32"
    if ([Environment]::Is64BitOperatingSystem) {
        if ($architecture -eq "x86") {
            $systemDir = Join-Path $env:windir "SysWOW64"
        } elseif ([Environment]::Is64BitProcess -eq $false) {
            $systemDir = Join-Path $env:windir "Sysnative"
        }
    }

    # VC++ runtime DLLs imported by the native libraries shipped with the server (libsodium, onnxruntime, ...)
    $requiredDlls = switch ($architecture) {
        "x64" { "vcruntime140.dll", "vcruntime140_1.dll", "msvcp140.dll", "msvcp140_1.dll" }
        "x86" { "vcruntime140.dll", "msvcp140.dll" }
        "arm64" { "vcruntime140.dll", "msvcp140.dll", "msvcp140_1.dll" }
    }

    foreach ($dll in $requiredDlls) {
        if (-not (Test-Path (Join-Path $serverDir $dll)) -and -not (Test-Path (Join-Path $systemDir $dll))) {
            return $false
        }
    }

    return $true
}

function Get-ServiceAccountIdentity($account) {
    if ([string]::IsNullOrEmpty($account)) {
        return New-Object System.Security.Principal.SecurityIdentifier("S-1-5-19")
    }

    switch -Regex ($account) {
        '^(NT AUTHORITY\\)?(LocalService|LOCAL SERVICE)$' { return New-Object System.Security.Principal.SecurityIdentifier("S-1-5-19") }
        '^(NT AUTHORITY\\)?(NetworkService|NETWORK SERVICE)$' { return New-Object System.Security.Principal.SecurityIdentifier("S-1-5-20") }
        '^(NT AUTHORITY\\SYSTEM|LocalSystem)$' { return New-Object System.Security.Principal.SecurityIdentifier("S-1-5-18") }
        '^\.\\' { return New-Object System.Security.Principal.NTAccount("$env:COMPUTERNAME\$($account.Substring(2))") }
    }
    return New-Object System.Security.Principal.NTAccount($account)
}

function Test-AccountNeedsPassword($account) {
    # built-in service accounts, virtual accounts (NT SERVICE\...) and managed service accounts (...$) have no password
    return $account -notmatch '^((NT AUTHORITY\\)?(LocalService|LOCAL SERVICE|NetworkService|NETWORK SERVICE|SYSTEM)|LocalSystem|NT SERVICE\\.+|.+\$)$'
}

function SetAclOnDirectory($dir, $identity) {
    $acl = Get-Acl $dir
    $permissions = $identity, "FullControl", "ContainerInherit, ObjectInherit", "None", "Allow"
    $rule = New-Object -TypeName System.Security.AccessControl.FileSystemAccessRule -ArgumentList $permissions
    $acl.SetAccessRuleProtection($False, $False)
    $acl.AddAccessRule($rule)
    Set-Acl -Path $dir -AclObject $acl
}

function Get-FullPathOutsideOfDirectory($path, $dir) {
    if ([string]::IsNullOrWhiteSpace($path)) {
        return $null
    }

    # relative paths are resolved by the server against its own directory
    if ([System.IO.Path]::IsPathRooted($path) -eq $false) {
        $path = Join-Path $dir $path
    }

    $fullPath = [System.IO.Path]::GetFullPath($path).TrimEnd('\')
    if ($fullPath.StartsWith($dir.TrimEnd('\') + '\', [StringComparison]::OrdinalIgnoreCase) -or $fullPath -eq $dir.TrimEnd('\')) {
        return $null
    }

    return $fullPath
}

function ConvertTo-CommandLineArgument($argument) {
    if ($argument.Length -gt 0 -and $argument -notmatch '[\s"]') {
        return $argument
    }

    # backslashes are literal unless they precede a quote, so double those and escape the quotes
    $escaped = $argument -replace '(\\*)"', '$1$1\"' -replace '(\\+)$', '$1$1'
    return '"' + $escaped + '"'
}

function Invoke-Rvn($rvnPath, $arguments) {
    $startInfo = New-Object System.Diagnostics.ProcessStartInfo
    $startInfo.FileName = $rvnPath
    $startInfo.Arguments = ($arguments | ForEach-Object { ConvertTo-CommandLineArgument $_ }) -join ' '
    $startInfo.UseShellExecute = $false
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true

    $process = [System.Diagnostics.Process]::Start($startInfo)
    $stdout = $process.StandardOutput.ReadToEndAsync()
    $stderr = $process.StandardError.ReadToEndAsync()
    $process.WaitForExit()

    if ($stdout.Result) { Write-Host $stdout.Result.TrimEnd() }
    if ($stderr.Result) { Write-Host $stderr.Result.TrimEnd() }

    return $process.ExitCode
}

$scriptDirectory = Get-ScriptDirectory;
$settingsTemplateJson = "settings.default.json";
$settingsJson = "settings.json";
$rvn = "rvn.exe";
$serverDir = Join-Path $scriptDirectory "Server"
$serverExecutablePath = Join-Path $serverDir "Raven.Server.exe"

if ($SkipVcRuntimeCheck -eq $false) {
    $architecture = Get-ExecutableArchitecture $serverExecutablePath
    if ($architecture -and (Test-VcRuntimeInstalled $serverDir $architecture) -eq $false) {
        Write-Error ("RavenDB requires the Microsoft Visual C++ Redistributable ($architecture). " +
            "Install it from https://aka.ms/vs/17/release/vc_redist.$architecture.exe and run this script again. " +
            "To skip this check, use -SkipVcRuntimeCheck.") -ErrorAction Continue
        exit 5
    }
}

$serviceAccountIdentity = Get-ServiceAccountIdentity $ServiceAccount

# a virtual account (NT SERVICE\<name>) exists only once its service is registered
if ($ServiceAccount -notmatch '^NT SERVICE\\') {
    try {
        $serviceAccountIdentity.Translate([System.Security.Principal.SecurityIdentifier]) | Out-Null
    }
    catch {
        Write-Error "Account '$ServiceAccount' was not found." -ErrorAction Continue
        exit 6
    }
}

if ($ServiceAccount -and $null -eq $ServiceAccountPassword -and $Unattended -eq $false -and (Test-AccountNeedsPassword $ServiceAccount)) {
    $ServiceAccountPassword = Read-Host -Prompt "Password for '$ServiceAccount'" -AsSecureString
}

$settingsJsonPath = Join-Path $serverDir $settingsJson
$settingsTemplateJsonPath = Join-Path $serverDir $settingsTemplateJson;

$isAlreadyConfigured = Test-Path $settingsJsonPath

if ($isAlreadyConfigured) {
    if ($PSBoundParameters.ContainsKey("Port") -or $PSBoundParameters.ContainsKey("DataDir")) {
        Write-Error "Server was run before, so -Port and -DataDir can't be applied. Change 'ServerUrl' and 'DataDir' in $settingsJsonPath instead." -ErrorAction Continue
        exit 6
    }

    write-host "Server was run before - attempt to use existing configuration."
    $settings = Get-Content $settingsJsonPath -raw | ConvertFrom-Json
    $serverUrl = $settings.ServerUrl
} else {
    write-host "Server run for the first time."

    if ((CheckPortIsClosed $Port) -eq $false) {
        Write-Error "Port $Port is not available." -ErrorAction Continue
        exit 2
    }

    try {
        $settings = Get-Content $settingsTemplateJsonPath -raw | ConvertFrom-Json
        $serverUrl = "http://127.0.0.1:$Port"
        $settings | Add-Member -NotePropertyName "ServerUrl" -NotePropertyValue $serverUrl -Force
        if ($DataDir) {
            $settings | Add-Member -NotePropertyName "DataDir" -NotePropertyValue $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($DataDir) -Force
        }
        $settings | ConvertTo-Json | Set-Content $settingsTemplateJsonPath -Encoding UTF8
    }
    catch {
        Write-Error $_.Exception -ErrorAction Continue
        exit 3
    }
}

$logsPath = $settings.'Logs.Path'
if ($null -eq $logsPath) {
    $logsPath = "Logs"
}

$rvnPath = Join-Path $serverDir $rvn

Try
{
    $rvnArguments = @("windows-service", "register", "--service-name", $ServiceName)

    if ($ServiceAccount) {
        $rvnArguments += @("--service-user-name", $ServiceAccount)
    }

    if ($null -ne $ServiceAccountPassword) {
        $passwordPtr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($ServiceAccountPassword)
        try {
            $rvnArguments += @("--service-user-password", [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($passwordPtr))
        }
        finally {
            [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($passwordPtr)
        }
    }

    $exitCode = Invoke-Rvn $rvnPath $rvnArguments
    if ($exitCode -ne 0) {
        throw "Registering the '$ServiceName' service failed (rvn exit code: $exitCode)."
    }

    SetAclOnDirectory $serverDir $serviceAccountIdentity

    foreach ($dir in @($settings.DataDir, $logsPath)) {
        $outsideDir = Get-FullPathOutsideOfDirectory $dir $serverDir
        if ($outsideDir) {
            write-host "Granting the service account access to $outsideDir."
            New-Item -ItemType Directory -Path $outsideDir -Force | Out-Null
            SetAclOnDirectory $outsideDir $serviceAccountIdentity
        }
    }

    if ($AddFirewallRule) {
        $firewallRuleName = "RavenDB-Server-$ServiceName"
        Get-NetFirewallRule -Name $firewallRuleName -ErrorAction SilentlyContinue | Remove-NetFirewallRule
        New-NetFirewallRule -Name $firewallRuleName -DisplayName "RavenDB Server ($ServiceName)" -Group "RavenDB" `
            -Direction Inbound -Action Allow -Program $serverExecutablePath -Profile Domain, Private | Out-Null
        write-host "Firewall rule '$firewallRuleName' allows inbound connections to $serverExecutablePath (Domain and Private profiles)."
    }

    $exitCode = Invoke-Rvn $rvnPath @("windows-service", "start", "--service-name", $ServiceName)
    if ($exitCode -ne 0) {
        throw "Starting the '$ServiceName' service failed (rvn exit code: $exitCode)."
    }
}
catch
{
    write-error $_.Exception -ErrorAction Continue
    exit 4
}

Write-Host "Service started, server listening on $serverUrl."

if ($isAlreadyConfigured -eq $false) {
    Write-Host "You can now finish setting up the RavenDB service in the browser."
}

if ($Unattended -eq $false) {
    Start-Sleep -Seconds 3
    Start-Process $serverUrl
}
