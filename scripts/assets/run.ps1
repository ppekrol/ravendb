#Requires -Version 4.0

param(
    # Doesn't check whether the Microsoft Visual C++ Redistributable is installed.
    [switch]$SkipVcRuntimeCheck
)

function Get-ScriptDirectory
{
    $Invocation = (Get-Variable MyInvocation -Scope 1).Value;
    Split-Path $Invocation.MyCommand.Path;
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

$ErrorActionPreference = "Stop";

$scriptDirectory = Get-ScriptDirectory;
$versionPath = Join-Path $scriptDirectory "version.txt";
$executable = "Raven.Server.exe";
$executableDir = Join-Path $scriptDirectory "Server"
$executablePath = Join-Path $executableDir $executable;

if ($SkipVcRuntimeCheck -eq $false) {
    $architecture = Get-ExecutableArchitecture $executablePath
    if ($architecture -and (Test-VcRuntimeInstalled $executableDir $architecture) -eq $false) {
        Write-Host -ForegroundColor Red ("RavenDB requires the Microsoft Visual C++ Redistributable ($architecture). " +
            "Install it from https://aka.ms/vs/17/release/vc_redist.$architecture.exe and run this script again. " +
            "To skip this check, use -SkipVcRuntimeCheck.")
        Read-Host -Prompt "Press enter to continue...";
        exit 1
    }
}

$assemblyVersion =  & $executablePath --version;
$version = $null;

if (Test-Path $versionPath) {
    $version = Get-Content -Path $versionPath;
}

if ($version -ne $assemblyVersion) {
    Set-Content -Path $versionPath $assemblyVersion;
    Start-Process "http://ravendb.net/first-run?type=start&ver=$assemblyVersion";
}

$args = @( "--browser" );

Push-Location $executableDir;

Try
{
    Invoke-Expression -Command ".\$executable $args";
    if ($LASTEXITCODE -ne 0) { 
        Read-Host -Prompt "Press enter to continue...";
    }
}
Finally
{
    Pop-Location;
}
