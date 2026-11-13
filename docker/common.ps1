
function GetVersionFromArtifactName() {
    $versionRegex = [regex]'RavenDB-([0-9]\.[0-9]\.[0-9]+(-[a-zA-Z]+-[0-9-]+)?)-[a-z]+'
    $fname = $(Get-ChildItem $ArtifactsDir `
        | Where-Object { $_.Name -Match $versionRegex } `
        | Sort-Object LastWriteTime -Descending `
        | Select-Object -First 1).Name
    $match = $fname | select-string -Pattern $versionRegex
    $version = $match.Matches[0].Groups[1]

    if (!$Version) {
        throw "Could not parse version from artifact file name: $fname"
    }

    return $version
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

function GetUbuntuImageTags($repo, $version, $arch, [bool]$useVersionTagsOnly = $false) {
    switch ($arch) {
        "x64" { 
            $versionTags = @(
                "$($repo):$($version)-ubuntu.24.04-x64"
            )
            if ($useVersionTagsOnly) {
                return $versionTags
            }
            return $versionTags + @(
                "$($repo):ubuntu-latest",
                "$($repo):ubuntu-latest-lts",
                "$($repo):7.2-ubuntu-latest"
            )
            break;
        }
        "arm32v7" {
            $versionTags = @(
                "$($repo):$($version)-ubuntu.24.04-arm32v7"
            )
            if ($useVersionTagsOnly) {
                return $versionTags
            }
            return $versionTags + @(
                "$($repo):ubuntu-arm32v7-latest",
                "$($repo):ubuntu-arm32v7-latest-lts",
                "$($repo):7.2-ubuntu-arm32v7-latest"
            )
            break;
        }
        "arm64v8" {
            $versionTags = @(
                "$($repo):$($version)-ubuntu.24.04-arm64v8"
                )
            if ($useVersionTagsOnly) {
                return $versionTags
            }
            return $versionTags + @(
                "$($repo):ubuntu-arm64v8-latest",
                "$($repo):ubuntu-arm64v8-latest-lts",
                "$($repo):7.2-ubuntu-arm64v8-latest"
                )
                break;
        }
        Default {
            throw "Arch not supported."
        }
    }
}

function GetWindowsImageTags($repo, $version, $WinVer, [bool]$useVersionTagsOnly = $false) {
    switch ($winver) {
        "1809" {
            $versionTags = @(
                "$($repo):$($version)-windows-1809"
            )
            if ($useVersionTagsOnly) {
                return $versionTags
            }
            return $versionTags + @(
                "$($repo):windows-1809-latest",
                "$($repo):windows-1809-latest-lts",
                "$($repo):7.2-windows-1809-latest"
            )
            break;
        }
        "ltsc2022" {
            $versionTags = @(
                "$($repo):$($version)-windows-ltsc2022"
            )
            if ($useVersionTagsOnly) {
                return $versionTags
            }
            return $versionTags + @(
                "$($repo):windows-ltsc2022-latest",
                "$($repo):windows-ltsc2022-latest-lts",
                "$($repo):7.2-windows-ltsc2022-latest"
            )
            break;
        }
        Default{
            throw "Windows Version not supported. There are 'ltsc2022' and '1809' avaliable."
        }        
    }
}

function CreateImageSbom {
    # Writes <ArtifactsDir>/RavenDB-<image tag>.docker.cdx.json: base image, OS, OS packages (Linux) and the
    # RavenDB package SBOM of the archive that went into the image (scripts/sbom/generate-image-sbom.js).
    param (
        [Parameter(Mandatory = $true)][string]$ImageTag,
        [Parameter(Mandatory = $true)][string]$DockerfilePath,
        [Parameter(Mandatory = $true)][string]$PackageFilePath,
        [Parameter(Mandatory = $true)][string]$ArtifactsDir,
        [string]$OsName = "",
        [string]$OsVersion = ""
    )

    $generator = Resolve-Path (Join-Path $PSScriptRoot "..\scripts\sbom\generate-image-sbom.js")
    $baseImage = (Get-Content $DockerfilePath | Select-String -Pattern '^\s*FROM\s+(?:--platform=\S+\s+)?(\S+)' | Select-Object -Last 1).Matches[0].Groups[1].Value
    $packageSbom = [io.path]::Combine((Split-Path $PackageFilePath -Parent), ((Split-Path $PackageFilePath -Leaf) -replace '\.(zip|tar\.bz2|tar)$', '') + ".cdx.json")
    $sbomPath = Join-Path $ArtifactsDir "RavenDB-$($ImageTag.Split(':')[-1]).docker.cdx.json"

    $nodeArgs = @($generator, "--image", $ImageTag, "--base-image", $baseImage, "--out", $sbomPath, "--exclude-os-package", "ravendb")

    # base image digest: local image store first, then the registry (BuildKit keeps pulled base images in its own cache)
    $baseDigest = docker image inspect $baseImage --format '{{range .RepoDigests}}{{println .}}{{end}}' 2>$null | Where-Object { $_ -match '@sha256:' } | Select-Object -First 1
    if (-not $baseDigest) {
        $baseDigest = docker buildx imagetools inspect $baseImage --format '{{.Manifest.Digest}}' 2>$null | Where-Object { $_ -match '^sha256:' } | Select-Object -First 1
    }
    if ($baseDigest) {
        $nodeArgs += @("--base-digest", $baseDigest.Split('@')[-1].Trim())
    }

    if (Test-Path $packageSbom) {
        $nodeArgs += @("--package-sbom", $packageSbom)
    } else {
        Write-Host "WARN: package SBOM $packageSbom not found, the image SBOM will not list RavenDB components."
    }

    $tempFiles = @()
    try {
        if ([string]::IsNullOrEmpty($OsName)) {
            # Linux image: read the OS and the dpkg database from the built image itself
            $osRelease = [IO.Path]::GetTempFileName(); $tempFiles += $osRelease
            docker run --rm --entrypoint cat $ImageTag /etc/os-release | Set-Content -Path $osRelease -Encoding utf8
            CheckLastExitCode

            $dpkg = [IO.Path]::GetTempFileName(); $tempFiles += $dpkg
            docker run --rm --entrypoint dpkg-query $ImageTag -W -f '${Package}\t${Version}\t${Architecture}\t${source:Package}\t${source:Version}\n' | Set-Content -Path $dpkg -Encoding utf8
            CheckLastExitCode

            $nodeArgs += @("--os-release", $osRelease, "--dpkg", $dpkg)
        } else {
            $nodeArgs += @("--os-name", $OsName, "--os-version", $OsVersion)
        }

        Write-Host "Create image SBOM $sbomPath..."
        & node @nodeArgs
        CheckLastExitCode
    } finally {
        $tempFiles | ForEach-Object { Remove-Item $_ -ErrorAction SilentlyContinue }
    }
}

function GetManifestTags {
    param (
        $repo,
        $version
    )

    return @(
        "${repo}:latest",
        "${repo}:latest-lts",
        "${repo}:7.2-latest"
        "${repo}:${version}"
    )
}

function GetImageTagsForManifest {
    param (
        [string]$repo
    )

    return @(
        "${repo}:7.2-ubuntu-latest",
        "${repo}:7.2-ubuntu-arm32v7-latest",
        "${repo}:7.2-ubuntu-arm64v8-latest",
        "${repo}:7.2-windows-ltsc2022-latest"
    )
}
