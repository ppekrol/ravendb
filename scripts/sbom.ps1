function CreatePackageSbom ( $projectDir, $releaseDir, $packageDir, $archiveName, $productName, $packOpts ) {
    $generator = [io.path]::combine($projectDir, "scripts", "sbom", "generate-sbom.js")
    $supplement = [io.path]::combine($projectDir, "scripts", "sbom", "supplement.json")
    $studioLock = [io.path]::combine($projectDir, "src", "Raven.Studio", "package-lock.json")
    $vex = [io.path]::combine($projectDir, "scripts", "sbom", "vex.cdx.json")
    $sbomPath = [io.path]::combine($releaseDir, "$archiveName.cdx.json")

    write-host "Create SBOM $sbomPath..."

    $nodeArgs = @(
        $generator,
        "--scan-dir", $packageDir,
        "--product-name", $productName,
        "--version", $packOpts.VersionInfo.Version,
        "--target", $packOpts.Target.Name,
        "--supplement", $supplement,
        "--repo-root", $projectDir,
        "--studio-lock", $studioLock,
        "--vex", $vex,
        "--out", $sbomPath
    )

    exec { node @nodeArgs }
    CheckLastExitCode
}
