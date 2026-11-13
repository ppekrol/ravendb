# SBOM generation

`build.ps1` writes a CycloneDX 1.6 SBOM next to every server and tools archive:

```
artifacts/RavenDB-<version>-<target>.cdx.json
artifacts/RavenDB-<version>-<target>.Tools.cdx.json
```

It is generated from the laid-out package directory (what actually ships), not from the source tree,
by `generate-sbom.js` (called from `CreatePackageSbom` in `scripts/sbom.ps1`).

## Sources

| What | Where it comes from |
|---|---|
| NuGet packages, .NET runtime packs, our projects | every `*.deps.json` in the package |
| Custom builds published under upstream NuGet IDs (Lucene.Net, SharpSnmpLib) | `supplement.json` → `overrides` (pedigree incl. source commit, supplier, license) |
| Natives copied by a NuGet package's build targets | `supplement.json` → `bundledFiles` |
| Native binaries (librvnpal, zstd, VC++ runtime, ...) | `supplement.json` → `components`; version read from `pal.ver`, the PE version resource, or an embedded version string |
| Studio npm dependencies (bundled in `Raven.Studio.zip`) | `src/Raven.Studio/package-lock.json`, production dependencies only |
| Licenses | `.nuspec` files in the local NuGet cache, `package.json` in Studio `node_modules` |
| Exploitability statements (VEX) | `vex.cdx.json` → `vulnerabilities`, embedded for the components present in the package |

When a `libs/*.nupkg` custom build is replaced, update its `overrides` entry (the source commit is in the nupkg's
`.nuspec` `<repository>` element).

## When the build fails with "shipped binaries are not attributed to any component"

A `.dll`/`.so`/`.dylib`/`.exe` landed in the package that no `.deps.json` describes. Add it to `supplement.json`:

- it comes from a NuGet package's build targets → add the file name to `bundledFiles` under that package ID;
- it is a native library we build or vendor → add an entry to `components` (name, supplier, licenses, file globs, how to find the version).

## VEX: when a scanner reports a vulnerability in a component we ship

Customers feed the SBOM to scanners (Dependency-Track, Grype, Snyk, ...) and send us the CVEs they get. Most of them
will be in code paths RavenDB never uses. We answer with VEX (Vulnerability Exploitability eXchange) statements in
`vex.cdx.json`, which are embedded into every SBOM that contains the affected component.

1. **Triage** the report: is the vulnerable code reachable in RavenDB, with our configuration?
2. **Record the result** in `vex.cdx.json`, referencing the component by its purl as it appears in the SBOM:

   ```json
   {
     "id": "CVE-2024-12345",
     "source": { "name": "NVD", "url": "https://nvd.nist.gov/vuln/detail/CVE-2024-12345" },
     "analysis": {
       "state": "not_affected",
       "justification": "code_not_reachable",
       "detail": "RavenDB only uses X of package Y; the vulnerable Z API is never called.",
       "firstIssued": "2026-11-13T00:00:00Z"
     },
     "affects": [ { "ref": "pkg:nuget/Some.Package@1.2.3" } ]
   }
   ```

   - `state`: `not_affected` (requires `justification` or `detail`), `exploitable` (we ship a fix - also add `response`),
     `in_triage`, `false_positive`, `resolved`.
   - `justification` for `not_affected`: `code_not_present`, `code_not_reachable`, `requires_configuration`,
     `requires_dependency`, `requires_environment`, `protected_by_compiler`, `protected_at_runtime`,
     `protected_at_perimeter`, `protected_by_mitigating_control`.
3. **If exploitable**, fix it (upgrade the dependency) - the statement then goes away with the old version.
4. The next build embeds the statement; the generator fails on malformed entries.

Owner of triage and of `vex.cdx.json`: to be decided (RavenDB-28011).

## Running it by hand

```
node scripts/sbom/generate-sbom.js --scan-dir artifacts/windows-x64/ServerPackage --product-name "RavenDB Server" \
  --version 7.2.0 --target windows-x64 --supplement scripts/sbom/supplement.json --repo-root . \
  --studio-lock src/Raven.Studio/package-lock.json --vex scripts/sbom/vex.cdx.json \
  --out RavenDB-7.2.0-windows-x64.cdx.json
```

## Open items

- SBOMs are not uploaded yet: `upload.ps1` only picks up `*.zip`, `*.tar`, `*.tar.bz2`.
- Docker images are not covered (OS packages from the base image need an image scanner, e.g. `docker buildx --sbom`).
