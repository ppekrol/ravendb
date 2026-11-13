#!/usr/bin/env node
// Generates a CycloneDX 1.6 SBOM for a RavenDB Docker image:
//   - the image itself (metadata.component, type "container")
//   - its base image (FROM reference, digest when known) and operating system (/etc/os-release)
//   - OS packages installed in the image (dpkg database), as pkg:deb purls
//   - RavenDB itself: the package SBOM of the archive that went into the image (generate-sbom.js output),
//     nested as one application component with its components, dependencies and VEX statements
//
// Usage:
//   node generate-image-sbom.js --image ravendb/ravendb:7.2.0-ubuntu.24.04-x64 --base-image mcr.microsoft.com/dotnet/runtime-deps:10.0-noble
//                               --out RavenDB-7.2.0-docker-ubuntu.24.04-x64.cdx.json
//                               [--base-digest sha256:...] [--package-sbom RavenDB-7.2.0-linux-x64.cdx.json]
//                               [--os-release os-release.txt] [--dpkg dpkg.txt] [--os-name windows --os-version nanoserver-ltsc2022]
//                               [--exclude-os-package ravendb]
//   dpkg.txt: output of  dpkg-query -W -f '${Package}\t${Version}\t${Architecture}\t${source:Package}\t${source:Version}\n'
'use strict';
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const args = {};
for (let i = 2; i < process.argv.length; i += 2) {
    if (!process.argv[i].startsWith('--')) fail(`unexpected argument: ${process.argv[i]}`);
    args[process.argv[i].slice(2)] = process.argv[i + 1];
}
for (const req of ['image', 'base-image', 'out'])
    if (!args[req]) fail(`missing --${req}`);

function fail(msg) { console.error(`[image-sbom] ERROR: ${msg}`); process.exit(1); }
function log(msg) { console.log(`[image-sbom] ${msg}`); }
function readText(p) { return fs.readFileSync(p, 'utf8').replace(/^﻿/, ''); }
const enc = s => encodeURIComponent(s);
const SUPPLIER = { name: 'Hibernating Rhinos', url: ['https://ravendb.net'] };

// "registry/namespace/name:tag" -> { registry, name, tag }
function parseImageRef(ref) {
    let [rest, digest] = ref.split('@');
    const slash = rest.lastIndexOf('/'), colon = rest.lastIndexOf(':');
    const tag = colon > slash ? rest.slice(colon + 1) : 'latest';
    rest = colon > slash ? rest.slice(0, colon) : rest;
    const parts = rest.split('/');
    const registry = parts.length > 1 && /[.:]|^localhost$/.test(parts[0]) ? parts.shift() : 'docker.io';
    return { registry, name: parts.join('/'), tag, digest };
}
function dockerPurl(ref, digest) {
    const r = parseImageRef(ref);
    const version = digest || r.digest || r.tag;
    const q = [`repository_url=${enc(`${r.registry}/${r.name}`)}`];
    if (digest || r.digest) q.push(`tag=${enc(r.tag)}`);
    return `pkg:docker/${r.name.split('/').map(enc).join('/')}@${enc(version)}?${q.join('&')}`;
}

const image = parseImageRef(args.image);
const imageRef = `image:${image.registry}/${image.name}:${image.tag}`;
const components = [];
const dependencies = [];
const imageDeps = [];

// ---------- base image ----------
const base = parseImageRef(args['base-image']);
const baseRef = `image:${base.registry}/${base.name}:${base.tag}`;
const baseComponent = { 'bom-ref': baseRef, type: 'container', name: `${base.registry}/${base.name}`, version: base.tag, purl: dockerPurl(args['base-image'], args['base-digest']) };
if (args['base-digest']) baseComponent.hashes = [{ alg: 'SHA-256', content: args['base-digest'].replace(/^sha256:/, '') }];
else log(`WARN base image digest unknown for ${args['base-image']}`);
components.push(baseComponent);
imageDeps.push(baseRef);

// ---------- operating system ----------
let osId = args['os-name'], osVersion = args['os-version'], osVersionId = args['os-version'], osPretty;
if (args['os-release']) {
    const kv = Object.fromEntries(readText(args['os-release']).split(/\r?\n/).filter(l => l.includes('='))
        .map(l => { const i = l.indexOf('='); return [l.slice(0, i), l.slice(i + 1).replace(/^"|"$/g, '')]; }));
    osId = kv.ID; osVersionId = kv.VERSION_ID; osVersion = kv.VERSION_ID; osPretty = kv.PRETTY_NAME;
}
let osRef;
if (osId) {
    osRef = `os:${osId}@${osVersion}`;
    components.push({ 'bom-ref': osRef, type: 'operating-system', name: osId, version: osVersion, description: osPretty });
    imageDeps.push(osRef);
}

// ---------- OS packages (dpkg) ----------
const exclude = new Set((args['exclude-os-package'] || '').split(',').filter(Boolean));
let osPackages = 0;
if (args.dpkg) {
    if (!osId) fail('--dpkg needs --os-release (or --os-name / --os-version) for the purl distro qualifier');
    const distro = `${osId}-${osVersionId}`;
    const osDeps = [];
    for (const line of readText(args.dpkg).split(/\r?\n/)) {
        if (!line.trim()) continue;
        const [name, version, arch, srcName, srcVersion] = line.split('\t');
        if (!name || !version || exclude.has(name)) continue;
        const q = [`arch=${enc(arch)}`, `distro=${enc(distro)}`];
        if (srcName && (srcName !== name || (srcVersion && srcVersion !== version)))
            q.push(`upstream=${enc(srcVersion && srcVersion !== version ? `${srcName}@${srcVersion}` : srcName)}`);
        const purl = `pkg:deb/${enc(osId)}/${enc(name)}@${enc(version)}?${q.join('&')}`;
        if (components.some(c => c['bom-ref'] === purl)) continue;
        components.push({ 'bom-ref': purl, type: 'library', name, version, purl });
        osDeps.push(purl);
        osPackages++;
    }
    if (!osPackages) fail(`no packages read from ${args.dpkg}`);
    dependencies.push({ ref: osRef, dependsOn: osDeps });
}

// ---------- RavenDB (package SBOM) ----------
let vulnerabilities = [];
let ravenComponents = 0;
if (args['package-sbom']) {
    const pkg = JSON.parse(readText(args['package-sbom']));
    const product = { ...pkg.metadata.component, type: 'application' };
    product.components = pkg.components;
    product.properties = [...(product.properties || []), { name: 'ravendb:packageSbom', value: path.basename(args['package-sbom']) }];
    components.push(product);
    imageDeps.push(product['bom-ref']);
    dependencies.push(...(pkg.dependencies || []));
    vulnerabilities = pkg.vulnerabilities || [];
    ravenComponents = pkg.components.length;
} else {
    log('WARN no --package-sbom: the image SBOM will not describe RavenDB itself');
}

dependencies.unshift({ ref: imageRef, dependsOn: imageDeps });

const bom = {
    bomFormat: 'CycloneDX',
    specVersion: '1.6',
    serialNumber: `urn:uuid:${crypto.randomUUID()}`,
    version: 1,
    metadata: {
        timestamp: new Date().toISOString(),
        tools: { components: [{ type: 'application', name: 'ravendb-generate-image-sbom', version: '1.0' }] },
        component: {
            'bom-ref': imageRef, type: 'container', name: `${image.registry}/${image.name}`, version: image.tag,
            supplier: SUPPLIER, purl: dockerPurl(args.image)
        },
        supplier: SUPPLIER
    },
    components,
    dependencies
};
if (vulnerabilities.length) bom.vulnerabilities = vulnerabilities;
fs.writeFileSync(args.out, JSON.stringify(bom, null, 2) + '\n');

log(`${path.basename(args.out)}: base ${base.name}:${base.tag}${args['base-digest'] ? '' : ' (no digest)'}, ` +
    `os ${osId || '-'} ${osVersion || ''}, ${osPackages} OS packages, RavenDB ${ravenComponents} components, ${vulnerabilities.length} VEX statements`);
