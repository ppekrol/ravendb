#!/usr/bin/env node
// Generates a CycloneDX 1.6 SBOM for a laid-out RavenDB package directory (Server or Tools).
//
// Sources, in order:
//   1. every *.deps.json in --scan-dir       -> NuGet packages, .NET runtime packs, first-party projects
//   2. --supplement (scripts/sbom/supplement.json)
//        overrides  -> forks published under upstream NuGet IDs (pedigree, supplier, license)
//        components -> native binaries no deps.json knows about (librvnpal, zstd, TraceEvent natives)
//   3. --studio-lock (optional)               -> Studio npm production dependencies bundled in Raven.Studio.zip
//   4. local NuGet cache / Studio node_modules -> license data (offline)
//   5. --vex (optional, scripts/sbom/vex.cdx.json) -> our exploitability statements, embedded for components in this package
//
// Fails (exit 1) when a shipped binary cannot be attributed to any component, so new
// unmanaged natives cannot slip into a release without an SBOM entry.
//
// Usage:
//   node generate-sbom.js --scan-dir <dir> --product-name "RavenDB Server" --version 7.2.0 --target windows-x64
//                         --supplement scripts/sbom/supplement.json --repo-root . --out RavenDB-...cdx.json
//                         [--studio-lock src/Raven.Studio/package-lock.json] [--vex scripts/sbom/vex.cdx.json]
'use strict';
const fs = require('fs');
const os = require('os');
const path = require('path');
const crypto = require('crypto');

// ---------- args ----------
const args = {};
for (let i = 2; i < process.argv.length; i += 2) {
    if (!process.argv[i].startsWith('--')) fail(`unexpected argument: ${process.argv[i]}`);
    args[process.argv[i].slice(2)] = process.argv[i + 1];
}
for (const req of ['scan-dir', 'product-name', 'version', 'target', 'supplement', 'repo-root', 'out'])
    if (!args[req]) fail(`missing --${req}`);

const scanDir = path.resolve(args['scan-dir']);
const repoRoot = path.resolve(args['repo-root']);
const supplement = readJson(args.supplement);
const SUPPLIER = { name: 'Hibernating Rhinos', url: ['https://ravendb.net'] };
const FIRST_PARTY_LICENSE = [{ license: { name: 'LicenseRef-RavenDB-EULA' } }];

function fail(msg) { console.error(`[sbom] ERROR: ${msg}`); process.exit(1); }
function log(msg) { console.log(`[sbom] ${msg}`); }
function readJson(p) { return JSON.parse(fs.readFileSync(p, 'utf8').replace(/^﻿/, '')); }
const b64ToHex = b => Buffer.from(b, 'base64').toString('hex');
const sha256 = f => crypto.createHash('sha256').update(fs.readFileSync(f)).digest('hex');
const globToRe = g => new RegExp('^' + g.replace(/[.+^${}()|[\]\\]/g, '\\$&').replace(/\*/g, '.*') + '$', 'i');
const toLicenses = ids => ids.map(id => /^[A-Za-z0-9.+-]+$/.test(id) && !id.startsWith('LicenseRef') ? { license: { id } } : { license: { name: id } });

// ---------- shipped files ----------
const shipped = [];
(function walk(d, rel) {
    for (const e of fs.readdirSync(d, { withFileTypes: true })) {
        const r = rel ? `${rel}/${e.name}` : e.name;
        if (e.isDirectory()) walk(path.join(d, e.name), r); else shipped.push(r);
    }
})(scanDir, '');
const owner = new Map(); // lowercase relative path or basename -> bom-ref
const ownerOf = f => owner.get(f.toLowerCase()) || owner.get(path.basename(f).toLowerCase());

// ---------- 1. deps.json ----------
const comps = new Map(); // bom-ref -> component
const deps = new Map();  // bom-ref -> Set<bom-ref>
const entryPoints = [];  // first-party apps (one per *.deps.json)
const depSet = ref => { if (!deps.has(ref)) deps.set(ref, new Set()); return deps.get(ref); };

const depsFiles = shipped.filter(f => f.endsWith('.deps.json'));
if (!depsFiles.length) fail(`no *.deps.json found under ${scanDir}`);

for (const df of depsFiles) {
    const dj = readJson(path.join(scanDir, df));
    const libs = dj.libraries || {};
    const dir = path.posix.dirname(df) === '.' ? '' : path.posix.dirname(df) + '/';
    const refOf = (key) => {
        const [name, version] = key.split('/');
        const type = (libs[key] || {}).type;
        if (type === 'reference') return null; // duplicate view of a project reference
        if (type === 'project') return `ravendb:${name}@${version}`;
        return `pkg:nuget/${name.replace(/^runtimepack\./, '')}@${version}`;
    };

    for (const target of Object.values(dj.targets || {})) {
        for (const [key, entry] of Object.entries(target)) {
            const ref = refOf(key);
            if (!ref) continue;
            const lib = libs[key] || {};
            const [rawName, version] = key.split('/');
            const name = rawName.replace(/^runtimepack\./, '');
            if (!comps.has(ref)) {
                const c = { 'bom-ref': ref, type: lib.type === 'runtimepack' ? 'framework' : 'library', name, version };
                if (lib.type === 'project') { c.supplier = SUPPLIER; c.licenses = FIRST_PARTY_LICENSE; }
                else c.purl = ref;
                if (lib.sha512 && lib.sha512.startsWith('sha512-')) c.hashes = [{ alg: 'SHA-512', content: b64ToHex(lib.sha512.slice(7)) }];
                comps.set(ref, c);
            }
            if (lib.type === 'project') {
                // app host executable (built for every Exe project, even when only referenced)
                for (const host of [name, `${name}.exe`]) owner.set(`${dir}${host}`.toLowerCase(), ref);
                if (`${name}.deps.json` === path.posix.basename(df) && !entryPoints.includes(ref)) {
                    entryPoints.push(ref);
                    comps.get(ref).type = 'application';
                }
            }
            const d = depSet(ref);
            for (const [dn, dv] of Object.entries(entry.dependencies || {})) {
                const dref = refOf(`${dn}/${dv}`);
                if (dref) d.add(dref);
            }
            for (const section of ['runtime', 'native', 'runtimeTargets', 'resources']) {
                for (const [f, meta] of Object.entries(entry[section] || {})) {
                    const p = section === 'resources' ? `${meta.locale}/${path.basename(f)}` : path.basename(f);
                    owner.set(`${dir}${p}`.toLowerCase(), ref);
                    owner.set(path.basename(p).toLowerCase(), owner.get(path.basename(p).toLowerCase()) || ref);
                }
            }
        }
    }
}

// ---------- 2a. supplement overrides (forks under upstream IDs) ----------
for (const [key, o] of Object.entries(supplement.overrides || {})) {
    const ref = `pkg:nuget/${key}`;
    const c = comps.get(ref);
    if (!c) continue; // not part of this package
    if (o.purlQualifiers) c.purl = `${ref}?${o.purlQualifiers}`;
    if (o.supplier) c.supplier = { name: o.supplier };
    if (o.licenses) c.licenses = toLicenses(o.licenses);
    if (o.pedigree) c.pedigree = o.pedigree;
}

// ---------- 2b. supplement bundledFiles (natives a NuGet package copies via build targets) ----------
for (const [pkg, globs] of Object.entries(supplement.bundledFiles || {})) {
    const c = [...comps.values()].find(x => (x.purl || '').startsWith(`pkg:nuget/${pkg}@`));
    if (!c) continue;
    const res = globs.map(globToRe);
    const files = shipped.filter(f => res.some(re => re.test(path.basename(f))));
    if (!files.length) continue;
    c.components = (c.components || []).concat(files.map(f => ({ type: 'file', name: f, hashes: [{ alg: 'SHA-256', content: sha256(path.join(scanDir, f)) }] })));
    for (const f of files) owner.set(f.toLowerCase(), c['bom-ref']);
}

// ---------- 2c. supplement components (natives) ----------
function peFileVersion(buf) {
    // VS_FIXEDFILEINFO: dwSignature 0xFEEF04BD, dwStrucVersion, dwFileVersionMS, dwFileVersionLS
    const i = buf.indexOf(Buffer.from([0xbd, 0x04, 0xef, 0xfe]));
    if (i < 0 || i + 16 > buf.length) return null;
    const ms = buf.readUInt32LE(i + 8), ls = buf.readUInt32LE(i + 12);
    return `${ms >>> 16}.${ms & 0xffff}.${ls >>> 16}.${ls & 0xffff}`;
}
function detectVersion(spec, file) {
    const v = spec.versionFrom;
    if (!v) return spec.version;
    if (v.file) return fs.readFileSync(path.join(repoRoot, v.file), 'utf8').trim();
    const buf = fs.readFileSync(path.join(scanDir, file));
    if (v.pe) { const pv = peFileVersion(buf); if (pv) return pv; }
    if (v.pattern) {
        const found = new Set(buf.toString('latin1').match(new RegExp(v.pattern, 'g')) || []);
        if (found.size === 1) return [...found][0];
        if (found.size > 1) log(`WARN ${file}: ambiguous version strings ${[...found].join(', ')}`);
    }
    return 'unknown';
}

const nativeRefs = [];
for (const spec of supplement.components || []) {
    const res = spec.files.map(globToRe);
    const files = shipped.filter(f => res.some(re => re.test(path.basename(f))));
    if (!files.length) continue; // e.g. Windows-only natives in a Linux package
    const byVersion = new Map();
    for (const f of files) {
        const ver = detectVersion(spec, f);
        if (ver === 'unknown') log(`WARN ${f}: could not determine ${spec.name} version`);
        if (!byVersion.has(ver)) byVersion.set(ver, []);
        byVersion.get(ver).push(f);
    }
    for (const [version, vfiles] of byVersion) {
        const ref = `native:${spec.name}@${version}`;
        const c = {
            'bom-ref': ref, type: 'library', name: spec.name, version,
            supplier: spec.supplier === 'Hibernating Rhinos' ? SUPPLIER : { name: spec.supplier },
            licenses: toLicenses(spec.licenses),
            description: spec.description,
            // per-file hashes so a customer can verify exactly which binaries this entry describes
            components: vfiles.map(f => ({ type: 'file', name: f, hashes: [{ alg: 'SHA-256', content: sha256(path.join(scanDir, f)) }] }))
        };
        if (spec.purl) c.purl = spec.purl.replace(/\{version\}/g, version);
        if (spec.pedigree) {
            c.pedigree = JSON.parse(JSON.stringify(spec.pedigree).replace(/\{version\}/g, version));
        }
        comps.set(ref, c);
        nativeRefs.push({ ref, spec });
        for (const f of vfiles) owner.set(f.toLowerCase(), ref);
    }
}
for (const { ref, spec } of nativeRefs) {
    const parentPrefix = spec.usedBy && (spec.usedBy.startsWith('nuget:') ? `pkg:nuget/${spec.usedBy.slice(6)}@` : `ravendb:${spec.usedBy}@`);
    const parent = parentPrefix && [...comps.keys()].find(k => k.startsWith(parentPrefix));
    if (parent) depSet(parent).add(ref);
}

// ---------- 3. Studio (npm production dependencies) ----------
const studioZip = shipped.find(f => path.basename(f).toLowerCase() === 'raven.studio.zip');
if (studioZip) {
    const studioRef = `ravendb:Raven.Studio@${args.version}`;
    comps.set(studioRef, {
        'bom-ref': studioRef, type: 'application', name: 'Raven.Studio', version: args.version,
        supplier: SUPPLIER, licenses: FIRST_PARTY_LICENSE,
        components: [{ type: 'file', name: studioZip, hashes: [{ alg: 'SHA-256', content: sha256(path.join(scanDir, studioZip)) }] }]
    });
    owner.set(studioZip.toLowerCase(), studioRef);
    entryPoints.push(studioRef);
    if (!args['studio-lock']) fail('package contains Raven.Studio.zip but --studio-lock was not given');
    const lock = readJson(args['studio-lock']);
    if (!(lock.lockfileVersion >= 2)) fail(`unsupported package-lock lockfileVersion ${lock.lockfileVersion}`);
    const studioDeps = depSet(studioRef);
    for (const [k, v] of Object.entries(lock.packages || {})) {
        if (!k.startsWith('node_modules/') || v.dev || v.devOptional || v.link) continue;
        const name = k.slice(k.lastIndexOf('node_modules/') + 'node_modules/'.length);
        const ref = `pkg:npm/${name.replace(/^@/, '%40')}@${v.version}`;
        if (!comps.has(ref)) {
            const c = { 'bom-ref': ref, type: 'library', name, version: v.version, purl: ref, properties: [{ name: 'ravendb:bundledInto', value: 'Raven.Studio.zip' }] };
            if (v.license) c.licenses = [{ expression: v.license }];
            if (v.integrity && v.integrity.startsWith('sha512-')) c.hashes = [{ alg: 'SHA-512', content: b64ToHex(v.integrity.slice(7)) }];
            comps.set(ref, c);
        }
        studioDeps.add(ref);
    }
}

// ---------- 4. license enrichment (offline) ----------
const nugetRoot = process.env.NUGET_PACKAGES || path.join(os.homedir(), '.nuget', 'packages');
const studioModules = args['studio-lock'] ? path.join(path.dirname(args['studio-lock']), 'node_modules') : null;
for (const c of comps.values()) {
    if (c.licenses) continue;
    const purl = c.purl || '';
    if (purl.startsWith('pkg:nuget/')) {
        const dir = path.join(nugetRoot, c.name.toLowerCase(), c.version.toLowerCase());
        const nuspec = fs.existsSync(dir) && fs.readdirSync(dir).find(f => f.endsWith('.nuspec'));
        if (!nuspec) continue;
        const x = fs.readFileSync(path.join(dir, nuspec), 'utf8');
        const expr = x.match(/<license\s+type="expression"[^>]*>([^<]+)<\/license>/);
        const url = x.match(/<licenseUrl>([^<]+)<\/licenseUrl>/);
        if (expr) c.licenses = [{ expression: expr[1].trim() }];
        else if (url) c.licenses = [{ license: { name: 'See license URL', url: url[1].trim() } }];
    } else if (purl.startsWith('pkg:npm/') && studioModules) {
        const pj = path.join(studioModules, ...c.name.split('/'), 'package.json');
        if (!fs.existsSync(pj)) continue;
        const l = readJson(pj).license;
        if (typeof l === 'string') c.licenses = [{ expression: l }];
    }
}

// ---------- coverage gate ----------
// detect executables/libraries by header, so extensionless Unix binaries (createdump, app hosts) are caught too
const BINARY_MAGIC = ['4d5a', '7f454c46', 'cffaedfe', 'cefaedfe', 'feedfacf', 'feedface', 'cafebabe']; // PE, ELF, Mach-O 64/32 (LE/BE), fat
const isBinary = f => {
    const fd = fs.openSync(path.join(scanDir, f), 'r');
    try {
        const h = Buffer.alloc(4);
        const n = fs.readSync(fd, h, 0, 4, 0);
        const hex = h.subarray(0, n).toString('hex');
        return BINARY_MAGIC.some(m => hex.startsWith(m));
    } finally { fs.closeSync(fd); }
};
const unowned = shipped.filter(f => !ownerOf(f));
const unownedBinaries = unowned.filter(isBinary);
if (unownedBinaries.length) {
    fail(`${unownedBinaries.length} shipped binaries are not attributed to any component. ` +
        `Add them to scripts/sbom/supplement.json:\n  ${unownedBinaries.join('\n  ')}`);
}

// ---------- 5. VEX (optional): our exploitability statements for known vulnerabilities ----------
// Entries reference components by purl (qualifiers ignored) or bom-ref; only entries for components in this package are embedded.
const VEX_STATES = ['resolved', 'resolved_with_pedigree', 'exploitable', 'in_triage', 'false_positive', 'not_affected'];
const vulnerabilities = [];
if (args.vex) {
    const refByKey = new Map();
    for (const c of comps.values()) {
        refByKey.set(c['bom-ref'], c['bom-ref']);
        if (c.purl) refByKey.set(c.purl.split('?')[0], c['bom-ref']);
    }
    for (const v of readJson(args.vex).vulnerabilities || []) {
        const state = v.analysis && v.analysis.state;
        if (!v.id || !VEX_STATES.includes(state))
            fail(`${args.vex}: entry ${v.id || '(no id)'} needs an id and analysis.state (${VEX_STATES.join(', ')})`);
        if (state === 'not_affected' && !v.analysis.justification && !v.analysis.detail)
            fail(`${args.vex}: ${v.id} is not_affected but has neither analysis.justification nor analysis.detail`);
        if (!(v.affects || []).length)
            fail(`${args.vex}: ${v.id} has no affects[] entries`);
        const affects = v.affects.map(a => refByKey.get(String(a.ref).split('?')[0])).filter(Boolean);
        if (affects.length) vulnerabilities.push({ ...v, affects: [...new Set(affects)].map(ref => ({ ref })) });
    }
}

// ---------- emit ----------
const productRef = `ravendb:${args['product-name']}@${args.version}`;
const bom = {
    bomFormat: 'CycloneDX',
    specVersion: '1.6',
    serialNumber: `urn:uuid:${crypto.randomUUID()}`,
    version: 1,
    metadata: {
        timestamp: new Date().toISOString(),
        tools: { components: [{ type: 'application', name: 'ravendb-generate-sbom', version: '1.0' }] },
        component: {
            'bom-ref': productRef, type: 'application', name: args['product-name'], version: args.version,
            supplier: SUPPLIER, licenses: FIRST_PARTY_LICENSE,
            properties: [{ name: 'ravendb:target', value: args.target }]
        },
        supplier: SUPPLIER
    },
    components: [...comps.values()].sort((a, b) => a['bom-ref'].localeCompare(b['bom-ref'])),
    dependencies: [
        { ref: productRef, dependsOn: entryPoints },
        ...[...deps.entries()].map(([ref, d]) => ({ ref, dependsOn: [...d].filter(x => comps.has(x)).sort() }))
    ]
};
if (vulnerabilities.length) bom.vulnerabilities = vulnerabilities;
fs.writeFileSync(args.out, JSON.stringify(bom, null, 2) + '\n');

const count = pred => bom.components.filter(pred).length;
log(`${path.basename(args.out)}: ${bom.components.length} components ` +
    `(nuget ${count(c => (c.purl || '').startsWith('pkg:nuget/'))}, ` +
    `npm ${count(c => (c.purl || '').startsWith('pkg:npm/'))}, ` +
    `native ${nativeRefs.length}, first-party ${count(c => c['bom-ref'].startsWith('ravendb:'))}), ` +
    `${count(c => !c.licenses)} without license, ${unowned.length} non-binary files unattributed, ` +
    `${vulnerabilities.length} VEX statements`);
