import fs from 'node:fs/promises';
import path from 'node:path';
import process from 'node:process';

const registry = 'https://registry.npmjs.org';
const token = process.env.NODE_AUTH_TOKEN || process.env.NPM_TOKEN;

if (!token) {
  console.warn('Skipping npm public access check: NODE_AUTH_TOKEN/NPM_TOKEN is not set.');
  process.exit(0);
}

const workspaceRoots = ['packages'];

async function findPackageJsons(dir) {
  const entries = await fs.readdir(dir, { withFileTypes: true });
  const files = [];

  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      if (entry.name === 'node_modules' || entry.name === 'wa-sqlite') continue;
      files.push(...(await findPackageJsons(fullPath)));
    } else if (entry.name === 'package.json') {
      files.push(fullPath);
    }
  }

  return files;
}

async function setPublicAccess(packageName) {
  const response = await fetch(`${registry}/-/package/${encodeURIComponent(packageName)}/access`, {
    method: 'POST',
    headers: {
      authorization: `Bearer ${token}`,
      'content-type': 'application/json',
    },
    body: JSON.stringify({ access: 'public' }),
  });

  if (response.status === 404) {
    console.warn(`Skipping ${packageName}: package is not published yet.`);
    return;
  }

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Failed to set ${packageName} public: ${response.status} ${body}`);
  }

  console.log(`Ensured public npm access for ${packageName}`);
}

const packageJsons = (await Promise.all(workspaceRoots.map(findPackageJsons))).flat();
const publicPackages = [];

for (const file of packageJsons) {
  const packageJson = JSON.parse(await fs.readFile(file, 'utf8'));
  if (packageJson.private) continue;
  if (packageJson.publishConfig?.access !== 'public') continue;
  publicPackages.push(packageJson.name);
}

for (const packageName of publicPackages.sort()) {
  await setPublicAccess(packageName);
}
