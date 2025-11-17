'use strict';

const fs = require('fs');
const os = require('os');
const path = require('path');
const https = require('https');
const { spawnSync } = require('child_process');

const pkg = require('../package.json');

const DEFAULT_REPO = 'eventdbx/eventdbx';
const MAX_REDIRECTS = 5;
const TARGETS = {
  'darwin-arm64': { triple: 'aarch64-apple-darwin', archive: '.tar.xz', binary: 'dbx' },
  'darwin-x64': { triple: 'x86_64-apple-darwin', archive: '.tar.xz', binary: 'dbx' },
  'linux-arm64': { triple: 'aarch64-unknown-linux-gnu', archive: '.tar.xz', binary: 'dbx' },
  'linux-x64': { triple: 'x86_64-unknown-linux-gnu', archive: '.tar.xz', binary: 'dbx' },
  'win32-x64': { triple: 'x86_64-pc-windows-msvc', archive: '.zip', binary: 'dbx.exe' },
};

function log(message) {
  console.log(`[eventdbx] ${message}`);
}

function shouldSkipInstall() {
  const value = process.env.EVENTDBX_SKIP_INSTALL ?? process.env.npm_config_eventdbx_skip_install;
  if (!value) {
    return false;
  }
  return ['1', 'true', 'yes'].includes(String(value).toLowerCase());
}

function resolveVersionTag() {
  const override = process.env.EVENTDBX_VERSION || process.env.npm_config_eventdbx_version;
  const source = (override || pkg.version).trim();
  if (!source) {
    throw new Error('Missing version for EventDBX install.');
  }
  return source.startsWith('v') ? source : `v${source}`;
}

function resolveRepo() {
  return process.env.EVENTDBX_REPO || DEFAULT_REPO;
}

function resolveTarget() {
  const platform = (
    process.env.EVENTDBX_PLATFORM ||
    process.env.npm_config_eventdbx_platform ||
    os.platform()
  ).toLowerCase();
  const architecture = (
    process.env.EVENTDBX_ARCH ||
    process.env.npm_config_eventdbx_arch ||
    os.arch()
  ).toLowerCase();
  const key = `${platform}-${architecture}`;
  const target = TARGETS[key];
  if (!target) {
    throw new Error(
      `Unsupported platform/architecture combination (${platform}-${architecture}). ` +
        `Download binaries manually from https://github.com/${resolveRepo()}/releases instead.`
    );
  }
  return target;
}

async function downloadFile(url, destination, redirects = 0) {
  await fs.promises.mkdir(path.dirname(destination), { recursive: true });
  log(`Downloading ${url}`);

  return new Promise((resolve, reject) => {
    const request = https.get(
      url,
      {
        headers: {
          'User-Agent': `eventdbx-npm-installer/${pkg.version}`,
          Accept: 'application/octet-stream',
        },
      },
      response => {
        if (
          response.statusCode &&
          response.statusCode >= 300 &&
          response.statusCode < 400 &&
          response.headers.location
        ) {
          if (redirects >= MAX_REDIRECTS) {
            response.resume();
            reject(new Error('Too many HTTP redirects while downloading EventDBX.'));
            return;
          }
          const nextUrl = response.headers.location;
          response.resume();
          downloadFile(nextUrl, destination, redirects + 1)
            .then(resolve)
            .catch(reject);
          return;
        }

        if (response.statusCode !== 200) {
          response.resume();
          reject(
            new Error(`Failed to download ${url} (status ${response.statusCode ?? 'unknown'})`)
          );
          return;
        }

        const file = fs.createWriteStream(destination);
        response.pipe(file);
        file.on('finish', () => {
          file.close(resolve);
        });
        file.on('error', err => {
          file.close();
          fs.rm(destination, { force: true }, () => reject(err));
        });
      }
    );

    request.on('error', err => {
      fs.rm(destination, { force: true }, () => reject(err));
    });
    request.setTimeout(60_000, () => {
      request.destroy(new Error('Request timed out while downloading EventDBX.'));
    });
  });
}

function runCommand(command, args, options = {}) {
  const result = spawnSync(command, args, {
    stdio: options.quiet ? 'ignore' : 'inherit',
    windowsHide: true,
  });
  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    throw new Error(
      `${command} ${args.join(' ')} exited with status ${result.status ?? 'unknown'}`
    );
  }
}

function extractArchive(archivePath, target, destination) {
  fs.mkdirSync(destination, { recursive: true });
  switch (target.archive) {
    case '.tar.xz': {
      runCommand('tar', ['-xJf', archivePath, '-C', destination]);
      break;
    }
    case '.tar.gz': {
      runCommand('tar', ['-xzf', archivePath, '-C', destination]);
      break;
    }
    case '.zip': {
      if (process.platform === 'win32') {
        runCommand('powershell.exe', [
          '-NoProfile',
          '-Command',
          `Expand-Archive -Path "${archivePath}" -DestinationPath "${destination}" -Force`,
        ]);
      } else {
        runCommand('unzip', ['-o', archivePath, '-d', destination]);
      }
      break;
    }
    default:
      throw new Error(`Unsupported archive format ${target.archive}`);
  }
}

function findBinary(root, binaryName) {
  const stack = [root];
  while (stack.length) {
    const current = stack.pop();
    const entries = fs.readdirSync(current, { withFileTypes: true });
    for (const entry of entries) {
      const fullPath = path.join(current, entry.name);
      if (entry.isDirectory()) {
        stack.push(fullPath);
      } else if (entry.isFile() && entry.name === binaryName) {
        return fullPath;
      }
    }
  }
  throw new Error(`Unable to locate ${binaryName} inside extracted archive.`);
}

function installBinary(source, destinationDir, binaryName) {
  fs.mkdirSync(destinationDir, { recursive: true });
  const destination = path.join(destinationDir, binaryName);
  if (fs.existsSync(destination)) {
    fs.rmSync(destination, { force: true });
  }
  fs.copyFileSync(source, destination);
  if (process.platform !== 'win32') {
    fs.chmodSync(destination, 0o755);
  }
  return destination;
}

async function install() {
  if (shouldSkipInstall()) {
    log('Skipping native binary download (EVENTDBX_SKIP_INSTALL set).');
    return;
  }

  const repo = resolveRepo();
  const target = resolveTarget();
  const versionTag = resolveVersionTag();
  const assetName = `eventdbx-${target.triple}${target.archive}`;
  const downloadUrl = `https://github.com/${repo}/releases/download/${versionTag}/${assetName}`;
  const binDir = path.join(__dirname, '..', 'bin');
  const nativeDir = path.join(binDir, 'native');
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'eventdbx-'));
  const archivePath = path.join(tmpDir, assetName);
  const extractDir = path.join(tmpDir, 'extract');

  fs.rmSync(nativeDir, { recursive: true, force: true });
  fs.mkdirSync(nativeDir, { recursive: true });

  try {
    await downloadFile(downloadUrl, archivePath);
    extractArchive(archivePath, target, extractDir);
    const extractedBinary = findBinary(extractDir, target.binary);
    const installedPath = installBinary(extractedBinary, nativeDir, target.binary);
    log(`Installed EventDBX ${versionTag} for ${target.triple} at ${installedPath}`);
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
}

install().catch(error => {
  const message = error && typeof error.message === 'string' ? error.message : String(error);
  console.error(`[eventdbx] Failed to install the CLI: ${message}`);
  if (process.env.DEBUG_EVENTDBX_INSTALL) {
    console.error(error);
  }
  process.exitCode = 1;
});
