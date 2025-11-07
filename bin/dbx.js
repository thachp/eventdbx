#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');

const binaryName = process.platform === 'win32' ? 'dbx.exe' : 'dbx';
const binaryPath = path.join(__dirname, 'native', binaryName);

if (!fs.existsSync(binaryPath)) {
  console.error(
    '[eventdbx] Native binary not found. Re-run `npm install eventdbx` or set EVENTDBX_SKIP_INSTALL=0.',
  );
  process.exit(1);
}

const child = spawn(binaryPath, process.argv.slice(2), {
  stdio: 'inherit',
  windowsHide: true,
});

child.on('close', code => {
  process.exit(code ?? 1);
});

child.on('error', err => {
  console.error(`[eventdbx] Failed to start EventDBX CLI: ${err.message}`);
  process.exit(1);
});
