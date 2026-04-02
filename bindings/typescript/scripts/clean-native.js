#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

const libRoot = path.resolve(__dirname, '..', 'lib');
fs.rmSync(libRoot, { recursive: true, force: true });
