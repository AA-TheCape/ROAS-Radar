import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import {
  buildNorthbeamBenchmarkReport,
  renderNorthbeamBenchmarkReportJson,
  renderNorthbeamBenchmarkReportMarkdown
} from '../src/modules/attribution/benchmark.js';

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

test('northbeam parity report artifacts stay in sync with the checked-in generator output', () => {
  const report = buildNorthbeamBenchmarkReport();
  const expectedJson = renderNorthbeamBenchmarkReportJson(report);
  const expectedMarkdown = renderNorthbeamBenchmarkReportMarkdown(report);
  const actualJson = readFileSync(path.join(repoRoot, 'docs/benchmarks/northbeam-parity-report.json'), 'utf8');
  const actualMarkdown = readFileSync(path.join(repoRoot, 'docs/benchmarks/northbeam-parity-report.md'), 'utf8');

  assert.equal(actualJson, expectedJson);
  assert.equal(actualMarkdown, expectedMarkdown);
});
