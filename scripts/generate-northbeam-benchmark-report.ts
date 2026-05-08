import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  buildNorthbeamBenchmarkReport,
  renderNorthbeamBenchmarkReportJson,
  renderNorthbeamBenchmarkReportMarkdown
} from '../src/modules/attribution/benchmark.js';

async function main(): Promise<void> {
  const report = buildNorthbeamBenchmarkReport();
  const json = renderNorthbeamBenchmarkReportJson(report);
  const markdown = renderNorthbeamBenchmarkReportMarkdown(report);
  const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
  const outputDir = path.join(repoRoot, 'docs', 'benchmarks');

  await mkdir(outputDir, { recursive: true });
  await writeFile(path.join(outputDir, 'northbeam-parity-report.json'), json, 'utf8');
  await writeFile(path.join(outputDir, 'northbeam-parity-report.md'), markdown, 'utf8');
}

void main();
