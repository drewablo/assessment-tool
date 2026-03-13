import test from 'node:test';
import assert from 'node:assert/strict';
import { freshnessBadgeClass, formatPercentile } from './intelligence.js';

test('formatPercentile formats null and numeric values', () => {
  assert.equal(formatPercentile(null), 'N/A');
  assert.equal(formatPercentile(84.2), 'P84');
});

test('freshnessBadgeClass maps stale pipelines to red treatment', () => {
  assert.match(freshnessBadgeClass('stale'), /bg-red-100/);
});
