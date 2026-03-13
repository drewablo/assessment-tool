export function freshnessBadgeClass(status) {
  if (status === 'fresh') return 'bg-emerald-100 text-emerald-700';
  if (status === 'aging') return 'bg-amber-100 text-amber-700';
  if (status === 'stale') return 'bg-red-100 text-red-700 ring-1 ring-red-300';
  return 'bg-gray-100 text-gray-600';
}

export function formatPercentile(value) {
  if (value === null) return 'N/A';
  return `P${Math.round(value)}`;
}
