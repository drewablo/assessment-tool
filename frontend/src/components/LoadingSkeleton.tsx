"use client";

/** Animated placeholder shown while an analysis is in progress. */
export default function LoadingSkeleton() {
  return (
    <div className="space-y-6 animate-pulse" aria-busy="true" aria-label="Loading analysis results">
      {/* Header placeholder */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
        <div className="space-y-2">
          <div className="h-6 w-48 bg-gray-200 rounded" />
          <div className="h-4 w-72 bg-gray-100 rounded" />
        </div>
        <div className="flex gap-2">
          <div className="h-9 w-28 bg-gray-100 rounded-lg" />
          <div className="h-9 w-28 bg-gray-100 rounded-lg" />
          <div className="h-9 w-32 bg-gray-200 rounded-lg" />
        </div>
      </div>

      {/* Score gauge row */}
      <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-6">
        <div className="flex flex-col sm:flex-row items-center gap-8">
          {/* Gauge circle */}
          <div className="w-36 h-36 rounded-full bg-gray-200 flex-shrink-0" />
          {/* Metric cards */}
          <div className="grid grid-cols-2 gap-4 flex-1 w-full">
            {[1, 2, 3, 4].map((i) => (
              <div key={i} className="rounded-lg border border-gray-100 p-4 space-y-2">
                <div className="h-3 w-24 bg-gray-200 rounded" />
                <div className="h-6 w-12 bg-gray-100 rounded" />
                <div className="h-3 w-16 bg-gray-100 rounded" />
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Demographics + map row */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-6 space-y-3">
          <div className="h-5 w-40 bg-gray-200 rounded" />
          {[1, 2, 3, 4, 5].map((i) => (
            <div key={i} className="flex justify-between">
              <div className="h-4 w-36 bg-gray-100 rounded" />
              <div className="h-4 w-20 bg-gray-100 rounded" />
            </div>
          ))}
        </div>
        <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
          <div className="h-64 bg-gray-100" />
        </div>
      </div>

      {/* Competitor table placeholder */}
      <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-6 space-y-3">
        <div className="h-5 w-44 bg-gray-200 rounded" />
        <div className="h-4 w-60 bg-gray-100 rounded" />
        <div className="space-y-2 mt-4">
          {[1, 2, 3, 4, 5, 6].map((i) => (
            <div key={i} className="flex gap-4">
              <div className="h-4 flex-1 bg-gray-100 rounded" />
              <div className="h-4 w-16 bg-gray-100 rounded" />
              <div className="h-4 w-20 bg-gray-100 rounded" />
              <div className="h-4 w-16 bg-gray-100 rounded" />
            </div>
          ))}
        </div>
      </div>

      {/* Screen reader status */}
      <p className="sr-only" role="status">
        Running analysis. This may take a few seconds…
      </p>
    </div>
  );
}
