# Dashboard API Contracts

This document defines the dashboard response envelope and the metric/property naming conventions used by the interactive market dashboard.

## Shared response envelope

```ts
interface DashboardResponse<T> {
  catchment: {
    center: { lat: number; lng: number; address: string };
    driveTimeMinutes: number;
    zipCodes: string[];
    geojson: GeoJSON.FeatureCollection;
  };
  data: T;
  metadata: {
    dataYear: number;
    projectionYears: number[];
    lastUpdated: string;
    confidenceBand?: "high" | "medium" | "low";
  };
}
```

## Shared time-series point

```ts
interface TimeSeriesPoint {
  year: number;
  value: number;
  projected: boolean;
}
```

## Example module payloads

### Schools income dashboard payload

```ts
interface SchoolIncomeDashboardData {
  summary: {
    medianHouseholdIncome: number;
    medianFamilyIncome: number;
    highIncomeFamilies: number;
  };
  timeSeries: Record<"familiesWithChildren" | "highIncomeFamilies", TimeSeriesPoint[]>;
  distribution: { bracket: string; count2024: number; count2029: number }[];
  byZip: Record<string, {
    medianFamilyIncome: number;
    familiesWithChildren: number;
    projectedMedianFamilyIncome: number;
    projectedFamiliesWithChildren: number;
    distribution: { bracket: string; count2024: number; count2029: number }[];
  }>;
}
```

### Elder care dashboard payload

```ts
interface ElderCareDashboardData {
  summary: {
    seniors65Plus: number;
    seniors75Plus: number;
    seniorsLivingAlone: number;
  };
  timeSeries: Record<"seniors65Plus" | "seniors75Plus", TimeSeriesPoint[]>;
  facilities: Array<{
    name: string;
    lat: number;
    lng: number;
    rating?: number;
    beds?: number;
    occupancyPct?: number;
  }>;
  byZip: Record<string, {
    seniors65Plus: number;
    seniors75Plus: number;
    projected5Year: number;
    projected10Year: number;
  }>;
}
```

### Housing dashboard payload

```ts
interface HousingDashboardData {
  summary: {
    costBurdenedHouseholds: number;
    renterHouseholds: number;
    hudEligibleHouseholds: number;
  };
  timeSeries: Record<"costBurdenedHouseholds" | "hudEligibleHouseholds", TimeSeriesPoint[]>;
  distribution: { bracket: string; households2024: number; households2029: number }[];
  byZip: Record<string, {
    costBurdenedHouseholds: number;
    renterHouseholds: number;
    projectedCostBurdenedHouseholds: number;
  }>;
}
```

## Notes

- `geojson` is intended for ZIP/ZCTA boundaries rather than tract boundaries.
- Projected values must be visually differentiated in the frontend.
- Existing `AnalysisResponse` remains intact for the current assessment flow; these contracts are additive.
- Live implementation note: `/api/dashboard` now returns an additive dashboard payload with `zip_codes`, ZIP `FeatureCollection`, per-ZIP metric maps, drilldowns, module-specific series metadata, and projection/freshness metadata.
- Geometry payloads are intended to come from the cached Census ZCTA bundle produced by `python -m pipeline.cli ingest-zcta`; the API reports `geometry_source`, and the endpoint now returns a structured `ZCTA_CACHE_MISSING` error if the cache is absent or empty.

## Per-ZIP metric keys used in `FeatureCollection.properties`

These keys are attached directly to each ZIP feature so the frontend choropleth can switch metrics generically.

### Schools

- `schoolAgePopulation`
- `familiesWithChildren`
- `medianFamilyIncome`
- `competitorCount`

### Elder Care

- `seniors65Plus`
- `seniors75Plus`
- `medianSeniorIncome`
- `facilityCount`

### Housing

- `totalPopulation`
- `costBurdenedHouseholds`
- `costBurdenRate`
- `renterHouseholds`
- `hudEligibleHouseholds`
- `medianHouseholdIncome`
