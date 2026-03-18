import type { FeatureCollection } from "geojson";
import {
  DashboardMetricOption,
  DashboardSeries,
  DashboardSidebarItem,
  DashboardTabItem,
  DashboardDistributionBucket,
  DashboardTimeSeriesPoint,
  ParameterBarField,
  ZipDrilldownData,
} from "@/lib/dashboard";
import type { CompetitorSchool } from "@/lib/types";

export type DashboardModuleSlug = "schools" | "elder-care" | "housing";

export interface DashboardPreviewModule {
  slug: DashboardModuleSlug;
  label: string;
  eyebrow: string;
  title: string;
  description: string;
  primaryLabel: string;
  primaryValue: string;
  secondaryLabel: string;
  secondaryValue: string;
  driveTimeMinutes: number;
  zipCount: number;
  sidebarItems: DashboardSidebarItem[];
  tabs: DashboardTabItem[];
  metricOptions: DashboardMetricOption[];
  featureCollection: FeatureCollection;
  trendTitle: string;
  trendSubtitle: string;
  trendSeries: DashboardSeries[];
  trendData: DashboardTimeSeriesPoint[];
  distributionTitle: string;
  distributionSubtitle: string;
  distributionData: DashboardDistributionBucket[];
  zipDrilldowns: Record<string, ZipDrilldownData>;
  highlightCards: Array<{ label: string; value: string; detail: string }>;
  address?: string;
  parameterFields?: ParameterBarField[];
  competitors?: CompetitorSchool[];
  competitorCounts?: {
    catholicCount: number;
    totalPrivateCount: number;
    radiusMiles: number;
  };
}

const sharedTabs: DashboardTabItem[] = [
  { key: "summary", label: "Summary" },
  { key: "distribution", label: "Distribution" },
  { key: "projections", label: "Projections" },
  { key: "map_view", label: "Map View" },
  { key: "drilldown", label: "ZIP Drilldown" },
];

const schoolsConfig: DashboardPreviewModule = {
  slug: "schools",
  label: "Schools",
  eyebrow: "Schools dashboard",
  title: "School Market View",
  description: "A schools-focused dashboard shell for market overview, affordability, student body, enrollment, and competitor review.",
  primaryLabel: "tuition",
  primaryValue: "$27,950",
  secondaryLabel: "View",
  secondaryValue: "All ZIPs",
  driveTimeMinutes: 30,
  zipCount: 18,
  sidebarItems: [
    { key: "market_overview", title: "Market Overview", description: "Population, income, and diversity context for the school catchment.", badge: "Core" },
    { key: "affordability", title: "Affordability", description: "High-income families, tuition gap, and distribution shifts." },
    { key: "enrollment", title: "Enrollment", description: "Market depth and competitor pressure on enrollment goals." },
    { key: "student_body", title: "Student Body", description: "School-age cohort mix and near-term projection signals." },
    { key: "competitors", title: "Competitors", description: "Map and table views of the nearby school landscape." },
  ],
  tabs: sharedTabs,
  metricOptions: [
    { key: "schoolAgePopulation", label: "School-Age Population" },
    { key: "familiesWithChildren", label: "Families with Children" },
    { key: "medianFamilyIncome", label: "Median Family Income", format: "currency" },
  ],
  featureCollection: {
    type: "FeatureCollection",
    features: [
      {
        type: "Feature",
        properties: { zipCode: "33971", name: "33971 Lehigh Acres", schoolAgePopulation: 9200, familiesWithChildren: 4867, medianFamilyIncome: 66098 },
        geometry: { type: "Polygon", coordinates: [[[-81.93, 26.59], [-81.86, 26.59], [-81.86, 26.66], [-81.93, 26.66], [-81.93, 26.59]]] },
      },
      {
        type: "Feature",
        properties: { zipCode: "33913", name: "33913 Fort Myers", schoolAgePopulation: 7400, familiesWithChildren: 3924, medianFamilyIncome: 91144 },
        geometry: { type: "Polygon", coordinates: [[[-81.86, 26.57], [-81.77, 26.57], [-81.77, 26.66], [-81.86, 26.66], [-81.86, 26.57]]] },
      },
      {
        type: "Feature",
        properties: { zipCode: "33967", name: "33967 Fort Myers", schoolAgePopulation: 5600, familiesWithChildren: 2718, medianFamilyIncome: 80774 },
        geometry: { type: "Polygon", coordinates: [[[-81.88, 26.50], [-81.79, 26.50], [-81.79, 26.57], [-81.88, 26.57], [-81.88, 26.50]]] },
      },
    ],
  },
  trendTitle: "High-Income Families ($200K+)",
  trendSubtitle: "Historical values are solid and projected values are dashed to keep forecasts clearly distinct from observed data.",
  trendSeries: [
    { key: "highIncome", label: "High-Income Families", color: "#16a34a" },
    { key: "families", label: "Families with Children", color: "#2563eb" },
  ],
  trendData: [
    { year: 2019, families: 9180, highIncome: 4630 },
    { year: 2020, families: 9410, highIncome: 4875 },
    { year: 2021, families: 9720, highIncome: 5250 },
    { year: 2022, families: 10030, highIncome: 5690 },
    { year: 2023, families: 10210, highIncome: 6030 },
    { year: 2024, families: 10580, highIncome: 6450 },
    { year: 2025, families: 10860, highIncome: 6845, projected: true },
    { year: 2026, families: 11140, highIncome: 7240, projected: true },
    { year: 2027, families: 11470, highIncome: 7710, projected: true },
    { year: 2028, families: 11850, highIncome: 8235, projected: true },
    { year: 2029, families: 12210, highIncome: 8790, projected: true },
  ],
  distributionTitle: "2024 vs 2029 Distribution of Household Income",
  distributionSubtitle: "Comparison buckets show where tuition-paying capacity is expected to deepen over the next five years.",
  distributionData: [
    { bucket: "<$50K", primary: 1342, comparison: 1210 },
    { bucket: "$50K-$75K", primary: 2140, comparison: 2265 },
    { bucket: "$75K-$100K", primary: 1982, comparison: 2148 },
    { bucket: "$100K-$125K", primary: 1724, comparison: 1836 },
    { bucket: "$125K-$150K", primary: 1360, comparison: 1475 },
    { bucket: "$150K-$200K", primary: 1088, comparison: 1248 },
    { bucket: "$200K-$350K", primary: 904, comparison: 1125 },
    { bucket: ">$350K", primary: 290, comparison: 406 },
  ],
  zipDrilldowns: {
    "33971": {
      zipCode: "33971",
      placeLabel: "Lehigh Acres",
      summary: "Strong family growth with improving income capacity and a narrowing tuition gap.",
      currentYear: 2024,
      projectedYear: 2029,
      metrics: [
        { label: "Families with Children", current: 4867, projected: 5326 },
        { label: "Median Family Income", current: 66098, projected: 72651, format: "currency" },
        { label: "Financial Gap", current: 27000, projected: 26000, format: "currency", invertChange: true },
      ],
      distribution: [
        { bucket: "<$50K", current: 580, projected: 512 },
        { bucket: "$25K-$50K", current: 1041, projected: 1022 },
        { bucket: "$50K-$75K", current: 1262, projected: 1298 },
        { bucket: "$75K-$100K", current: 938, projected: 1006 },
        { bucket: "$100K-$125K", current: 543, projected: 602 },
        { bucket: "$125K-$150K", current: 342, projected: 388 },
        { bucket: "$150K-$200K", current: 423, projected: 486 },
        { bucket: ">$200K", current: 175, projected: 236 },
      ],
    },
    "33913": {
      zipCode: "33913",
      placeLabel: "Fort Myers",
      summary: "Highest-income ZIP in the catchment with a large projected gain in affluent households.",
      currentYear: 2024,
      projectedYear: 2029,
      metrics: [
        { label: "Families with Children", current: 3924, projected: 4295 },
        { label: "Median Family Income", current: 91144, projected: 100882, format: "currency" },
        { label: "Financial Gap", current: 12000, projected: 8800, format: "currency", invertChange: true },
      ],
      distribution: [
        { bucket: "<$50K", current: 211, projected: 168 },
        { bucket: "$25K-$50K", current: 348, projected: 310 },
        { bucket: "$50K-$75K", current: 621, projected: 588 },
        { bucket: "$75K-$100K", current: 774, projected: 812 },
        { bucket: "$100K-$125K", current: 642, projected: 711 },
        { bucket: "$125K-$150K", current: 518, projected: 596 },
        { bucket: "$150K-$200K", current: 497, projected: 618 },
        { bucket: ">$200K", current: 313, projected: 492 },
      ],
    },
  },
  highlightCards: [
    { label: "Market depth", value: "10.6×", detail: "Addressable market versus reference enrollment target." },
    { label: "Top ZIP", value: "33971", detail: "Largest families-with-children base in the catchment." },
    { label: "Projected gain", value: "+36.3%", detail: "High-income families through 2029 in the strongest ZIP cluster." },
  ],
};

const elderCareConfig: DashboardPreviewModule = {
  slug: "elder-care",
  label: "Elder Care",
  eyebrow: "Elder care dashboard",
  title: "Elder Care Market View",
  description: "A care-market dashboard shell for community profile, facilities, partnership viability, financial context, and projections.",
  primaryLabel: "care type",
  primaryValue: "Assisted Living",
  secondaryLabel: "Priority",
  secondaryValue: "Mission-aligned",
  driveTimeMinutes: 30,
  zipCount: 14,
  sidebarItems: [
    { key: "community_profile", title: "Community Profile", description: "Senior population, age concentration, and growth context.", badge: "Core" },
    { key: "market_landscape", title: "Market Landscape", description: "Facilities, ratings, occupancy, and capacity pressure." },
    { key: "partnership_viability", title: "Partnership Viability", description: "Underserved ZIPs and quality gaps in the local care system." },
    { key: "financial_context", title: "Financial Context", description: "Income distribution and mission-sensitive affordability context." },
    { key: "projections", title: "Projections", description: "5-year and 10-year senior cohort growth outlook." },
  ],
  tabs: sharedTabs,
  metricOptions: [
    { key: "seniors65Plus", label: "Seniors 65+" },
    { key: "seniors75Plus", label: "Seniors 75+" },
    { key: "medianSeniorIncome", label: "Senior Household Income", format: "currency" },
  ],
  featureCollection: {
    type: "FeatureCollection",
    features: [
      {
        type: "Feature",
        properties: { zipCode: "33907", name: "33907 Fort Myers", seniors65Plus: 8420, seniors75Plus: 3110, medianSeniorIncome: 54800 },
        geometry: { type: "Polygon", coordinates: [[[-81.93, 26.56], [-81.85, 26.56], [-81.85, 26.62], [-81.93, 26.62], [-81.93, 26.56]]] },
      },
      {
        type: "Feature",
        properties: { zipCode: "33919", name: "33919 Fort Myers", seniors65Plus: 9640, seniors75Plus: 4025, medianSeniorIncome: 61200 },
        geometry: { type: "Polygon", coordinates: [[[-81.95, 26.51], [-81.86, 26.51], [-81.86, 26.56], [-81.95, 26.56], [-81.95, 26.51]]] },
      },
      {
        type: "Feature",
        properties: { zipCode: "33908", name: "33908 Fort Myers", seniors65Plus: 7130, seniors75Plus: 2650, medianSeniorIncome: 68750 },
        geometry: { type: "Polygon", coordinates: [[[-81.95, 26.43], [-81.85, 26.43], [-81.85, 26.51], [-81.95, 26.51], [-81.95, 26.43]]] },
      },
    ],
  },
  trendTitle: "Senior Population Outlook",
  trendSubtitle: "Module-specific projections distinguish observed senior growth from actuarial-style forward estimates.",
  trendSeries: [
    { key: "seniors65Plus", label: "Seniors 65+", color: "#2563eb" },
    { key: "seniors85Plus", label: "Seniors 85+", color: "#16a34a" },
  ],
  trendData: [
    { year: 2019, seniors65Plus: 21480, seniors85Plus: 3180 },
    { year: 2020, seniors65Plus: 21860, seniors85Plus: 3265 },
    { year: 2021, seniors65Plus: 22340, seniors85Plus: 3395 },
    { year: 2022, seniors65Plus: 22820, seniors85Plus: 3510 },
    { year: 2023, seniors65Plus: 23140, seniors85Plus: 3655 },
    { year: 2024, seniors65Plus: 23610, seniors85Plus: 3820 },
    { year: 2025, seniors65Plus: 24025, seniors85Plus: 3990, projected: true },
    { year: 2026, seniors65Plus: 24490, seniors85Plus: 4175, projected: true },
    { year: 2027, seniors65Plus: 24985, seniors85Plus: 4370, projected: true },
    { year: 2028, seniors65Plus: 25510, seniors85Plus: 4560, projected: true },
    { year: 2029, seniors65Plus: 26070, seniors85Plus: 4770, projected: true },
  ],
  distributionTitle: "Senior Household Income Distribution",
  distributionSubtitle: "Current and projected buckets help identify mission-fit and private-pay balance across the service area.",
  distributionData: [
    { bucket: "<$35K", primary: 2640, comparison: 2530 },
    { bucket: "$35K-$50K", primary: 3280, comparison: 3210 },
    { bucket: "$50K-$75K", primary: 4135, comparison: 4250 },
    { bucket: "$75K-$100K", primary: 2980, comparison: 3125 },
    { bucket: "$100K-$150K", primary: 2210, comparison: 2385 },
    { bucket: "$150K+", primary: 1075, comparison: 1240 },
  ],
  zipDrilldowns: {
    "33919": {
      zipCode: "33919",
      placeLabel: "Fort Myers",
      summary: "Largest 75+ cohort in the catchment with a comparatively healthy middle-income base.",
      currentYear: 2024,
      projectedYear: 2029,
      metrics: [
        { label: "Seniors 65+", current: 9640, projected: 10610 },
        { label: "Seniors 75+", current: 4025, projected: 4588 },
        { label: "Median Senior Income", current: 61200, projected: 66450, format: "currency" },
      ],
      distribution: [
        { bucket: "<$35K", current: 880, projected: 820 },
        { bucket: "$35K-$50K", current: 1090, projected: 1052 },
        { bucket: "$50K-$75K", current: 1544, projected: 1598 },
        { bucket: "$75K-$100K", current: 1160, projected: 1224 },
        { bucket: "$100K-$150K", current: 822, projected: 901 },
        { bucket: "$150K+", current: 419, projected: 502 },
      ],
    },
    "33907": {
      zipCode: "33907",
      placeLabel: "Fort Myers",
      summary: "Strong mission-aligned senior base with meaningful 85+ growth pressure and moderate income levels.",
      currentYear: 2024,
      projectedYear: 2029,
      metrics: [
        { label: "Seniors 65+", current: 8420, projected: 9280 },
        { label: "Seniors 75+", current: 3110, projected: 3545 },
        { label: "Median Senior Income", current: 54800, projected: 58950, format: "currency" },
      ],
      distribution: [
        { bucket: "<$35K", current: 964, projected: 901 },
        { bucket: "$35K-$50K", current: 1180, projected: 1164 },
        { bucket: "$50K-$75K", current: 1360, projected: 1398 },
        { bucket: "$75K-$100K", current: 1015, projected: 1080 },
        { bucket: "$100K-$150K", current: 640, projected: 701 },
        { bucket: "$150K+", current: 262, projected: 318 },
      ],
    },
  },
  highlightCards: [
    { label: "Underserved ZIP", value: "33907", detail: "High 75+ growth with lower facility density than the rest of the catchment." },
    { label: "5-year growth", value: "+10.4%", detail: "Projected increase in the catchment 85+ cohort." },
    { label: "Quality gap", value: "2 ZIPs", detail: "Areas where senior concentration and weaker facility ratings overlap." },
  ],
};

const housingConfig: DashboardPreviewModule = {
  slug: "housing",
  label: "Low-Income Housing",
  eyebrow: "Housing dashboard",
  title: "Affordable Housing Market View",
  description: "A housing-focused dashboard shell for community profile, need assessment, existing resources, and demographic trends.",
  primaryLabel: "AMI threshold",
  primaryValue: "60% AMI",
  secondaryLabel: "Priority",
  secondaryValue: "Cost-burdened renters",
  driveTimeMinutes: 30,
  zipCount: 16,
  sidebarItems: [
    { key: "community_profile", title: "Community Profile", description: "Population, poverty pressure, and existing housing stock context.", badge: "Core" },
    { key: "need_assessment", title: "Need Assessment", description: "Income buckets, cost burden, and housing-gap signals." },
    { key: "existing_resources", title: "Existing Resources", description: "Subsidized housing, LIHTC, and resource concentration." },
    { key: "demographic_trends", title: "Demographic Trends", description: "Population growth, migration pressure, and diversity shifts." },
  ],
  tabs: sharedTabs,
  metricOptions: [
    { key: "costBurdenedHouseholds", label: "Cost-Burdened Households" },
    { key: "renterHouseholds", label: "Renter Households" },
    { key: "medianHouseholdIncome", label: "Median Household Income", format: "currency" },
  ],
  featureCollection: {
    type: "FeatureCollection",
    features: [
      {
        type: "Feature",
        properties: { zipCode: "33916", name: "33916 Fort Myers", costBurdenedHouseholds: 3225, renterHouseholds: 4410, medianHouseholdIncome: 39850 },
        geometry: { type: "Polygon", coordinates: [[[-81.89, 26.62], [-81.82, 26.62], [-81.82, 26.68], [-81.89, 26.68], [-81.89, 26.62]]] },
      },
      {
        type: "Feature",
        properties: { zipCode: "33901", name: "33901 Fort Myers", costBurdenedHouseholds: 2540, renterHouseholds: 3710, medianHouseholdIncome: 45210 },
        geometry: { type: "Polygon", coordinates: [[[-81.91, 26.60], [-81.84, 26.60], [-81.84, 26.62], [-81.91, 26.62], [-81.91, 26.60]]] },
      },
      {
        type: "Feature",
        properties: { zipCode: "33905", name: "33905 Fort Myers", costBurdenedHouseholds: 2145, renterHouseholds: 3180, medianHouseholdIncome: 43120 },
        geometry: { type: "Polygon", coordinates: [[[-81.83, 26.59], [-81.74, 26.59], [-81.74, 26.66], [-81.83, 26.66], [-81.83, 26.59]]] },
      },
    ],
  },
  trendTitle: "Cost-Burdened Household Outlook",
  trendSubtitle: "Observed housing pressure is shown separately from projected burden growth to avoid overstating forecast certainty.",
  trendSeries: [
    { key: "costBurdenedHouseholds", label: "Cost-Burdened Households", color: "#2563eb" },
    { key: "hudEligibleHouseholds", label: "HUD-Eligible Households", color: "#16a34a" },
  ],
  trendData: [
    { year: 2019, costBurdenedHouseholds: 6520, hudEligibleHouseholds: 4980 },
    { year: 2020, costBurdenedHouseholds: 6835, hudEligibleHouseholds: 5140 },
    { year: 2021, costBurdenedHouseholds: 7140, hudEligibleHouseholds: 5295 },
    { year: 2022, costBurdenedHouseholds: 7425, hudEligibleHouseholds: 5510 },
    { year: 2023, costBurdenedHouseholds: 7680, hudEligibleHouseholds: 5735 },
    { year: 2024, costBurdenedHouseholds: 7910, hudEligibleHouseholds: 5890 },
    { year: 2025, costBurdenedHouseholds: 8125, hudEligibleHouseholds: 6065, projected: true },
    { year: 2026, costBurdenedHouseholds: 8350, hudEligibleHouseholds: 6240, projected: true },
    { year: 2027, costBurdenedHouseholds: 8580, hudEligibleHouseholds: 6425, projected: true },
    { year: 2028, costBurdenedHouseholds: 8805, hudEligibleHouseholds: 6615, projected: true },
    { year: 2029, costBurdenedHouseholds: 9055, hudEligibleHouseholds: 6830, projected: true },
  ],
  distributionTitle: "Income Distribution for Housing Need Review",
  distributionSubtitle: "The comparison view highlights how many households remain concentrated below affordability thresholds.",
  distributionData: [
    { bucket: "<$25K", primary: 2080, comparison: 2115 },
    { bucket: "$25K-$50K", primary: 3210, comparison: 3295 },
    { bucket: "$50K-$75K", primary: 2485, comparison: 2560 },
    { bucket: "$75K-$100K", primary: 1460, comparison: 1530 },
    { bucket: "$100K-$150K", primary: 880, comparison: 925 },
    { bucket: "$150K+", primary: 335, comparison: 372 },
  ],
  zipDrilldowns: {
    "33916": {
      zipCode: "33916",
      placeLabel: "Fort Myers",
      summary: "Highest cost-burden pressure in the catchment with a large renter base and persistent sub-$50K concentration.",
      currentYear: 2024,
      projectedYear: 2029,
      metrics: [
        { label: "Cost-Burdened Households", current: 3225, projected: 3575 },
        { label: "Renter Households", current: 4410, projected: 4630 },
        { label: "Median Household Income", current: 39850, projected: 42100, format: "currency" },
      ],
      distribution: [
        { bucket: "<$25K", current: 960, projected: 1010 },
        { bucket: "$25K-$50K", current: 1420, projected: 1512 },
        { bucket: "$50K-$75K", current: 730, projected: 760 },
        { bucket: "$75K-$100K", current: 380, projected: 401 },
        { bucket: "$100K-$150K", current: 180, projected: 194 },
        { bucket: "$150K+", current: 52, projected: 63 },
      ],
    },
    "33905": {
      zipCode: "33905",
      placeLabel: "Fort Myers",
      summary: "Large working-family renter cluster with steady pressure across the low-to-middle income bands.",
      currentYear: 2024,
      projectedYear: 2029,
      metrics: [
        { label: "Cost-Burdened Households", current: 2145, projected: 2360 },
        { label: "Renter Households", current: 3180, projected: 3335 },
        { label: "Median Household Income", current: 43120, projected: 45890, format: "currency" },
      ],
      distribution: [
        { bucket: "<$25K", current: 620, projected: 648 },
        { bucket: "$25K-$50K", current: 1015, projected: 1056 },
        { bucket: "$50K-$75K", current: 845, projected: 872 },
        { bucket: "$75K-$100K", current: 402, projected: 430 },
        { bucket: "$100K-$150K", current: 228, projected: 245 },
        { bucket: "$150K+", current: 70, projected: 84 },
      ],
    },
  },
  highlightCards: [
    { label: "Need gap", value: "3,225 HH", detail: "Largest cost-burdened ZIP in the catchment." },
    { label: "Projected burden", value: "+14.5%", detail: "Expected growth in burdened households through 2029." },
    { label: "Resource gap", value: "2 ZIPs", detail: "High-need areas with the thinnest subsidized housing presence." },
  ],
};

export const dashboardPreviewModules: DashboardPreviewModule[] = [
  schoolsConfig,
  elderCareConfig,
  housingConfig,
];

export function getDashboardPreviewModule(slug: string): DashboardPreviewModule | undefined {
  return dashboardPreviewModules.find((module) => module.slug === slug);
}
