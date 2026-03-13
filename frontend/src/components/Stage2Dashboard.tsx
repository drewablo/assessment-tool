import { Stage2Score } from "@/lib/types";

type Props = {
  stage2: Stage2Score;
  ministryType?: "schools" | "housing" | "elder_care";
};

type Band = "strong" | "caution" | "at_risk" | "critical" | "insufficient";

type MetricConfig = {
  key: string;
  name: string;
  benchmark: string;
};

const schoolGroups: { title: string; metrics: MetricConfig[] }[] = [
  {
    title: "Operating Health",
    metrics: [
      { key: "net_operating_position", name: "Net Operating Position", benchmark: "Higher is healthier (operating only)" },
      { key: "tuition_dependency_ratio", name: "Tuition Dependency Ratio", benchmark: "Balanced revenue mix preferred" },
      { key: "non_operating_revenue_share", name: "Non-Operating Revenue Share", benchmark: "Lower dependency is healthier" },
    ],
  },
  {
    title: "Per-Student Efficiency",
    metrics: [
      { key: "effective_tuition_rate", name: "Effective Tuition Rate", benchmark: "Computed when tuition aid is available" },
      { key: "revenue_per_student", name: "Revenue per Student", benchmark: "Higher supports operating resilience" },
      { key: "expense_per_student", name: "Expense per Student", benchmark: "Lower is generally healthier" },
    ],
  },
  {
    title: "Year-over-Year Trends",
    metrics: [
      { key: "enrollment_trend", name: "Enrollment Trend", benchmark: "Direction and % change" },
      { key: "revenue_trend", name: "Revenue Trend", benchmark: "Direction and % change" },
      { key: "expense_trend", name: "Expense Trend", benchmark: "Direction and % change" },
    ],
  },
];

const missingInputLabels: Record<string, string> = {
  historical_financials: "historical financial years",
  school_audit_financials: "audit-derived financial years",
  fiscal_year: "fiscal year",
  tuition_revenue: "tuition revenue",
  other_revenue: "other revenue",
  total_expenses: "total operating expenses",
  enrollment: "enrollment",
  school_stage2_confirmed: "user confirmation",
};

function scoreBand(score: number | null): Band {
  if (score == null) return "insufficient";
  if (score >= 75) return "strong";
  if (score >= 55) return "caution";
  if (score >= 35) return "at_risk";
  return "critical";
}

function bandClasses(band: Band): string {
  if (band === "strong") return "bg-green-50 text-green-700";
  if (band === "caution") return "bg-yellow-50 text-yellow-700";
  if (band === "at_risk") return "bg-orange-50 text-orange-700";
  if (band === "critical") return "bg-red-50 text-red-700";
  return "bg-gray-100 text-gray-600";
}

function bandLabel(band: Band): string {
  if (band === "strong") return "Strong";
  if (band === "caution") return "Caution";
  if (band === "at_risk") return "At Risk";
  if (band === "critical") return "Critical";
  return "Insufficient Data";
}

function interpretation(metricName: string, score: number | null): string {
  if (score == null) return "Insufficient data to score this indicator.";
  if (score >= 75) return `${metricName} is within a healthy range.`;
  if (score >= 55) return `${metricName} is acceptable but worth monitoring.`;
  if (score >= 35) return `${metricName} is below benchmark — review recommended.`;
  return `${metricName} is a significant risk factor.`;
}

export default function Stage2Dashboard({ stage2, ministryType = "schools" }: Props) {
  if (stage2.readiness === "not_ready") {
    return (
      <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-5">
        <h3 className="text-lg font-semibold text-gray-900">{ministryType === "schools" ? "Institutional Financial Health" : ministryType === "housing" ? "Housing Operating Health" : "Elder Care Operating Health"}</h3>
        <p className="text-sm text-gray-600 mt-2">Enter financial data to generate Stage 2 institutional analysis.</p>
      </div>
    );
  }

  const componentsByKey = new Map(stage2.components.map((c) => [c.key, c]));
  const groups = ministryType === "schools"
    ? schoolGroups
    : [{
        title: "Operating KPI Components",
        metrics: stage2.components.map((component) => ({
          key: component.key,
          name: component.label || component.key.replaceAll("_", " "),
          benchmark: "Component returned by backend Stage 2 scoring.",
        })),
      }];
  const allMetrics = groups.flatMap((group) =>
    group.metrics.map((m) => ({ ...m, score: componentsByKey.get(m.key)?.score ?? null }))
  );
  const ranked = [...allMetrics].filter((m) => m.score != null).sort((a, b) => (a.score ?? 0) - (b.score ?? 0));
  const topRisks = ranked.slice(0, 2);
  const strengths = [...ranked].reverse().slice(0, 2);
  const missingInputsText = stage2.missing_inputs.map((i) => missingInputLabels[i] ?? i.replaceAll("_", " ")).join(", ");
  const overallBand = scoreBand(stage2.score);

  return (
    <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-5 space-y-4">
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-2">
        <h3 className="text-lg font-semibold text-gray-900">{ministryType === "schools" ? "Institutional Financial Health" : ministryType === "housing" ? "Housing Operating Health" : "Elder Care Operating Health"}</h3>
        {stage2.available && stage2.score != null && (
          <span className={`inline-flex items-center rounded-full px-3 py-1 text-sm font-semibold ${bandClasses(overallBand)}`}>
            Stage 2 Score: {stage2.score}/100
          </span>
        )}
      </div>

      {stage2.readiness === "partial" && (
        <div className="rounded-lg border border-yellow-200 bg-yellow-50 p-3 text-sm text-yellow-800">
          Partial data — score is estimated from available inputs. Missing: {missingInputsText || "none listed"}
        </div>
      )}

      {groups.map((group) => (
        <section key={group.title} className="space-y-2">
          <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide">{group.title}</p>
          <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-3">
            {group.metrics.map((metric) => {
              const score = componentsByKey.get(metric.key)?.score ?? null;
              const band = scoreBand(score);
              return (
                <div key={metric.key} className="rounded-xl border border-gray-200 shadow-sm p-3 bg-white space-y-2">
                  <div className="flex items-start justify-between gap-2">
                    <p className="text-sm font-semibold text-gray-900">{metric.name}</p>
                    <span className={`rounded-full px-2 py-0.5 text-xs font-semibold ${bandClasses(band)}`}>{bandLabel(band)}</span>
                  </div>
                  <p className="text-xs text-gray-500">{score == null ? "—" : `${score}/100`}</p>
                  <p className="text-xs text-gray-600">{metric.benchmark}</p>
                  <p className="text-xs text-gray-700">{interpretation(metric.name, score)}</p>
                </div>
              );
            })}
          </div>
        </section>
      ))}

      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
        <div className="rounded-xl border border-gray-200 shadow-sm p-3 bg-white">
          <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-2">Top Risks</p>
          {topRisks.length === 0 ? (
            <p className="text-sm text-gray-500">Insufficient data to identify top risks.</p>
          ) : (
            <ul className="space-y-1">
              {topRisks.map((item) => (
                <li key={item.key} className="text-sm text-gray-700">{item.name}: {item.score}/100</li>
              ))}
            </ul>
          )}
        </div>
        <div className="rounded-xl border border-gray-200 shadow-sm p-3 bg-white">
          <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-2">Strengths</p>
          {strengths.length === 0 ? (
            <p className="text-sm text-gray-500">Insufficient data to identify strengths.</p>
          ) : (
            <ul className="space-y-1">
              {strengths.map((item) => (
                <li key={item.key} className="text-sm text-gray-700">{item.name}: {item.score}/100</li>
              ))}
            </ul>
          )}
        </div>
      </div>

      {!stage2.available && (
        <p className="text-sm text-gray-600">Enter 1–3 years of financial data above to unlock the full Stage 2 analysis.</p>
      )}
    </div>
  );
}
