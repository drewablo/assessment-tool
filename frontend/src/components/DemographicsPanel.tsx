"use client";

import { DemographicData } from "@/lib/types";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from "recharts";

interface Props {
  demographics: DemographicData;
  countyName: string;
  ministryType: "schools" | "housing" | "elder_care";
  gender: "coed" | "boys" | "girls";
  gradeLevel: "k5" | "k8" | "high_school" | "k12";
}

const GRADE_AGES: Record<string, string> = {
  k5: "5–11",
  k8: "5–14",
  high_school: "14–17",
  k12: "5–17",
};

const GENDER_PREFIX: Record<string, string> = {
  coed: "",
  boys: "Boys",
  girls: "Girls",
};

/** E.g. "Boys (14–17)" or "School-Age (5–17)" for coed/k12 */
function schoolAgeLabel(gender: string, gradeLevel: string): string {
  const ages = GRADE_AGES[gradeLevel] ?? "5–17";
  const prefix = GENDER_PREFIX[gender] ?? "";
  return prefix ? `${prefix} (${ages})` : `School-Age (${ages})`;
}

function fmt(n: number | null | undefined, prefix = ""): string {
  if (n == null) return "N/A";
  return prefix + n.toLocaleString();
}

export default function DemographicsPanel({ demographics: d, countyName, ministryType, gender, gradeLevel }: Props) {
  const ageRange = GRADE_AGES[gradeLevel] ?? "5–17";
  const genderPrefix = GENDER_PREFIX[gender] ?? "";
  const funnelAgeLabel = genderPrefix ? `${genderPrefix} ${ageRange}` : `Age ${ageRange}`;
  const funnelCatholicLabel = genderPrefix
    ? `Est. Catholic\n${genderPrefix} ${ageRange}`
    : `Est. Catholic\n${ageRange}`;

  const targetPopulation = d.ministry_target_population ?? d.school_age_population;
  const hasSeniorOutlook = ministryType === "elder_care" && (d.seniors_projected_5yr != null || d.seniors_projected_10yr != null);

  const populationData = ministryType === "schools"
    ? [
        { name: "Total Pop.", value: d.total_population, color: "#6b7280" },
        { name: "Under 18", value: d.population_under_18, color: "#3b82f6" },
        { name: funnelAgeLabel, value: d.school_age_population, color: "#8b5cf6" },
        { name: funnelCatholicLabel, value: d.estimated_catholic_school_age, color: "#172d57" },
      ].filter((item) => item.value != null && item.value > 0)
    : [
        { name: "Total Pop.", value: d.total_population, color: "#6b7280" },
        {
          name: ministryType === "elder_care" ? "Target Seniors" : "Cost-Burdened HH",
          value: targetPopulation,
          color: "#172d57",
        },
      ].filter((item) => item.value != null && item.value > 0);

  return (
    <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-6">
      <h2 className="text-lg font-bold text-gray-900 mb-1">Demographics</h2>
      <p className="text-xs text-gray-400 mb-5">
        ACS 5-year estimates (2022) · {countyName}
        {ministryType === "schools" && (gender !== "coed" || gradeLevel !== "k12")
          ? ` · filtered for ${schoolAgeLabel(gender, gradeLevel)}`
          : ""}
      </p>

      {/* Key stats grid */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
        <Stat label="Total Population" value={fmt(d.total_population)} />
        <Stat label={ministryType === "schools" ? schoolAgeLabel(gender, gradeLevel) : ministryType === "elder_care" ? "Target Seniors" : "Cost-Burdened HH"} value={fmt(targetPopulation)} />
        {ministryType === "schools" && (
          <Stat
            label={`Est. Catholic ${genderPrefix ? genderPrefix + " " : ""}${ageRange}`}
            value={fmt(d.estimated_catholic_school_age)}
            note={`~${d.estimated_catholic_pct}% of pop.`}
          />
        )}
        <Stat
          label="Median HH Income"
          value={fmt(d.median_household_income, "$")}
        />
        <Stat
          label="Total Households"
          value={fmt(d.total_households)}
        />
        {ministryType === "schools" ? (
          <>
            <Stat
              label="Families w/ Children"
              value={fmt(d.families_with_children)}
              note={
                d.families_with_children && d.total_households
                  ? `${Math.round((d.families_with_children / d.total_households) * 100)}% of households`
                  : undefined
              }
            />
            <Stat
              label="Owner-Occupied"
              value={d.owner_occupied_pct != null ? `${d.owner_occupied_pct}%` : "N/A"}
              note="Stability indicator"
            />
            {d.pipeline_ratio != null && (
              <Stat
                label="K Pipeline (Under-5)"
                value={fmt(d.population_under_5)}
                note={`${(d.pipeline_ratio * 100).toFixed(0)}% of school-age · Score: ${d.pipeline_score ?? "N/A"}/100`}
              />
            )}
            {d.private_enrollment_rate_pct != null && (
              <Stat
                label="Private School Rate"
                value={`${d.private_enrollment_rate_pct.toFixed(1)}%`}
                note={`${d.private_enrollment_rate_pct > 10.5 ? "Above" : "Below"} natl avg (10.5%) · Score: ${d.private_enrollment_score ?? "N/A"}/100`}
              />
            )}
          </>
        ) : (
          <>
            <Stat
              label={ministryType === "elder_care" ? "Senior-Focused Demand Signal" : "Housing Need Signal"}
              value={targetPopulation != null && d.total_households ? `${Math.round((targetPopulation / d.total_households) * 100)}%` : "N/A"}
              note={ministryType === "elder_care" ? "Target seniors as share of households" : "Cost-burdened households as share of households"}
            />
            {ministryType === "housing" && (
              <>
                <Stat
                  label="HUD Tenant HH (Joined)"
                  value={fmt(d.hud_tenant_households)}
                  note="Tenant aggregates via exact HUD/tract joins"
                />
                <Stat
                  label="Nearby QCT Projects"
                  value={fmt(d.qct_designated_projects)}
                  note="HUD Qualified Census Tract designations"
                />
                <Stat
                  label="Nearby DDA Projects"
                  value={fmt(d.dda_designated_projects)}
                  note="HUD Difficult Development Area designations"
                />
              </>
            )}
          </>
        )}
      </div>

      {hasSeniorOutlook && (
        <div className="mb-6">
          <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-2">
            Senior Population Outlook
          </p>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <Stat label="Current" value={fmt(d.seniors_65_plus ?? targetPopulation)} />
            <Stat label="Est. 5-Year" value={fmt(d.seniors_projected_5yr)} />
            <Stat label="Est. 10-Year" value={fmt(d.seniors_projected_10yr)} />
          </div>
          <p className="text-xs text-gray-400 italic mt-2">
            Projections based on SSA period life table survival rates.
          </p>
        </div>
      )}

      {/* Population waterfall chart */}
      {populationData.length > 0 && (
        <div>
          <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-2">
            Population Funnel
          </p>
          <ResponsiveContainer width="100%" height={160}>
            <BarChart data={populationData} margin={{ top: 0, right: 0, left: 10, bottom: 0 }}>
              <XAxis dataKey="name" tick={{ fontSize: 11 }} />
              <YAxis tick={{ fontSize: 11 }} tickFormatter={(v) => `${(v / 1000).toFixed(0)}k`} />
              <Tooltip formatter={(v: number) => v.toLocaleString()} />
              <Bar dataKey="value" radius={[4, 4, 0, 0]}>
                {populationData.map((entry, index) => (
                  <Cell key={index} fill={entry.color} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
          <p className="text-xs text-gray-400 text-center mt-1">
            {ministryType === "schools"
              ? "Catholic school-age estimate uses CARA state-level Catholic population data"
              : ministryType === "elder_care"
              ? "Target seniors reflects elder-care scoring population for this market"
              : "Cost-burdened renter households reflect housing-need scoring population"}
          </p>
        </div>
      )}
    </div>
  );
}

function Stat({
  label,
  value,
  note,
}: {
  label: string;
  value: string;
  note?: string;
}) {
  return (
    <div className="bg-gray-50 rounded-lg p-3">
      <p className="text-xs text-gray-500 mb-0.5">{label}</p>
      <p className="text-lg font-bold text-gray-900">{value}</p>
      {note && <p className="text-xs text-gray-400">{note}</p>}
    </div>
  );
}
