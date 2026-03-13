import { PopulationGravityMap, DirectionSegment } from "@/lib/types";

interface Props {
  gravity: PopulationGravityMap;
}

const DIRECTIONS = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"] as const;

const BURDEN_COLORS: Record<string, { fill: string; border: string; label: string }> = {
  Growing: { fill: "rgba(239,68,68,0.15)", border: "#ef4444", label: "text-red-700" },
  Stable: { fill: "rgba(234,179,8,0.15)", border: "#eab308", label: "text-yellow-700" },
  Declining: { fill: "rgba(34,197,94,0.15)", border: "#22c55e", label: "text-green-700" },
};

function getSegment(byDirection: Record<string, DirectionSegment>, dir: string): DirectionSegment {
  return byDirection[dir] || { cost_burdened_renters: 0, renter_households: 0, burden_ratio: null, growth_signal: null };
}

export default function HousingGravityPanel({ gravity }: Props) {
  const byDirection = gravity?.by_direction || {};
  const values = DIRECTIONS.map((dir) => {
    const seg = getSegment(byDirection, dir);
    return {
      dir,
      seg,
      burdened: seg.cost_burdened_renters ?? 0,
      renters: seg.renter_households ?? 0,
    };
  });
  const totalBurdened = values.reduce((sum, item) => sum + item.burdened, 0);

  if (!gravity || totalBurdened <= 0) {
    return (
      <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-6">
        <h3 className="text-lg font-semibold text-gray-900">Housing Need Distribution</h3>
        <p className="text-sm text-gray-600 mt-1">
          Cost-burdened renter households by direction from site address
        </p>
        <div className="mt-4 rounded-lg bg-gray-50 p-4 text-sm text-gray-600">
          Note: directional data is not available for county-level analyses. Enter a specific address to enable this feature.
        </div>
      </div>
    );
  }

  const dominant = gravity.dominant_direction ?? values.slice().sort((a, b) => b.burdened - a.burdened)[0]?.dir;
  const ranked = values.slice().sort((a, b) => b.burdened - a.burdened).slice(0, 3);

  const highBurdenDirs = values.filter((v) => v.seg.growth_signal === "Growing").map((v) => v.dir);
  const lowBurdenDirs = values.filter((v) => v.seg.growth_signal === "Declining" && v.burdened > 0).map((v) => v.dir);

  const center = 120;
  const ringBase = 36;
  const ringMax = 42;

  const polar = (radius: number, angleDeg: number) => {
    const rad = ((angleDeg - 90) * Math.PI) / 180;
    return { x: center + radius * Math.cos(rad), y: center + radius * Math.sin(rad) };
  };

  const wedge = (startDeg: number, endDeg: number, innerR: number, outerR: number) => {
    const p1 = polar(outerR, startDeg);
    const p2 = polar(outerR, endDeg);
    const p3 = polar(innerR, endDeg);
    const p4 = polar(innerR, startDeg);
    const largeArc = endDeg - startDeg > 180 ? 1 : 0;
    return `M ${p1.x} ${p1.y} A ${outerR} ${outerR} 0 ${largeArc} 1 ${p2.x} ${p2.y} L ${p3.x} ${p3.y} A ${innerR} ${innerR} 0 ${largeArc} 0 ${p4.x} ${p4.y} Z`;
  };

  return (
    <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-6">
      <h3 className="text-lg font-semibold text-gray-900">Housing Need Distribution</h3>
      <p className="text-sm text-gray-600 mt-1">
        Cost-burdened renter households by direction from site address
      </p>

      <div className="mt-5 flex flex-col lg:flex-row gap-6">
        <svg viewBox="0 0 240 240" className="w-full max-w-[320px] h-auto">
          {values.map(({ dir, seg, burdened }, i) => {
            const share = totalBurdened > 0 ? burdened / totalBurdened : 0;
            const start = i * 45;
            const end = start + 45;
            const outer = ringBase + ringMax * share;
            const fill = dir === dominant ? "#ea580c" : `rgba(234,88,12,${0.2 + share * 0.6})`;
            const burdenColor = seg.growth_signal ? BURDEN_COLORS[seg.growth_signal] : null;
            const strokeColor = burdenColor ? burdenColor.border : "#ffffff";
            const strokeWidth = burdenColor && seg.growth_signal !== "Stable" ? 2 : 1;
            const labelAngle = start + 22.5;
            const labelPos = polar(94, labelAngle);
            return (
              <g key={dir}>
                <path d={wedge(start, end, ringBase, outer)} fill={fill} stroke={strokeColor} strokeWidth={strokeWidth} />
                <text x={labelPos.x} y={labelPos.y} textAnchor="middle" dominantBaseline="middle" className="fill-gray-700 text-[9px] font-semibold">
                  {dir}
                </text>
              </g>
            );
          })}
          <circle cx={center} cy={center} r={28} fill="#f3f4f6" />
          <text x={center} y={center} textAnchor="middle" dominantBaseline="middle" className="fill-gray-700 text-[10px] font-semibold">
            Renters
          </text>
        </svg>

        <div className="flex-1 space-y-2">
          <h4 className="text-sm font-semibold text-gray-800">Top directions</h4>
          <ul className="space-y-1 text-sm text-gray-700">
            {ranked.map(({ dir, seg, burdened, renters }) => {
              const pct = totalBurdened > 0 ? (burdened / totalBurdened) * 100 : 0;
              const bc = seg.growth_signal ? BURDEN_COLORS[seg.growth_signal] : null;
              const burdenPct = renters > 0 ? ((burdened / renters) * 100).toFixed(0) : null;
              return (
                <li key={dir}>
                  {dir} — {burdened.toLocaleString()} cost-burdened households ({pct.toFixed(0)}%)
                  {burdenPct && (
                    <span className="ml-1 text-xs text-orange-600">· {burdenPct}% of renters</span>
                  )}
                  {seg.growth_signal && bc && (
                    <span className={`ml-1 text-xs font-medium ${bc.label}`}>
                      · {seg.growth_signal === "Growing" ? "High burden" : seg.growth_signal === "Stable" ? "Moderate burden" : "Lower burden"}
                    </span>
                  )}
                </li>
              );
            })}
          </ul>

          <div className="mt-4 rounded-lg bg-gray-50 p-4 text-sm text-gray-700">
            The highest concentration of cost-burdened renters is to the <span className="font-semibold">{dominant}</span> of the site.
            {highBurdenDirs.length > 0 && (
              <> The <span className="font-semibold text-red-700">{highBurdenDirs.join(", ")}</span> {highBurdenDirs.length === 1 ? "corridor has" : "corridors have"} a high renter cost-burden rate, indicating concentrated affordable housing need.</>
            )}
            {lowBurdenDirs.length > 0 && (
              <> The <span className="font-semibold text-green-700">{lowBurdenDirs.join(", ")}</span> {lowBurdenDirs.length === 1 ? "corridor shows" : "corridors show"} lower cost-burden rates.</>
            )}
            {" "}Siting and outreach toward the dominant direction may maximize occupancy and community impact.
          </div>

          <div className="flex gap-3 text-xs text-gray-500 mt-2">
            <span className="flex items-center gap-1"><span className="inline-block w-2.5 h-2.5 rounded-full bg-red-500" /> High burden</span>
            <span className="flex items-center gap-1"><span className="inline-block w-2.5 h-2.5 rounded-full bg-yellow-500" /> Moderate</span>
            <span className="flex items-center gap-1"><span className="inline-block w-2.5 h-2.5 rounded-full bg-green-500" /> Lower burden</span>
          </div>

          {!gravity.gravity_weighted && (
            <div className="rounded-lg bg-gray-50 p-3 text-xs text-gray-600">
              Note: directional data is not available for county-level analyses. Enter a specific address to enable this feature.
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
