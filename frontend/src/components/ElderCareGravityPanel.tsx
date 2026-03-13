import { PopulationGravityMap, DirectionSegment } from "@/lib/types";

interface Props {
  gravity: PopulationGravityMap;
}

const DIRECTIONS = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"] as const;

const NEED_COLORS: Record<string, { fill: string; border: string; label: string }> = {
  Growing: { fill: "rgba(239,68,68,0.15)", border: "#ef4444", label: "text-red-700" },
  Stable: { fill: "rgba(234,179,8,0.15)", border: "#eab308", label: "text-yellow-700" },
  Declining: { fill: "rgba(34,197,94,0.15)", border: "#22c55e", label: "text-green-700" },
};

function getSegment(byDirection: Record<string, DirectionSegment>, dir: string): DirectionSegment {
  return byDirection[dir] || { seniors_65_plus: 0, seniors_75_plus: 0, seniors_living_alone: 0, isolation_ratio: null, growth_signal: null };
}

export default function ElderCareGravityPanel({ gravity }: Props) {
  const byDirection = gravity?.by_direction || {};
  const values = DIRECTIONS.map((dir) => {
    const seg = getSegment(byDirection, dir);
    return {
      dir,
      seg,
      pop75: seg.seniors_75_plus ?? 0,
      pop65: seg.seniors_65_plus ?? 0,
      alone: seg.seniors_living_alone ?? 0,
    };
  });
  const total75 = values.reduce((sum, item) => sum + item.pop75, 0);

  if (!gravity || total75 <= 0) {
    return (
      <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-6">
        <h3 className="text-lg font-semibold text-gray-900">Senior Population Distribution</h3>
        <p className="text-sm text-gray-600 mt-1">
          Senior population concentration by direction from facility address
        </p>
        <div className="mt-4 rounded-lg bg-gray-50 p-4 text-sm text-gray-600">
          Note: directional data is not available for county-level analyses. Enter a specific address to enable this feature.
        </div>
      </div>
    );
  }

  const dominant = gravity.dominant_direction ?? values.slice().sort((a, b) => b.pop75 - a.pop75)[0]?.dir;
  const ranked = values.slice().sort((a, b) => b.pop75 - a.pop75).slice(0, 3);

  const highNeedDirs = values.filter((v) => v.seg.growth_signal === "Growing").map((v) => v.dir);
  const lowNeedDirs = values.filter((v) => v.seg.growth_signal === "Declining" && v.pop75 > 0).map((v) => v.dir);

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
      <h3 className="text-lg font-semibold text-gray-900">Senior Population Distribution</h3>
      <p className="text-sm text-gray-600 mt-1">
        Population age 75+ by direction from facility address
      </p>

      <div className="mt-5 flex flex-col lg:flex-row gap-6">
        <svg viewBox="0 0 240 240" className="w-full max-w-[320px] h-auto">
          {values.map(({ dir, seg, pop75 }, i) => {
            const share = total75 > 0 ? pop75 / total75 : 0;
            const start = i * 45;
            const end = start + 45;
            const outer = ringBase + ringMax * share;
            const fill = dir === dominant ? "#7c3aed" : `rgba(124,58,237,${0.2 + share * 0.6})`;
            const needColor = seg.growth_signal ? NEED_COLORS[seg.growth_signal] : null;
            const strokeColor = needColor ? needColor.border : "#ffffff";
            const strokeWidth = needColor && seg.growth_signal !== "Stable" ? 2 : 1;
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
            Seniors
          </text>
        </svg>

        <div className="flex-1 space-y-2">
          <h4 className="text-sm font-semibold text-gray-800">Top directions</h4>
          <ul className="space-y-1 text-sm text-gray-700">
            {ranked.map(({ dir, seg, pop75, alone }) => {
              const pct = total75 > 0 ? (pop75 / total75) * 100 : 0;
              const nc = seg.growth_signal ? NEED_COLORS[seg.growth_signal] : null;
              return (
                <li key={dir}>
                  {dir} — {pop75.toLocaleString()} seniors age 75+ ({pct.toFixed(0)}%)
                  {alone > 0 && (
                    <span className="ml-1 text-xs text-purple-600">· {alone.toLocaleString()} living alone</span>
                  )}
                  {seg.growth_signal && nc && (
                    <span className={`ml-1 text-xs font-medium ${nc.label}`}>
                      · {seg.growth_signal === "Growing" ? "High isolation" : seg.growth_signal === "Stable" ? "Moderate isolation" : "Low isolation"}
                    </span>
                  )}
                </li>
              );
            })}
          </ul>

          <div className="mt-4 rounded-lg bg-gray-50 p-4 text-sm text-gray-700">
            The highest concentration of seniors age 75+ is to the <span className="font-semibold">{dominant}</span> of the facility.
            {highNeedDirs.length > 0 && (
              <> The <span className="font-semibold text-red-700">{highNeedDirs.join(", ")}</span> {highNeedDirs.length === 1 ? "corridor has" : "corridors have"} high senior isolation rates, indicating greater care needs.</>
            )}
            {lowNeedDirs.length > 0 && (
              <> The <span className="font-semibold text-green-700">{lowNeedDirs.join(", ")}</span> {lowNeedDirs.length === 1 ? "corridor shows" : "corridors show"} lower isolation rates.</>
            )}
            {" "}Outreach and community partnerships in the dominant direction may yield the highest service utilization.
          </div>

          <div className="flex gap-3 text-xs text-gray-500 mt-2">
            <span className="flex items-center gap-1"><span className="inline-block w-2.5 h-2.5 rounded-full bg-red-500" /> High isolation</span>
            <span className="flex items-center gap-1"><span className="inline-block w-2.5 h-2.5 rounded-full bg-yellow-500" /> Moderate</span>
            <span className="flex items-center gap-1"><span className="inline-block w-2.5 h-2.5 rounded-full bg-green-500" /> Low isolation</span>
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
