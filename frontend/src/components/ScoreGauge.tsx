"use client";

interface Props {
  score: number;
  label: string;
  conservative?: number;
  optimistic?: number;
}

function scoreColor(score: number): string {
  if (score >= 75) return "#16a34a"; // green-600
  if (score >= 55) return "#ca8a04"; // yellow-600
  if (score >= 35) return "#ea580c"; // orange-600
  return "#dc2626"; // red-600
}

function scoreLabel(score: number): string {
  if (score >= 75) return "Strong";
  if (score >= 55) return "Moderate";
  if (score >= 35) return "Challenging";
  return "Difficult";
}

export default function ScoreGauge({ score, label, conservative, optimistic }: Props) {
  const circumference = 251.2; // 2π × 40
  const offset = circumference - (score / 100) * circumference;
  const color = scoreColor(score);

  const hasScenario =
    conservative !== undefined && optimistic !== undefined &&
    (conservative !== score || optimistic !== score);

  const srLabel = `Feasibility score: ${score} out of 100, rated ${scoreLabel(score)}`;

  return (
    <div className="flex flex-col items-center gap-2">
      <div
        className="relative w-36 h-36"
        role="img"
        aria-label={srLabel}
      >
        <svg
          className="w-full h-full -rotate-90"
          viewBox="0 0 100 100"
          aria-hidden="true"
          focusable="false"
        >
          {/* Track */}
          <circle cx="50" cy="50" r="40" fill="none" stroke="#e5e7eb" strokeWidth="10" />
          {/* Score arc */}
          <circle
            cx="50"
            cy="50"
            r="40"
            fill="none"
            stroke={color}
            strokeWidth="10"
            strokeDasharray={circumference}
            strokeDashoffset={offset}
            strokeLinecap="round"
            style={{ transition: "stroke-dashoffset 0.8s ease" }}
          />
        </svg>
        <div className="absolute inset-0 flex flex-col items-center justify-center" aria-hidden="true">
          <span className="text-3xl font-bold" style={{ color }}>
            {score}
          </span>
          <span className="text-xs text-gray-500 font-medium">/ 100</span>
        </div>
      </div>
      <div className="text-center">
        <div className="font-bold text-lg" style={{ color }}>
          {scoreLabel(score)}
        </div>
        <div className="text-sm text-gray-500">{label}</div>
        {hasScenario && (
          <div className="mt-1 text-xs text-gray-400">
            Range: {conservative}–{optimistic}
          </div>
        )}
      </div>
    </div>
  );
}
