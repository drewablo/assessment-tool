"use client";

import { DashboardTabItem } from "@/lib/dashboard";

interface Props {
  tabs: DashboardTabItem[];
  activeKey: string;
  onChange: (key: string) => void;
}

export default function TabbedSubview({ tabs, activeKey, onChange }: Props) {
  return (
    <div className="inline-flex flex-wrap rounded-xl border border-slate-200 bg-white p-1 shadow-sm">
      {tabs.map((tab) => {
        const active = tab.key === activeKey;
        return (
          <button
            key={tab.key}
            type="button"
            onClick={() => onChange(tab.key)}
            className={`rounded-lg px-4 py-2 text-sm font-medium transition-colors ${
              active
                ? "bg-indigo-600 text-white shadow-sm"
                : "text-slate-600 hover:bg-slate-50 hover:text-slate-900"
            }`}
          >
            {tab.label}
          </button>
        );
      })}
    </div>
  );
}
