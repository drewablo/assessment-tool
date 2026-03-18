"use client";

import { DashboardSidebarItem } from "@/lib/dashboard";

interface Props {
  items: DashboardSidebarItem[];
  activeKey: string;
  onSelect: (key: string) => void;
}

export default function DashboardSidebar({ items, activeKey, onSelect }: Props) {
  return (
    <aside className="space-y-3">
      {items.map((item) => {
        const active = item.key === activeKey;
        return (
          <button
            key={item.key}
            type="button"
            onClick={() => onSelect(item.key)}
            className={`w-full rounded-2xl border px-4 py-4 text-left transition-all ${
              active
                ? "border-indigo-200 bg-indigo-50 shadow-sm"
                : "border-transparent bg-transparent hover:border-slate-200 hover:bg-white"
            }`}
          >
            <div className="flex items-start justify-between gap-3">
              <div>
                <p className={`text-sm font-semibold ${active ? "text-slate-950" : "text-slate-800"}`}>
                  {item.title}
                </p>
                <p className="mt-1 text-sm leading-5 text-slate-500">{item.description}</p>
              </div>
              {item.badge ? (
                <span className="rounded-full bg-indigo-100 px-2 py-0.5 text-xs font-semibold text-indigo-700">
                  {item.badge}
                </span>
              ) : null}
            </div>
          </button>
        );
      })}
    </aside>
  );
}
