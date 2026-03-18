import Link from "next/link";
import { dashboardPreviewModules } from "@/lib/dashboard-preview-data";

export default function DashboardPreviewHome() {
  return (
    <main className="min-h-screen bg-[#f7f7fc] px-6 py-10 text-slate-900">
      <div className="mx-auto max-w-[1200px] space-y-8">
        <div className="space-y-3">
          <p className="text-sm font-semibold uppercase tracking-[0.25em] text-indigo-600">Phase 2 dashboard previews</p>
          <h1 className="text-4xl font-semibold tracking-tight">Module dashboard gallery</h1>
          <p className="max-w-3xl text-base leading-7 text-slate-500">
            These routes move the project from the shared Phase 1 component library into Phase 2 module-specific dashboards. Each preview reuses the shared components and applies module-specific navigation, metrics, and drilldown copy while the ZIP-data pipeline and live endpoint wiring continue in later phases.
          </p>
        </div>

        <div className="grid gap-6 md:grid-cols-3">
          {dashboardPreviewModules.map((module) => (
            <Link
              key={module.slug}
              href={`/dashboard-preview/${module.slug}`}
              className="rounded-[28px] border border-slate-200 bg-white p-6 shadow-sm transition hover:-translate-y-0.5 hover:shadow-md"
            >
              <p className="text-xs font-semibold uppercase tracking-[0.2em] text-indigo-500">{module.eyebrow}</p>
              <h2 className="mt-3 text-2xl font-semibold tracking-tight text-slate-950">{module.label}</h2>
              <p className="mt-3 text-sm leading-6 text-slate-500">{module.description}</p>
              <div className="mt-6 flex items-center justify-between text-sm text-slate-500">
                <span>{module.zipCount} ZIPs in view</span>
                <span className="font-medium text-indigo-700">Open dashboard →</span>
              </div>
            </Link>
          ))}
        </div>
      </div>
    </main>
  );
}
