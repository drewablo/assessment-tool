import { notFound } from "next/navigation";
import ModuleDashboardView from "@/components/dashboard/modules/ModuleDashboardView";
import { getDashboardPreviewModule } from "@/lib/dashboard-preview-data";

interface Props {
  params: {
    module: string;
  };
}

export default function DashboardModulePage({ params }: Props) {
  const config = getDashboardPreviewModule(params.module);
  if (!config) {
    notFound();
  }

  return <ModuleDashboardView config={config} />;
}
