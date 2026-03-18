"use client";

import { Download, FileSpreadsheet } from "lucide-react";

interface Props {
  onDownloadPng?: () => void | Promise<void>;
  onDownloadCsv?: () => void;
}

export default function ChartActionBar({ onDownloadPng, onDownloadCsv }: Props) {
  return (
    <div className="flex flex-wrap items-center gap-4 text-sm text-slate-500">
      <button
        type="button"
        onClick={() => void onDownloadPng?.()}
        className="inline-flex items-center gap-1.5 hover:text-slate-700 transition-colors"
      >
        <Download className="w-4 h-4" />
        Download PNG
      </button>
      <button
        type="button"
        onClick={onDownloadCsv}
        className="inline-flex items-center gap-1.5 hover:text-slate-700 transition-colors"
      >
        <FileSpreadsheet className="w-4 h-4" />
        Download results as CSV
      </button>
    </div>
  );
}
