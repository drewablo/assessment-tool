import base64
import os
from typing import Dict, List, Optional

import anthropic
from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Pydantic schema for structured extraction output
# ---------------------------------------------------------------------------

class _ExtractedYear(BaseModel):
    fiscal_year: Optional[int] = None
    year_label_needs_confirmation: bool = False
    tuition_revenue: Optional[float] = None
    tuition_aid: Optional[float] = None
    other_revenue: Optional[float] = None
    total_expenses: Optional[float] = None
    non_operating_revenue: Optional[float] = None
    total_assets: Optional[float] = None
    enrollment: Optional[float] = None


class _AuditExtractionResult(BaseModel):
    years: List[_ExtractedYear] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Claude-powered extraction
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """\
You are a financial data extraction specialist. You will be given a school audit PDF.
Extract key financial figures for each fiscal year present in the document (up to the 3 most recent years).

Return data in the exact schema requested. All monetary values should be in raw dollars (no formatting).
If a value cannot be found, return null. If a fiscal year label is ambiguous or missing, set
year_label_needs_confirmation to true. Only return years that have actual financial data.
"""

_USER_PROMPT = """\
Extract the following financial data from this school audit document for each fiscal year present
(most recent 3 years only):

- fiscal_year: The 4-digit year (e.g. 2023). Use the end of the fiscal year.
- year_label_needs_confirmation: true if the year is ambiguous or could not be determined
- tuition_revenue: Net tuition and fees revenue
- tuition_aid: Financial aid / tuition assistance / scholarships granted
- other_revenue: Other operating revenue (auxiliary, contributions, program revenue, etc.)
- total_expenses: Total operating expenses
- non_operating_revenue: Non-operating revenue (investment income, endowment draws, transfers, one-time items)
- total_assets: Total assets from the balance sheet
- enrollment: Student enrollment count if mentioned

Return all amounts as plain numbers in US dollars (e.g. 1250000.00, not "$1,250,000").
Negative values (deficits, losses) should be negative numbers.
"""


def extract_audit_financials(file_bytes: bytes, filename: str, audit_index: int) -> Dict:
    """Extract financial data from an audit PDF using Claude's native document understanding."""
    client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

    pdf_b64 = base64.standard_b64encode(file_bytes).decode("utf-8")

    response = client.messages.parse(
        model="claude-haiku-4-5-20251001",
        max_tokens=2048,
        system=_SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "document",
                        "source": {
                            "type": "base64",
                            "media_type": "application/pdf",
                            "data": pdf_b64,
                        },
                    },
                    {"type": "text", "text": _USER_PROMPT},
                ],
            }
        ],
        output_format=_AuditExtractionResult,
    )

    result: _AuditExtractionResult = response.parsed_output

    rows: List[Dict] = []
    for extracted in result.years:
        field_names = ["tuition_revenue", "tuition_aid", "other_revenue",
                       "total_expenses", "non_operating_revenue", "total_assets"]
        missing = [f for f in field_names if getattr(extracted, f) is None]
        rows.append({
            "fiscal_year": extracted.fiscal_year,
            "year_label": str(extracted.fiscal_year) if extracted.fiscal_year else None,
            "year_label_needs_confirmation": extracted.year_label_needs_confirmation,
            "tuition_revenue": extracted.tuition_revenue,
            "tuition_aid": extracted.tuition_aid,
            "other_revenue": extracted.other_revenue,
            "total_expenses": extracted.total_expenses,
            "non_operating_revenue": extracted.non_operating_revenue,
            "total_assets": extracted.total_assets,
            "enrollment": extracted.enrollment,
            "source_file": filename,
            "source_audit_index": audit_index,
            "missing_fields": missing,
        })

    warnings = list(result.warnings)
    if any(r["year_label_needs_confirmation"] for r in rows):
        warnings.append("Fiscal year labels were ambiguous and require user confirmation.")

    return {"rows": rows, "warnings": warnings}


def dedupe_year_rows(rows: List[Dict]) -> List[Dict]:
    by_year: Dict[int, Dict] = {}
    ambiguous: List[Dict] = []
    for row in rows:
        y = row.get("fiscal_year")
        if y is None:
            ambiguous.append(row)
            continue
        prev = by_year.get(y)
        if prev is None or row.get("source_audit_index", -1) >= prev.get("source_audit_index", -1):
            by_year[y] = row
    merged = [by_year[y] for y in sorted(by_year.keys())][-3:]
    return merged + ambiguous[: max(0, 3 - len(merged))]
