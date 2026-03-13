"""
PDF report generation using ReportLab.
Produces a styled multi-page PDF for any ministry type analysis result.
"""

from __future__ import annotations

import io
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    HRFlowable,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

if TYPE_CHECKING:
    from models.schemas import AnalysisRequest, AnalysisResponse

# Brand colours
NAVY = colors.HexColor("#1e3a5f")
GREEN = colors.HexColor("#16a34a")
AMBER = colors.HexColor("#ca8a04")
ORANGE = colors.HexColor("#ea580c")
RED = colors.HexColor("#dc2626")
LIGHT_GRAY = colors.HexColor("#f3f4f6")
MID_GRAY = colors.HexColor("#6b7280")


def _score_color(score: int) -> colors.Color:
    if score >= 75:
        return GREEN
    if score >= 55:
        return AMBER
    if score >= 35:
        return ORANGE
    return RED


def _score_label(score: int) -> str:
    if score >= 75:
        return "Strong"
    if score >= 55:
        return "Moderate"
    if score >= 35:
        return "Challenging"
    return "Difficult"


def _h1(text: str) -> Paragraph:
    style = ParagraphStyle("H1", fontSize=20, leading=24, textColor=NAVY, spaceAfter=6, fontName="Helvetica-Bold")
    return Paragraph(text, style)


def _h2(text: str) -> Paragraph:
    style = ParagraphStyle("H2", fontSize=13, leading=16, textColor=NAVY, spaceBefore=12, spaceAfter=4, fontName="Helvetica-Bold")
    return Paragraph(text, style)


def _body(text: str) -> Paragraph:
    style = ParagraphStyle("Body", fontSize=9, leading=13, textColor=colors.black)
    return Paragraph(text, style)


def _caption(text: str) -> Paragraph:
    style = ParagraphStyle("Caption", fontSize=8, leading=10, textColor=MID_GRAY)
    return Paragraph(text, style)


def _score_table(score_obj) -> Table:
    """Render the 4-metric score breakdown as a table."""
    metrics = [
        ("Market Size", score_obj.market_size),
        ("Income Level", score_obj.income),
        ("Competition", score_obj.competition),
        ("Family / Pop. Density", score_obj.family_density),
    ]

    header = ["Metric", "Score", "Rating"]
    rows = [header]
    for label, m in metrics:
        if m is None:
            continue
        rows.append([label, str(m.score), m.rating])

    t = Table(rows, colWidths=[3 * inch, 1 * inch, 1.5 * inch])
    style = TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), NAVY),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [LIGHT_GRAY, colors.white]),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#e5e7eb")),
        ("ALIGN", (1, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ])
    t.setStyle(style)
    return t


def _competitor_table(schools: list, ministry_type: str) -> Table:
    if not schools:
        return Table([["No competitors found in catchment."]])

    header = ["Name", "Distance", "Type/Grade", "Enrollment/Beds"]
    rows = [header]
    for s in schools[:20]:  # cap at 20 rows
        dist = f"{s.distance_miles} mi" if hasattr(s, "distance_miles") else "—"
        grade = getattr(s, "grade_level", None) or getattr(s, "gender", "—")
        enr = str(int(s.enrollment)) if getattr(s, "enrollment", None) else "—"
        rows.append([s.name[:40], dist, grade, enr])

    col_w = [3.2 * inch, 0.9 * inch, 1.2 * inch, 1.1 * inch]
    t = Table(rows, colWidths=col_w)
    style = TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), NAVY),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [LIGHT_GRAY, colors.white]),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#e5e7eb")),
        ("ALIGN", (1, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ])
    t.setStyle(style)
    return t


def _bullet(text: str) -> Paragraph:
    style = ParagraphStyle("Bullet", fontSize=9, leading=13, leftIndent=12, firstLineIndent=-12, textColor=colors.black)
    return Paragraph(f"• {text}", style)


def _methodology_appendix(ministry_type: str) -> list:
    """Return a page-break + plain-English methodology section for any ministry type."""
    is_schools = ministry_type == "schools"
    is_housing = ministry_type == "housing"
    is_elder = ministry_type == "elder_care"

    label = {
        "schools": "Catholic School",
        "housing": "Affordable Housing",
        "elder_care": "Elder Care",
    }.get(ministry_type, "Ministry")

    story: list = [PageBreak()]
    story.append(_h1("Methodology Appendix"))
    story.append(_body(f"This appendix explains how the {label} Feasibility Score is calculated "
                       "and what each component means in plain English."))
    story.append(Spacer(1, 8))
    story.append(HRFlowable(width="100%", thickness=0.5, color=NAVY, spaceAfter=8))

    # --- What the score is ---
    story.append(_h2("What the 0–100 Score Means"))
    score_rows = [
        ["Score Range", "Rating", "Plain-English Meaning"],
        ["75–100", "Strong", "Market conditions are favorable. Warrants serious further investigation."],
        ["55–74", "Moderate", "Mixed signals. Some factors support viability; others raise caution."],
        ["35–54", "Challenging", "Material headwinds. Significant concerns require close examination."],
        ["0–34",  "Difficult",  "Market conditions are unfavorable. Continuation is high risk."],
    ]
    t = Table(score_rows, colWidths=[1.2 * inch, 1.0 * inch, 4.3 * inch])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), NAVY),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [LIGHT_GRAY, colors.white]),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#e5e7eb")),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]))
    story.append(t)
    story.append(Spacer(1, 8))

    # --- Scenario range ---
    story.append(_h2("Scenario Range (Conservative / Optimistic)"))
    story.append(_body(
        "The score is shown alongside a conservative and optimistic scenario. The width of this range "
        "reflects data confidence: a narrow range (±6 points) means the underlying Census data is "
        "robust; a wide range (±18 points) means the catchment area has a thin population base and "
        "the score should be treated with extra caution."
    ))
    story.append(Spacer(1, 8))

    # --- Four factors ---
    story.append(_h2("The Four Scoring Factors"))
    if is_schools:
        factors = [
            ("Market Size (35%)",
             "Estimates the number of Catholic school-age children in the area. A larger addressable "
             "student population improves this score. Calibrated for urban, suburban, and rural norms."),
            ("Income Level (25%)",
             "Measures median household income and the share of high-income households. Higher incomes "
             "suggest stronger tuition-paying capacity. States with school choice programs (vouchers or "
             "ESAs) receive a bonus because families can apply public dollars toward tuition."),
            ("Competition (25%)",
             "Counts nearby Catholic and private schools weighted by distance. A lower competitive "
             "density generally means less enrollment fragmentation and more sustainable conditions. "
             "Note: some competition signals proven demand — this factor balances both."),
            ("Family Density (15%)",
             "Measures the share of households with children under 18. A family-dense neighborhood is "
             "more likely to generate enrollment demand for Catholic education."),
        ]
    elif is_housing:
        factors = [
            ("Cost-Burdened Households (35%)",
             "The number of renter households spending more than 30% of income on housing — the "
             "federal threshold for being 'cost burdened.' Higher counts signal greater unmet need."),
            ("Income Need (25%)",
             "Scored inversely: lower area incomes indicate higher need and produce a higher score, "
             "reflecting the mission focus on underserved communities."),
            ("LIHTC Saturation (25%)",
             "The ratio of existing Low-Income Housing Tax Credit units to cost-burdened households. "
             "Lower saturation means the affordable supply is insufficient relative to need."),
            ("Renter Burden Intensity (15%)",
             "The share of all renters who are cost burdened. Higher intensity indicates community-wide "
             "housing stress, not just pockets of need."),
        ]
    else:  # elder_care
        factors = [
            ("Target Population (35%)",
             "In mission mode: vulnerable seniors (those living alone or below 200% of poverty). "
             "In market mode: adults age 75+, the group with the highest care utilization."),
            ("Income Fit (25%)",
             "In market mode: higher incomes signal self-pay capacity. In mission mode: lower incomes "
             "signal greater need for subsidized or affordable care — the score is reversed accordingly."),
            ("Bed Saturation (25%)",
             "The ratio of competitor beds to the senior population. Low saturation means the market "
             "is underserved. Higher score = more unmet demand."),
            ("Market Occupancy (15%)",
             "Weighted average occupancy across nearby facilities. High occupancy suggests proven, "
             "sustained demand. Low occupancy may indicate market softness or oversupply."),
        ]

    for title, desc in factors:
        story.append(_body(f"<b>{title}</b>"))
        story.append(_body(desc))
        story.append(Spacer(1, 5))

    story.append(Spacer(1, 4))

    # --- Three stages ---
    story.append(_h2("This Report is Stage 1 of Three"))
    story.append(_body(
        "A score above 60 is not a green light — it is a reason to proceed to the next stage. "
        "Three stages of analysis are required before any strategic commitment:"
    ))
    story.append(Spacer(1, 4))
    story.append(_bullet(
        "Stage 1 — Market Feasibility (this report): Census demographics, competitor landscape, "
        "income levels, and family or senior density. Answers: 'Is there a market here?'"
    ))
    story.append(Spacer(1, 3))
    story.append(_bullet(
        "Stage 2 — Institutional Economics: Enrollment sustainability, financial audit review, "
        "operating margin, subsidy dependency, and payer mix. Answers: 'Can this institution sustain itself?'"
    ))
    story.append(Spacer(1, 3))
    story.append(_bullet(
        "Stage 3 — Local Validation: Community listening sessions, sponsor engagement, direct "
        "demand surveys, and diocesan or regulatory alignment. Answers: 'Does the community confirm this?'"
    ))
    story.append(Spacer(1, 10))

    # --- Data sources ---
    story.append(_h2("Data Sources"))
    source_rows = [["Source", "What It Provides", "Typical Refresh"]]
    if is_schools:
        source_rows += [
            ["US Census ACS 5-year", "Population, income, household structure", "Annual (5-yr rolling avg)"],
            ["NCES Private School Survey", "School locations, enrollment, affiliation", "Every 2 years"],
            ["CARA / Georgetown", "State-level Catholic affiliation estimates", "Periodic"],
        ]
    elif is_housing:
        source_rows += [
            ["US Census ACS 5-year", "Renter households, income, cost burden", "Annual (5-yr rolling avg)"],
            ["HUD LIHTC Database", "Affordable housing unit inventory", "Annual"],
        ]
    else:
        source_rows += [
            ["US Census ACS 5-year", "Senior population, income, isolation", "Annual (5-yr rolling avg)"],
            ["CMS Care Compare", "Facility beds, occupancy, quality ratings", "Quarterly"],
        ]
    t2 = Table(source_rows, colWidths=[2.0 * inch, 3.0 * inch, 1.5 * inch])
    t2.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), NAVY),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [LIGHT_GRAY, colors.white]),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#e5e7eb")),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(t2)
    story.append(Spacer(1, 6))
    story.append(_caption(
        "Census ACS 5-year estimates aggregate five years of survey data, which smooths year-to-year "
        "volatility but means results reflect conditions from 2–5 years prior to publication. "
        "See the Data Freshness section for source ages specific to this report."
    ))
    return story


def generate_pdf_report(result: "AnalysisResponse", request: "AnalysisRequest") -> bytes:
    """Return a PDF as bytes for the given analysis result."""
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=letter,
        leftMargin=0.75 * inch,
        rightMargin=0.75 * inch,
        topMargin=0.75 * inch,
        bottomMargin=0.75 * inch,
    )

    ministry_label = {
        "schools": "Catholic School",
        "housing": "Affordable Housing",
        "elder_care": "Elder Care",
    }.get(result.ministry_type or "schools", "Ministry")

    score = result.feasibility_score.overall
    color = _score_color(score)
    label = _score_label(score)
    generated = datetime.now(timezone.utc).strftime("%B %d, %Y")

    story = []

    # ---- Header ----
    story.append(_h1(f"{ministry_label} Feasibility Report"))
    story.append(Spacer(1, 4))
    story.append(_body(f"<b>Project:</b> {result.school_name or request.school_name}"))
    story.append(_body(f"<b>Address:</b> {request.address}"))
    story.append(_body(f"<b>Generated:</b> {generated}"))
    story.append(HRFlowable(width="100%", thickness=1, color=NAVY, spaceAfter=8))

    # ---- Decision-support disclaimer (prominent, page 1) ----
    disclaimer_style = ParagraphStyle(
        "Disclaimer",
        fontSize=9,
        leading=13,
        textColor=colors.HexColor("#92400e"),
        backColor=colors.HexColor("#fffbeb"),
        borderColor=colors.HexColor("#fcd34d"),
        borderWidth=1,
        borderPadding=8,
        borderRadius=4,
        spaceAfter=10,
    )
    story.append(Paragraph(
        "<b>Decision-Support Tool — Stage 1 Market Screen Only.</b> "
        "This report is a directional signal based on US Census ACS demographic estimates and "
        "ministry-specific competitor databases. Scores are statistical estimates, not forecasts. "
        "This analysis does <b>not</b> constitute a recommendation to open, close, or transform a "
        "ministry. Strategic commitments require Stage 2 institutional economics review and "
        "Stage 3 local community validation alongside pastoral discernment and professional judgment.",
        disclaimer_style,
    ))
    story.append(Spacer(1, 4))

    # ---- Overall score ----
    score_style = ParagraphStyle(
        "ScoreHead",
        fontSize=36,
        leading=40,
        textColor=color,
        alignment=TA_CENTER,
        fontName="Helvetica-Bold",
    )
    story.append(Paragraph(str(score), score_style))
    story.append(Paragraph(
        f'<font color="#{color.hexval()[2:]}"><b>{label} Feasibility</b></font>',
        ParagraphStyle("ScoreLabel", fontSize=14, alignment=TA_CENTER, leading=18),
    ))
    if result.feasibility_score.scenario_conservative is not None:
        story.append(_caption(
            f"Scenario range: {result.feasibility_score.scenario_conservative}–{result.feasibility_score.scenario_optimistic}"
        ))
    story.append(Spacer(1, 10))

    # ---- Recommendation ----
    story.append(_h2("Recommendation"))
    story.append(_body(result.recommendation or "No recommendation generated."))
    if result.recommendation_detail:
        story.append(Spacer(1, 4))
        story.append(_body(result.recommendation_detail))
    story.append(Spacer(1, 8))

    # ---- Score breakdown ----
    story.append(_h2("Score Breakdown"))
    story.append(_score_table(result.feasibility_score))
    story.append(Spacer(1, 10))

    # ---- Demographics ----
    demo = result.demographics
    if demo:
        story.append(_h2("Catchment Demographics"))
        pct_hh_with_children = (
            demo.families_with_children / demo.total_households
            if demo.families_with_children and demo.total_households
            else None
        )
        demo_rows = [
            ["Metric", "Value"],
            ["Total Population", f"{demo.total_population:,}" if demo.total_population else "—"],
            ["Median Household Income", f"${demo.median_household_income:,.0f}" if demo.median_household_income else "—"],
            ["School-Age Children (5–17)", f"{demo.school_age_population:,}" if demo.school_age_population else "—"],
            ["Households w/ Children", f"{pct_hh_with_children:.1%}" if pct_hh_with_children else "—"],
            ["Seniors 65+", f"{demo.seniors_65_plus:,}" if demo.seniors_65_plus else "—"],
        ]
        t = Table(demo_rows, colWidths=[3.5 * inch, 3 * inch])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), NAVY),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [LIGHT_GRAY, colors.white]),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#e5e7eb")),
            ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("TOPPADDING", (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ]))
        story.append(t)
        story.append(Spacer(1, 10))

    # ---- Competitors ----
    if result.competitor_schools:
        story.append(_h2("Competitor Landscape"))
        story.append(_caption(
            f"{len(result.competitor_schools)} competitors found within catchment area."
        ))
        story.append(Spacer(1, 4))
        story.append(_competitor_table(result.competitor_schools, result.ministry_type or "schools"))
        story.append(Spacer(1, 8))


    if result.benchmark_narrative:
        story.append(_h2("Regional Benchmark Narrative"))
        story.append(_body(result.benchmark_narrative.narrative_summary))
        for item in result.benchmark_narrative.nearest_comparable_markets[:3]:
            story.append(_caption(f"Comparable market: {item}"))
        story.append(Spacer(1, 8))

    if result.board_report_pack:
        pack = result.board_report_pack
        story.append(_h2("Board-Ready Report Pack"))
        story.append(_body(f"<b>Executive summary:</b> {pack.executive_summary}"))
        for action in pack.immediate_next_actions[:3]:
            story.append(_body(f"• {action}"))
        story.append(_caption("12/24/36 month roadmap included in API response for governance planning."))
        story.append(Spacer(1, 8))

    if result.data_freshness:
        story.append(_h2("Data Freshness"))
        story.append(_caption(f"Generated UTC: {result.data_freshness.generated_at_utc} · Mode: {result.data_freshness.mode}"))
        for src in result.data_freshness.sources[:4]:
            age = f"{src.freshness_hours}h" if src.freshness_hours is not None else "n/a"
            story.append(_caption(f"{src.source_label}: {src.status} (age {age})"))
        story.append(Spacer(1, 6))
    # ---- Footer note ----
    story.append(HRFlowable(width="100%", thickness=0.5, color=MID_GRAY, spaceBefore=8))
    story.append(_caption(
        "Ministry Feasibility Tool · Data sources: US Census ACS 5-year estimates, NCES, HUD LIHTC, CMS Care Compare. "
        "See page 1 disclaimer for full scope limitations."
    ))

    # ---- Methodology appendix ----
    story.extend(_methodology_appendix(result.ministry_type or "schools"))

    doc.build(story)
    return buf.getvalue()
