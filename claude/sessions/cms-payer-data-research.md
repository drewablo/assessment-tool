# CMS Payer Data Sources & ACS B19037 Research

*Research date: 2026-03-19*

---

## 1. CMS County-Level Medicare Enrollment Datasets

### a) Medicare Monthly Enrollment (Best Overall Source)
- URL: https://data.cms.gov/summary-statistics-on-beneficiary-enrollment/medicare-and-medicaid-reports/medicare-monthly-enrollment
- Provides monthly beneficiary counts at national, state, and county level
- Breaks down by: Original Medicare vs. Medicare Advantage, Part D (standalone PDP vs. MA-PD)
- Format: CSV download and API (JSON or CSV via `data-api/v1/dataset/{id}/data.csv`)
- Update frequency: Monthly
- Filter county-level rows via the API: `?filter[BENE_GEO_LVL]=County`

### b) Monthly Enrollment by Contract/Plan/State/County (MA/Part D)
- URL: https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/monthly-enrollment-contract/plan/state/county
- Granular: enrollment by individual MA/PDP contract and plan, at the state/county level
- Format: CSV (downloadable zip files)
- Update frequency: Monthly (published by the 15th of each month)

### c) Monthly MA Enrollment by State/County/Plan Type
- URL: https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/monthly-ma-enrollment-state/county/plan-type
- Aggregated MA enrollment by county and plan type (HMO, PPO, PFFS, etc.)
- Format: CSV
- Update frequency: Monthly

### d) Medicare Geographic Variation -- by National, State & County
- URL: https://data.cms.gov/summary-statistics-on-use-and-payments/medicare-geographic-comparisons/medicare-geographic-variation-by-national-state-county
- Contains demographic, spending, utilization, and quality indicators at the county level for the Original Medicare (FFS) population
- Data dictionary: https://data.cms.gov/resources/medicare-geographic-variation-by-national-state-county-data-dictionary
- Format: CSV download and API on data.cms.gov
- Update frequency: Annual
- Source: CMS Chronic Conditions Data Warehouse (CCW), 100% FFS claims

### e) CMS Program Statistics -- Medicare Total Enrollment
- URL: https://data.cms.gov/summary-statistics-on-beneficiary-enrollment/medicare-and-medicaid-reports/cms-program-statistics-medicare-total-enrollment
- Area-of-residence tables: Total, Original Medicare, and MA enrollment
- Format: CSV/API on data.cms.gov
- Update frequency: Annual

---

## 2. County-Level Medicare Advantage vs. Original Medicare -- Best Dataset

The single best source for county-level MA vs. Original Medicare breakdowns is:

**Medicare Monthly Enrollment** on data.cms.gov
- URL: https://data.cms.gov/summary-statistics-on-beneficiary-enrollment/medicare-and-medicaid-reports/medicare-monthly-enrollment
- Data.gov catalog: https://catalog.data.gov/dataset/medicare-monthly-enrollment
- Fields include: Total Medicare, Original Medicare, MA/Other Health Plans, Part D PDP, Part D MA-PD -- all at the county level
- API pattern:
  ```
  https://data.cms.gov/data-api/v1/dataset/{dataset-uuid}/data?filter[BENE_GEO_LVL]=County
  ```
  (The exact UUID is visible on the dataset page; append `.csv` for CSV output.)

For plan-level detail, use the **Monthly Enrollment by Contract/Plan/State/County** files from cms.gov (CSV downloads).

---

## 3. County-Level Medicaid Enrollment

County-level Medicaid enrollment is **not** published as a simple public download.

- **CMS T-MSIS** is the authoritative source for county-level Medicaid enrollment. However, T-MSIS microdata requires a data use agreement through [ResDAC](https://resdac.org/cms-data). Not freely downloadable.
- **Medicaid.gov Enrollment Reports** and **KFF Medicaid Enrollment Tracker** provide **state-level only**, not county.
- **Census SAHIE (Small Area Health Insurance Estimates)** is the closest public proxy for county-level Medicaid coverage:
  - URL: https://www.census.gov/programs-surveys/sahie.html
  - Interactive tool: https://www.census.gov/data-tools/demo/sahie/
  - Uses MSIS/T-MSIS Medicaid enrollment as a model input; produces county-level estimates of insured/uninsured populations (not raw Medicaid counts)
  - Format: CSV download; also available via Census API
  - Update frequency: Annual

**Bottom line for Medicaid at county level:** No freely downloadable CMS dataset with raw Medicaid enrollment by county. Practical options: (a) SAHIE estimates as a proxy, (b) state-specific Medicaid agency data (varies by state), or (c) a ResDAC data use agreement for T-MSIS.

---

## 4. Formats Summary

| Dataset | Format | API? |
|---|---|---|
| Medicare Monthly Enrollment (data.cms.gov) | CSV, JSON | Yes (data.cms.gov API) |
| MA/Part D Monthly by Contract/Plan/State/County (cms.gov) | CSV (zip) | No (file download) |
| Medicare Geographic Variation (data.cms.gov) | CSV, JSON | Yes (data.cms.gov API) |
| CMS Program Statistics (data.cms.gov) | CSV, JSON | Yes |
| SAHIE (Census) | CSV | Yes (Census API) |

---

## 5. Update Frequencies

- **Medicare Monthly Enrollment**: Monthly
- **MA/Part D Contract/Plan/State/County**: Monthly (by the 15th)
- **Medicare Geographic Variation**: Annual
- **CMS Program Statistics**: Annual
- **SAHIE**: Annual (typically ~18 month lag)

---

## 6. ACS Table B19037: Age of Householder by Household Income

**Full title:** "Age of Householder by Household Income in the Past 12 Months (in inflation-adjusted dollars)"
**Universe:** Households

**Census API variable group:** `B19037`

API URL to see all variables:
```
https://api.census.gov/data/2022/acs/acs5/groups/B19037.html
```

Example API call for county-level data:
```
https://api.census.gov/data/2022/acs/acs5?get=NAME,group(B19037)&for=county:*&in=state:*&key=YOUR_KEY
```

**Structure -- 4 age groups x 16 income brackets:**

| Age Group | Subtotal Variable |
|---|---|
| All (Total) | B19037_001E |
| Under 25 years | B19037_002E |
| 25 to 44 years | B19037_019E |
| 45 to 64 years | B19037_036E |
| 65 years and over | B19037_053E |

Within each age group, the 16 income brackets (offset from the subtotal) are:
- Less than $10,000
- $10,000--$14,999
- $15,000--$19,999
- $20,000--$24,999
- $25,000--$29,999
- $30,000--$34,999
- $35,000--$39,999
- $40,000--$44,999
- $45,000--$49,999
- $50,000--$59,999
- $60,000--$74,999
- $75,000--$99,999
- $100,000--$124,999
- $125,000--$149,999
- $150,000--$199,999
- $200,000 or more

Each estimate variable ends in `E`; corresponding margins of error end in `M`. Available in both ACS 1-Year (places 65k+ population) and ACS 5-Year (all geographies down to block group).

Race/ethnicity iterations: B19037A (White alone), B19037B (Black), B19037C-I (other groups).

---

## Sources

- [Medicare Monthly Enrollment - data.cms.gov](https://data.cms.gov/summary-statistics-on-beneficiary-enrollment/medicare-and-medicaid-reports/medicare-monthly-enrollment)
- [Monthly Enrollment by Contract/Plan/State/County - CMS](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/monthly-enrollment-contract/plan/state/county)
- [Monthly MA Enrollment by State/County/Plan Type - CMS](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/monthly-ma-enrollment-state/county/plan-type)
- [Medicare Geographic Variation - data.cms.gov](https://data.cms.gov/summary-statistics-on-use-and-payments/medicare-geographic-comparisons/medicare-geographic-variation-by-national-state-county)
- [Medicare Geographic Variation Data Dictionary](https://data.cms.gov/resources/medicare-geographic-variation-by-national-state-county-data-dictionary)
- [CMS Program Statistics - Medicare Total Enrollment](https://data.cms.gov/summary-statistics-on-beneficiary-enrollment/medicare-and-medicaid-reports/cms-program-statistics-medicare-total-enrollment)
- [Medicare Enrollment Dashboard](https://data.cms.gov/tools/medicare-enrollment-dashboard)
- [Medicare Monthly Enrollment - Data.gov Catalog](https://catalog.data.gov/dataset/medicare-monthly-enrollment)
- [KFF Medicaid Enrollment and Unwinding Tracker](https://www.kff.org/medicaid/medicaid-enrollment-and-unwinding-tracker/)
- [Medicaid Enrollment Report - Medicaid.gov](https://www.medicaid.gov/medicaid/managed-care/enrollment-report)
- [Census SAHIE Program](https://www.census.gov/programs-surveys/sahie.html)
- [SAHIE Interactive Data Tool](https://www.census.gov/data-tools/demo/sahie/)
- [SAHIE Medicaid Data Inputs](https://www.census.gov/programs-surveys/sahie/technical-documentation/model-input-data/medicaid.html)
- [ResDAC - CMS Data](https://resdac.org/cms-data)
- [Table B19037 - Census Reporter](https://censusreporter.org/tables/B19037/)
- [B19037 on data.census.gov](https://data.census.gov/table/ACSDT1Y2024.B19037)
- [Census API B19037 Variable Group](https://api.census.gov/data/2022/acs/acs5/groups/B19037.html)
- [Data.CMS.gov Developer Portal](https://developer.cms.gov/data-cms/)
