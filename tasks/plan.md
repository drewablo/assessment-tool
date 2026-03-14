# HUD 202 Senior Housing Filter & Scoring Fix — Implementation Plan

## Problem Statement

Two issues with the senior housing module when `housing_target_population = "senior_only"`:

### Issue 1: HUD 202 Properties Not Showing on Map/Competitors List
**Root Cause:** In `backend/modules/housing.py:391`, Section 202 properties are only fetched when `USE_DB=true`:
```python
if housing_target_population == "senior_only" and USE_DB:
```
When `USE_DB=false` (e.g. local dev, CSV-fallback mode), Section 202 properties are **never** fetched, so the map and competitors list show zero HUD 202 entries. There is no CSV fallback for Section 202 data (unlike LIHTC which has `hud_lihtc_projects.csv`).

Additionally, even when `USE_DB=true`, the scoring still compares against **all LIHTC projects** combined with Section 202 — it doesn't filter to only HUD 202-relevant competitors.

### Issue 2: Scoring Uses Wrong Comparison Set
When `senior_only` is selected, the current code:
1. Fetches all LIHTC projects (general affordable housing)
2. Appends Section 202 projects
3. Scores against the combined list

This is incorrect. For senior housing, we should be comparing primarily against **HUD 202 properties** (the actual senior housing competitors), not all LIHTC projects. The LIHTC data should still inform market context, but the competition score should reflect senior-specific competitors.

### Issue 3: Cache Key Missing `housing_target_population`
`main.py:680-693` — the `_cache_key` function does NOT include `housing_target_population`, meaning a `senior_only` query and an `all_ages` query for the same address will return cached results from whichever ran first.

---

## Implementation Plan

### Step 1: Add CSV fallback for Section 202 data
**File:** `backend/competitors/hud_section202.py` (new file)

Create a CSV-based fallback loader (mirroring `hud_lihtc.py` pattern) that:
- Loads from `backend/data/hud_section_202_properties.csv` if present
- Returns Section 202 properties within radius using haversine distance
- Provides the same dict structure as `_get_nearby_section_202_db()`

### Step 2: Fix housing.py to fetch Section 202 without requiring USE_DB
**File:** `backend/modules/housing.py`

- Remove the `and USE_DB` guard on Section 202 fetching (line 391)
- Add CSV fallback: try DB first, fall back to CSV loader
- When `senior_only`: use Section 202 projects as the **primary competitor set** for competition scoring
- Keep LIHTC projects for market context (saturation ratio) but weight them differently

### Step 3: Implement senior-specific scoring when `senior_only` is selected
**File:** `backend/modules/housing.py`

When `target_population == "senior_only"`:
- **Competition score**: Calculate based on Section 202 properties (the actual senior housing competitors), not all LIHTC
- **Market size**: Continue using cost-burdened households + senior population boost (already implemented)
- **Saturation ratio**: Calculate using Section 202 assisted units vs. senior population, not LIHTC units vs. cost-burdened renters
- **Weights**: Already differentiated via `_housing_weights()` — verify they're appropriate

Changes to `_score_housing()`:
- Accept an optional `section_202_projects` parameter
- When `senior_only`, compute `competition` using Section 202 project density instead of general LIHTC saturation
- Compute a senior-specific saturation ratio: `section_202_assisted_units / seniors_65_plus`

### Step 4: Fix the cache key to include `housing_target_population`
**File:** `backend/main.py`

Add `housing_target_population` to the `_cache_key()` payload dict so senior and all-ages analyses don't collide.

### Step 5: Update competitor list construction for proper separation
**File:** `backend/modules/housing.py`

When `senior_only`:
- Present Section 202 properties first in the competitor list (primary competitors)
- LIHTC properties second (market context)
- Label them clearly in affiliation field

When `all_ages`:
- Continue current behavior (LIHTC only, no Section 202)

### Step 6: Update tests
**Files:**
- `backend/tests/test_hud_section202.py` — update to verify CSV fallback works
- `backend/tests/test_housing_target_population.py` — update to verify senior-specific scoring uses Section 202 competition
- Add test for cache key including `housing_target_population`

### Step 7: Verify frontend displays correctly
The frontend (`CompetitorTable.tsx`, `SchoolMap.tsx`) already handles Section 202 properties with:
- Amber markers on map
- Section 202 filter tabs
- Full address display in table
- REAC inspection scores

No frontend changes expected — verify the data flows through correctly.

---

## Files to Modify
1. `backend/competitors/hud_section202.py` — **NEW** CSV fallback loader
2. `backend/modules/housing.py` — Core scoring and filtering changes
3. `backend/main.py` — Cache key fix
4. `backend/tests/test_hud_section202.py` — Test updates
5. `backend/tests/test_housing_target_population.py` — Test updates

## Risk Assessment
- **Low risk**: Cache key fix (Step 4) — straightforward addition
- **Medium risk**: Scoring changes (Step 3) — must verify existing tests still pass with senior-specific competition scoring
- **Low risk**: CSV fallback (Step 1) — additive, doesn't change DB path

## Questions for User
1. Should LIHTC projects still appear in the competitor list when `senior_only` is selected, or should we show only Section 202 properties?
2. For the senior-specific saturation ratio, should we use `seniors_65_plus` as the denominator (total senior population) or `cost_burdened_renter_households` filtered by senior status?
