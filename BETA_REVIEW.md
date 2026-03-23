# Pre-Beta Audit Report — assessment-tool

**Date:** 2026-03-23
**Scope:** Full codebase audit (FastAPI backend + React/TypeScript frontend)
**Mode:** Read-only audit — no files modified

---

## Executive Summary

The assessment-tool platform is **approaching beta readiness** but has several issues that should be addressed before handing to testers. The backend scoring logic is generally well-protected against division-by-zero and handles external API failures gracefully with fallback strategies. However, there are meaningful gaps in security posture (no HTTPS, overly permissive CORS, plain-text password storage), a scoring logic bug where zero-population fallback values produce misleading results, and frontend UX gaps that will confuse non-technical Catholic ministry stakeholders.

**Top 3 Risks:**
1. **Security:** No TLS/HTTPS configured, CORS allows all methods/headers with credentials, and auth uses plain-text password comparison — collectively these expose the beta deployment to credential theft and MITM attacks.
2. **Scoring accuracy:** Saturation ratio fallback of `1.0` when target population is zero produces artificially poor competition scores, potentially misleading feasibility assessments in low-population catchments.
3. **Frontend UX:** Dashboard session handoff via `sessionStorage` + `window.open` is fragile across browsers, excessive chrome-to-content ratio on mobile, and pervasive financial/geospatial jargon will be opaque to board-level stakeholders.

---

## Phase 1: Backend — Bugs & Edge Cases

### Summary

The backend is well-structured with consistent division-by-zero guards across census, analysis, and scoring code. External API failures (Census, CMS, ORS isochrone, BLS) are handled with graceful degradation and fallback strategies. The main concerns are: (1) saturation ratio fallback values that produce misleading scores, (2) a latitude edge case in coordinate buffer calculations, and (3) partial cache states that silently degrade analysis quality without user-facing warnings.

**Key files reviewed:** `main.py`, `api/analysis.py`, `api/census.py`, `api/isochrone.py`, `api/geocoder.py`, `api/schools.py`, `api/school_stage2.py`, `modules/schools.py`, `modules/elder_care.py`, `modules/housing.py`, `pipeline/*.py`, `competitors/*.py`

---

## Phase 2: Backend — Security & Auth

### Summary

No hardcoded API keys or secrets were found in source code — all credentials load from environment variables. `.env.prod` is correctly gitignored. However, the security posture has significant gaps: CORS is overly permissive (`allow_methods=["*"]`, `allow_headers=["*"]` with `allow_credentials=True`), no security headers are set (HSTS, X-Content-Type-Options, CSP, X-Frame-Options), there is no rate limiting on any endpoint, the DELETE endpoint for history records has no RBAC beyond API key, and the Dockerfile runs as root. SQL injection is not a concern thanks to proper SQLAlchemy parameterized queries.

**Key files reviewed:** `main.py` (CORS/middleware), `db/connection.py`, `db/models.py`, `db/queries.py`, `.env.example`, `.env.prod.example`, `.gitignore`, `requirements.txt`, `Dockerfile`, `docker-compose*.yml`, `deploy.sh`, `nginx/nginx.conf`

---

## Phase 3: Frontend — UX / UI Polish

### Summary

The frontend has functional loading states and error handling for most paths, but several UX issues will impact beta testers: the dashboard session handoff mechanism is browser-dependent, error messages are vague (no guidance on recovery), the chrome-to-content ratio is poor on mobile (400px+ of headers/warnings before data), and terminology throughout assumes financial/demographic expertise that Catholic ministry board members are unlikely to have. Accessibility gaps include missing ARIA labels on interactive components, no alt text on map/chart renders, and sort headers lacking keyboard support.

**Key files reviewed:** All `src/app/**` pages, all `src/components/**` components, `src/lib/*.ts`, `middleware.ts`, `next.config.js`, `package.json`

---

## Phase 4: Performance & Scalability

### Summary

The backend uses asyncio effectively for external API calls, and Redis caching reduces redundant Census/CMS fetches. Key performance concerns: (1) no rate limiting means 5-10 concurrent beta testers could exhaust Census API quotas (500 requests/day for free tier), (2) PDF upload reads entire file into memory (up to 25MB per nginx config), (3) frontend renders unbounded highlight card lists without virtualization, and (4) the intelligence/opportunities page holds all sorted results in memory with only client-side pagination.

---

## 🔴 Blockers — Will break the app or expose sensitive data

### B1. No HTTPS/TLS in Production Nginx
- **File:** `nginx/nginx.conf:2`, `docker-compose.prod.yml:120`
- **Issue:** Nginx only listens on port 80 (HTTP). No SSL/TLS configuration. All traffic — including API keys, auth tokens, and assessment data — transmitted in plaintext.
- **Impact:** MITM attacks can intercept credentials and sensitive ministry feasibility data.
- **Fix:** Add Let's Encrypt/certbot integration; configure TLS termination in nginx.

### B2. Plain-Text Password Comparison in Auth
- **File:** `frontend/src/app/api/auth/[...nextauth]/route.ts:30`
- **Issue:** `stored === password` performs plain-text comparison. Credentials stored as `AUTH_USERS="alice:pass1,bob:pass2"` in environment variable with no hashing.
- **Impact:** If environment is leaked, all accounts are immediately compromised. No defense-in-depth.
- **Fix:** Hash passwords with bcrypt/argon2; store hashes in AUTH_USERS or a database.

### B3. Overly Permissive CORS with Credentials
- **File:** `backend/main.py:924-930`
- **Issue:** `allow_methods=["*"]` and `allow_headers=["*"]` combined with `allow_credentials=True`. This allows any HTTP method (including TRACE, DELETE) and any header from the allowed origin.
- **Impact:** Enables credential theft via crafted requests if an XSS vulnerability exists in the frontend.
- **Fix:** Restrict to `allow_methods=["GET", "POST", "DELETE", "OPTIONS"]` and `allow_headers=["Content-Type", "X-API-Key", "Authorization"]`.

### B4. Missing Security Headers
- **File:** `backend/main.py` (no security header middleware present)
- **Issue:** No middleware adds standard security headers:
  - ❌ `X-Content-Type-Options: nosniff`
  - ❌ `X-Frame-Options: DENY`
  - ❌ `Strict-Transport-Security` (HSTS)
  - ❌ `Content-Security-Policy`
- **Impact:** Vulnerable to clickjacking, MIME-type sniffing, and XSS amplification.
- **Fix:** Add a Starlette middleware that sets these headers on all responses.

### B5. Saturation Ratio Fallback Produces Misleading Scores
- **Files:** `backend/modules/elder_care.py:253`, `backend/modules/housing.py:242`
- **Issue:** When target population (e.g., seniors_65_plus, cost_burdened) is zero, `saturation_ratio` defaults to `1.0` instead of `0.0`. A ratio of 1.0 maps to a competition score of ~30 (high competition), which is incorrect — zero population means no demand signal, not high saturation.
- **Impact:** Feasibility assessments in low-population catchments will show artificially poor competition scores, potentially causing incorrect "do not proceed" recommendations.
- **Fix:** Change fallback from `1` to `0` (or return a neutral score with a data quality flag).

### B6. No Rate Limiting on Any Endpoint
- **File:** `backend/main.py` (all route handlers)
- **Issue:** No rate limiting middleware. External API dependencies (Census: 500 req/day free tier, ORS, BLS) have their own rate limits that will cascade as failures if multiple beta testers run analyses simultaneously.
- **Impact:** 5-10 concurrent testers could exhaust Census API quota, causing all subsequent analyses to fail for the rest of the day.
- **Fix:** Add per-IP rate limiting (e.g., `slowapi`) and implement request queuing for external API calls.

---

## 🟡 Warnings — Degraded experience or technical debt

### W1. Hardcoded Default Database Credentials in Source Code
- **File:** `backend/db/connection.py:15-18`
- **Issue:** Fallback `DATABASE_URL` includes `feasibility:feasibility` credentials. While env vars take precedence, the hardcoded default is exposed in version control.
- **Risk:** If `DATABASE_URL` env var is accidentally unset, production connects with known default credentials.

### W2. Docker Container Runs as Root
- **File:** `backend/Dockerfile`
- **Issue:** No `USER` directive — container processes run as root.
- **Risk:** Container escape vulnerabilities have elevated impact.

### W3. Redis Has No Authentication
- **File:** `docker-compose.yml:23`, `backend/main.py:688`
- **Issue:** Redis URL `redis://redis:6379/0` has no password. No `requirepass` configured.
- **Risk:** Anyone with network access to the Redis port can read/write cached data.

### W4. DELETE Endpoint Without RBAC
- **File:** `backend/main.py` (DELETE `/api/history/{record_id}`)
- **Issue:** DELETE method on analysis history only requires API key, no per-user authorization. Record IDs are sequential integers (trivially enumerable).
- **Risk:** Any authenticated user can delete any other user's analysis history.

### W5. File Upload Validates Extension Only
- **File:** `backend/main.py:1284-1295`
- **Issue:** PDF upload checks `.pdf` extension but not MIME type or file magic bytes. Entire file loaded into memory (`await uploaded.read()`).
- **Risk:** Memory exhaustion with large files; malicious non-PDF files with `.pdf` extension processed by pdfplumber.

### W6. Partial Cache State Silently Degrades Analysis
- **File:** `backend/modules/elder_care.py:355-363`
- **Issue:** When senior demographics are partially hydrated (65+ present but living-alone/poverty missing), a warning is logged but no fallback or confidence downgrade is applied. Downstream scoring uses missing fields as 0.
- **Risk:** Mission-mode scoring biased by incomplete demographic data without user awareness.

### W7. Stage 2 Partial Weighted Average Not Normalized
- **Files:** `backend/modules/elder_care.py:105-112`, `backend/modules/housing.py:151-158`
- **Issue:** When only some Stage 2 components are scored, the weighted average divides by the sum of available weights rather than the full weight budget. If only one low-weight component is provided, the score reflects only that component without normalization.
- **Risk:** Partial Stage 2 scores may over- or under-represent individual components.

### W8. GEOID Normalization Accepts Invalid Structures
- **File:** `backend/pipeline/ingest_hud_foundation.py:45-53`
- **Issue:** GEOIDs are zero-padded to 11 digits regardless of internal structure. A 3-digit input becomes `00000000123`, which is structurally invalid (should be 2+3+6 = state+county+tract).
- **Risk:** Mismatched join keys when linking HUD data to census tracts.

### W9. Competitor Name Filter After Typology Filter May Miss Exclusions
- **File:** `backend/pipeline/ingest_schools.py:79-91`
- **Issue:** Name-based exclusion keywords (behavioral, therapeutic) are checked only if the school passes the typology filter. Schools with excluded keywords but non-special-ed typology codes (e.g., typology=7) will be included as competitors.
- **Risk:** Some behavioral/therapeutic schools may appear as Catholic school competitors.

### W10. Latitude Edge Case in Coordinate Buffer
- **File:** `backend/api/census.py:313`
- **Issue:** `lon_buffer = radius_miles / (69.0 * math.cos(math.radians(lat)))` — at latitude near ±90°, `cos()` approaches 0, causing ZeroDivisionError.
- **Risk:** Extremely unlikely in practice (no Catholic ministries at the poles) but mathematically unsound.

### W11. Frontend Dashboard Session Handoff is Browser-Dependent
- **File:** `frontend/src/lib/dashboard-session.ts:10-12`
- **Issue:** `openDashboard()` writes to `sessionStorage` then calls `window.open("/dashboard", "_blank")`. Per HTML5 spec, the new window receives a *copy* of sessionStorage, but this behavior varies across browsers and can fail in incognito/private browsing modes.
- **Risk:** Dashboard may fail to load context in certain browser configurations, showing a "No analysis context found" error.

### W12. Excessive Chrome-to-Content Ratio on Mobile
- **Files:** `frontend/src/app/page.tsx:187-278`, `frontend/src/components/ResultsDashboard.tsx:153-195`
- **Issue:** 400px+ of headers, branding, info cards, and warning boxes before the user reaches the analysis form or results. On mobile, the content-to-chrome ratio is approximately 1:3.
- **Risk:** Beta testers on tablets/phones will need excessive scrolling to reach actionable content.

### W13. Pervasive Jargon Unsuitable for Non-Technical Stakeholders
- **Files:** Multiple components (`ResultsDashboard.tsx`, `MetricCard.tsx`, `DemographicsPanel.tsx`, `dashboard-live.ts`)
- **Issue:** Terms like "market depth ratio," "saturation," "propensity-weighted families," "isochrone," "catchment," "cost-burden rate," and "Stage 2 weighting profile" are used without plain-language explanations.
- **Risk:** Board-level Catholic ministry stakeholders will require training or post-processing to interpret dashboard outputs.

### W14. Data Freshness Warning Threshold Too Lenient
- **File:** `frontend/src/components/ResultsDashboard.tsx:115-127`
- **Issue:** Stale data warning only triggers when `freshness_hours > 8760` (1 year). Data from 6-11 months ago shows no staleness indicator.
- **Risk:** Users may make decisions based on significantly outdated demographic data without awareness.

### W15. No Brute Force Protection on Auth
- **Files:** `frontend/src/app/api/auth/[...nextauth]/route.ts`, `frontend/src/middleware.ts`
- **Issue:** No rate limiting on `/api/auth` endpoints. No account lockout after failed attempts.
- **Risk:** Credential stuffing or brute force attacks against beta user accounts.

### W16. `/api/data-health` Endpoint Bypasses Auth in Some Configurations
- **File:** `backend/main.py:964`
- **Issue:** The `/api/data-health` endpoint returns database table counts, pipeline status, and operational metadata. If `INTERNAL_API_KEY` is not set, this endpoint is publicly accessible.
- **Risk:** Information disclosure about database schema and data volumes.

---

## 🟢 Polish — Minor UX/UI or code quality improvements

### P1. Geocoder Coordinate Validation
- **File:** `backend/api/geocoder.py:77-87`
- No validation that returned coordinates fall within valid lat/lon ranges. Should clamp lat to [-90, 90] and lon to [-180, 180].

### P2. PSS Empty Coordinate Columns — Silent Fallback
- **File:** `backend/api/schools.py:375-376`
- Returns empty competitor list without raising an error or adding a data quality note when PSS coordinate columns are missing.

### P3. Elder Care Facility Affiliation String Logic
- **File:** `backend/modules/elder_care.py:195-199`
- Substring containment check for owner/affiliation matching produces inconsistent display depending on which string is shorter. Should use exact match or string similarity.

### P4. Adjusted Catholic Percentage Not Clamped
- **File:** `backend/api/analysis.py:301-305`
- `_adjusted_catholic_pct()` can theoretically return values outside [0, 1]. Should clamp result.

### P5. Longitude Buffer Approximation Uses Fixed 50.0
- **Files:** `backend/competitors/hud_lihtc.py:63-64`, `backend/competitors/hud_section202.py:96-97`
- Uses `radius_miles / 50.0` for longitude buffer; at 35°N actual value is ~54.5 miles/degree, causing slight under-inclusion. The precise haversine check downstream compensates, but the pre-filter is narrower than intended.

### P6. `__import__("math")` Anti-Pattern
- **File:** `backend/services/dashboard_service.py:230`
- Inline `__import__("math")` instead of top-level import. Functional but poor style.

### P7. Accessibility Gaps in Frontend
- **Files:** Multiple components
- Missing `htmlFor` on some form labels, no alt text on Leaflet map renders, sort headers in `CompetitorTable.tsx` lack `onKeyDown` handlers, only ~9 ARIA labels across main components.

### P8. Intelligence Page — No "Load More" Pagination
- **File:** `frontend/src/app/intelligence/page.tsx:203-211`
- Client-side pagination at 25 items with all results held in memory. No server-side cursor or "load more" pattern.

### P9. Unbounded Highlight Card Rendering
- **File:** `frontend/src/components/dashboard/modules/ModuleDashboardView.tsx:212-217`
- All highlight cards render without virtualization. Large result sets (50+) will bloat DOM.

### P10. No Version Info in Health Endpoint
- **File:** `frontend/src/app/api/health/route.ts:6`
- Returns `{ status: "ok" }` without commit hash or version info. Cannot detect stale deployments.

### P11. SQL Echo Risk in Production
- **File:** `backend/db/connection.py:22`
- `SQL_ECHO` env var can enable full SQL query logging including sensitive data. Should be disabled in production configs.

### P12. No Refresh Button on Results Dashboard
- **File:** `frontend/src/components/ResultsDashboard.tsx`
- Once results load, users cannot request fresh data without re-running the full analysis.

### P13. Error Messages Don't Distinguish Network vs API Errors
- **File:** `frontend/src/lib/api.ts:12-17`
- Generic error handling doesn't help users understand if the issue is connectivity, backend timeout, or data quality.

### P14. Horizontal Scroll Without Visual Cue
- **File:** `frontend/src/components/BenchmarkPanel.tsx:82`
- Table wraps with `overflow-x-auto` but no scrollbar indicator or fade effect to signal more content.

---

## Recommended Pre-Beta Checklist

Ordered by risk — fix top items before handing to testers:

1. **[B1] Configure HTTPS/TLS** in nginx with certificate management (Let's Encrypt or provided cert)
2. **[B2] Implement password hashing** in NextAuth credentials provider (bcrypt or argon2)
3. **[B3] Restrict CORS** to specific methods and headers (remove wildcard `*`)
4. **[B4] Add security headers** middleware (X-Content-Type-Options, X-Frame-Options, HSTS, CSP)
5. **[B5] Fix saturation ratio fallback** — change from `1` to `0` in elder_care.py:253 and housing.py:242
6. **[B6] Add rate limiting** (slowapi or similar) to protect external API quotas
7. **[W1] Remove hardcoded DB credentials** from connection.py default; require explicit DATABASE_URL
8. **[W4] Add per-user authorization** on DELETE endpoint for history records
9. **[W5] Validate PDF uploads** by MIME type/magic bytes, not just extension
10. **[W11] Add fallback for dashboard session** — use URL query params or localStorage as backup
11. **[W13] Add plain-language glossary** or tooltip layer for non-technical stakeholders
12. **[W12] Reduce mobile chrome** — collapse header/info sections or move below fold
13. **[W15] Add brute force protection** — rate limit auth endpoints, add account lockout
14. **[W2] Add non-root USER** directive to Dockerfile
15. **[W3] Configure Redis authentication** with requirepass
16. **[W14] Lower data freshness warning** threshold from 1 year to 90 days

---

*Report generated by automated pre-beta audit — 2026-03-23*
