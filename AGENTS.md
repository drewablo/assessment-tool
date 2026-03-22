# Codex project instructions

This repository is an internal assessment platform used for directional feasibility analysis and board-ready outputs.

## Priorities
1. Stability over cleverness
2. Graceful degradation over silent failure
3. Minimal, reviewable patches over broad refactors
4. Preserve module-specific language and output fidelity
5. Prefer deletion/simplification when complexity adds little value

## Architecture expectations
- Separate ingest, scoring, API shaping, and frontend rendering concerns
- Do not mix school-specific logic into other modules
- Avoid adding live external fetches inside user-facing request paths
- Prefer cached, precomputed, or ingest-time enrichment where possible

## When debugging
- Identify exact file/function boundaries first
- Trace backend-to-frontend contracts before changing UI
- Check fallback behavior when source data is partial or stale
- Flag noisy logging, timeout risks, and repeated uncached work

## When proposing fixes
Always provide:
- Likely root cause
- Exact files/functions involved
- Smallest high-confidence fix
- Regression tests to add
- Any structural issue revealed by the bug
