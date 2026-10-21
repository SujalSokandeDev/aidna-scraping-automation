# Graceful website page failures

## Approved scope

Continue valid website delivery when isolated pages are missing, unsupported or temporarily unreachable. Preserve diagnostics without turning infrastructure or widespread failures into successful runs. Do not change CMS ingestion, delete source records, or mark skipped pages delivered.

## Approach

Keep the existing site/batch orchestration and request retry adapter. Preserve typed request exceptions so HTTP status can distinguish access denial from a page-level skip. Add a dedicated unusable-page exception rather than treating arbitrary programming errors as expected skips. Add 429 to the transient status retry list.

Track warning details (URL, reason, optional HTTP status) separately from fatal failures. Persist warning states as skipped, still eligible on the next nightly run; the existing attachment-only permanent exclusion remains unchanged. Reports use completed_with_warnings while retaining deferred counts. Job summaries list warning details and counts.

Fail if every attempted non-attachment page fails, or at least five and 20% fail. This tolerates isolated issues but flags broad extraction/source failures. Access denial, database delivery, state-store and discovery failures remain fatal. Existing successful deliveries are preserved.

## Verification

Regression tests cover isolated bad pages with good delivery, skip/recovery, widespread-failure boundaries, fatal infrastructure/access cases, report/summary output and real local HTTP retry/exhaustion behavior. Run the complete existing suite before committing and pushing. No production data changes or manual scraping dispatch are needed for this exception-handling change.
