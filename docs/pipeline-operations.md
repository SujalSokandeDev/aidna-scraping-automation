# Pipeline delivery and recovery

## Destinations and identity

- CMS: `ztudiumplatforms.strapi_content_data.unified_cms_content_combined`.
- Websites: `ztudiumplatforms.strapi_content_data.unified_all_cms_content`.
- No writes to `unified_cms_only`, other legacy tables, or Vertex indexes.
- Existing source tags and IDs are unchanged. BigQuery identity is `(id, source, content_type)` because CMS IDs overlap between collections.
- Delivery is a parameterized, atomic BigQuery MERGE followed by readback. Only then is Supabase success tracking persisted and read back.
- Oversized batches split automatically. A single large record uses a uniquely named, one-hour-expiring `aidna_stage_<uuid>` table in the same dataset, followed by MERGE/readback and deletion of only that staging table. Content is not truncated. This path requires table-create/load/delete permissions in addition to normal query/table-update permissions. Individual source rows beyond the safe 90 MB ceiling fail explicitly.
- Existing duplicates are not deleted. Retrying after an uncertain upload or state failure updates matching identities instead of appending another copy.
- Failed CMS batches are saved as failed, never successful, and later batches can continue. Deferred counts include work not attempted after an interruption. A state readback also checks the progress timestamp and BigQuery ID, not merely a pre-existing success/hash value.
- First-seen fields and prior enrichment images are retained. No state reset or historical-content deletion runs in either mode.

## Sources

CMS defaults are `https://cms.businessabc.net/graphql` and `https://cms.sportsabc.org/graphql`; repository secrets may override them. BusinessABC collections: post, city, company, education, investor, place, influencer, knowledgebase. SportsABC collections: athletes, teams, stadiums, sports, federations, nationality, post.

| Website | Sitemap |
| --- | --- |
| FashionABC | https://www.fashionabc.org/sitemap.xml |
| FreedomX | https://freedomx.com/sitemap.xml |
| HedgeThink | http://www.hedgethink.com/sitemap_index.xml |
| IntelligentHQ | https://www.intelligenthq.com/sitemap_index.xml |
| TradersDNA | http://www.tradersdna.com/sitemap_index.xml |

Nested sitemaps are traversed with cycle/duplicate protection. Child sitemap failures remain visible even if other children deliver content.
Category, tag, and author navigation sitemaps are explicitly excluded and listed in reports, matching the existing exclusion of those navigation URLs. This also avoids treating IntelligentHQ's HTML response at `post_tag-sitemap3.xml` as an article-discovery failure; broken article sitemaps still fail visibly.

## Environment and credentials

Required repository secrets: `GCP_PROJECT_ID`, `GCP_SERVICE_ACCOUNT_JSON`, `SUPABASE_URL`, `SUPABASE_KEY`, `BUSINESSABC_STRAPI_BEARER_TOKEN`, `SPORTSABC_STRAPI_BEARER_TOKEN`. Optional CMS endpoint overrides: `BUSINESSABC_STRAPI_GRAPHQL_URL`, `SPORTSABC_STRAPI_GRAPHQL_URL`.

The workflow writes the service account to an absolute, runner-local file and removes it afterward. Never put credentials in artifacts. BigQuery requires table read/update and query-job permissions; Supabase requires tracking-table reads/upserts and checkpoint CRUD. Missing permissions fail explicitly rather than enabling a dry run.

Existing tables from `supabase_schema.sql` are used; no destructive migration is required. Readiness checks create and remove only a uniquely named probe checkpoint.

## Running and verifying

`AIDNA Nightly Scrape` runs nightly and can be dispatched manually with `target=cms|wp|both`, `mode=incremental|full`, and a positive `limit`. Start with `limit=1` for a live smoke test. The limit is per CMS collection or website, not a total across all jobs.

CMS sources and websites have independent matrix jobs. A source failure does not cancel unrelated sources; the final result is not green if a selected job failed, was cancelled, or never executed. Global workflow concurrency serializes runs to avoid overlapping state updates.
Discovery is also time-bounded through `PIPELINE_MAX_SECONDS` (CMS default 3,300 seconds; website workflow 2,400 seconds). Exhausting discovery time produces an explicit incomplete-extraction error, preserving valid delivery and retryable state rather than relying on the runner timeout.

Both modes rediscover sources non-destructively. CMS first inventories IDs and `updatedAt`, then downloads only the bounded changed/failed/untracked selection. Full mode also rotates through existing records by oldest scrape time. The complete downloaded source record is hashed for tracking; incremental refresh depends on the CMS updating `updatedAt`. Transiently overloaded full-record pages can split into exact offset windows without skipping records. Websites retry failed/untracked records and refresh successful ones when sitemap lastmod is newer. Legacy successful records without a hash are refreshed once. A site with no lastmod cannot advertise later edits reliably; periodic explicit revalidation is a separate operational decision.

WordPress pages explicitly marked as media attachments are recorded as skipped with a reason and are not uploaded as articles. Empty article pages without that marker still fail. Existing BigQuery content is not deleted by this exclusion.
Website scrape timestamps are captured after fetching the page, so source timestamp changes during that request do not automatically look newer than the saved scrape snapshot.

Optional Wikipedia/Wikimedia enrichment remains in the code, but is disabled in the scheduled workflow to prioritize reliable content delivery and avoid slow, unrelated enrichment calls. Existing images are preserved on update.

Review every job summary and its `report-*.json` artifact. Reports include discovered/fetched, processed, confirmed, failed, deferred counts and a delivery manifest. `backlog` means a bounded run succeeded but there is still work outstanding; it does not mean all historical content is up to date. Dispatch another bounded run to continue. A failed page, invalid content, unconfirmed BigQuery delivery, or state-write failure must be investigated, not marked as complete.

No changes here rebrand historical article text or purge search results. Those findings remain separate in `docs/investigations/2026-10-20-search-data-cleanup-and-pipeline-findings.md`.

## Live verification on 20 October 2026

Repair PR: https://github.com/SujalSokandeDev/aidna-scraping-automation/pull/1 .

Initial controlled run: https://github.com/SujalSokandeDev/aidna-scraping-automation/actions/runs/39456378391 .

- SportsABC: all seven collections passed; seven genuine records were delivered to `unified_cms_content_combined` with `SDNA` source and their original content types. BigQuery readback and Supabase success-state readback were confirmed.
- All five website jobs delivered a genuine published record to `unified_all_cms_content` with their existing `WordPress/<site>` source tags and `post` content type.
- The first IntelligentHQ job correctly reported incomplete discovery because its tag-navigation sitemap served HTML. The follow-up excludes navigation-only children explicitly. Live read-only retesting then found 12,303 eligible content URLs with no discovery errors.
- BusinessABC delivered eight verified records, one from each collection. Full posts extraction failed at page 147 after repeated 503s; all other collections completed. Direct reproduction showed two half-page responses of approximately 61 MB and 163 MB. Lightweight inventory of the same page succeeded. Production discovery was changed to inventory-first, bounded full-record downloads to avoid loading those huge pages just to select work.
- Follow-up website verification confirmed IntelligentHQ's correction; HedgeThink surfaced an empty media-attachment URL. Explicit attachment detection/exclusion was added, while empty articles remain failures. Further end-to-end verification is required for these follow-up changes and staging-table delivery before declaring the repair complete.
- These checks ingest genuine content, not synthetic production records. They must not be deleted as test cleanup. The only temporary database entries are uniquely scoped readiness checkpoints, removed by the readiness checks.
- Historical backlogs remain. Bounded delivery and passing connection checks do not imply every record is already current, or that Vertex has reindexed the content.
- Server-side redirects remain Neeraj's separate responsibility. The scraper entrypoint fixes do not deploy website redirects.

### Successful controlled verification

Run https://github.com/SujalSokandeDev/aidna-scraping-automation/actions/runs/39460921546 completed successfully for both CMS sources and all five websites. It deliberately exercised the expiring staging-table path using genuine content (`limit=2`, `verify_staging=true`).

| Source | Confirmed records | Extraction/delivery failures |
| --- | ---: | ---: |
| BusinessABC/CitiesABC: all eight collections | 16 | 0 |
| SportsABC: all seven collections | 14 | 0 |
| FashionABC | 2 | 0 |
| FreedomX | 2 | 0 |
| HedgeThink | 2 | 0 |
| IntelligentHQ | 2 | 0 |
| TradersDNA | 2 | 0 |

All 40 deliveries included BigQuery readback, tracking confirmation and successful staging cleanup. BusinessABC posts inventory covered 24,236 IDs without the former page-147 extraction failure; only the two selected full records were downloaded. Logs and JSON reports are retained as run artifacts.

This was a smoke test, not a full historical backfill: reports still showed 40,428 CMS and 6,962 website candidates deferred across sources. These include new, changed and legacy tracking-refresh candidates, not necessarily records missing from BigQuery. Release the repair only after its latest automated checks pass, then start a normal production run with `target=both`, `limit=1000`, `verify_staging=false`; inspect per-source reports rather than treating a bounded green run as proof the entire backlog has cleared.

## Tests

Install the two requirements files plus PyYAML in an isolated Python environment, then run `python -m unittest discover -s tests -v`. The 49 tests cover inventory-first selection, pagination failures, exact offset recovery, permanent errors, discovery deadlines, safe retry/resume, state ordering and probe cleanup, mixed existing/new tracking batches, dry-run isolation, attachment exclusion, post-fetch timestamps, sitemap traversal, identity collisions, large-batch splitting/staging cleanup, failed-batch isolation, verified MERGE delivery, and independent workflow jobs. Live delivery verification is additionally required before declaring the production pipeline healthy.
