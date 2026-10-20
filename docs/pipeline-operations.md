# Pipeline delivery and recovery

## Destinations and identity

- CMS: `ztudiumplatforms.strapi_content_data.unified_cms_content_combined`.
- Websites: `ztudiumplatforms.strapi_content_data.unified_all_cms_content`.
- No writes to `unified_cms_only`, other legacy tables, or Vertex indexes.
- Existing source tags and IDs are unchanged. BigQuery identity is `(id, source, content_type)` because CMS IDs overlap between collections.
- Delivery is a parameterized, atomic BigQuery MERGE followed by readback. Only then is Supabase success tracking persisted and read back.
- Existing duplicates are not deleted. Retrying after an uncertain upload or state failure updates matching identities instead of appending another copy.
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

Both modes rediscover sources non-destructively. CMS compares content hashes and retries changed/failed/untracked records. Websites retry failed/untracked records and refresh successful ones when sitemap lastmod is newer. Legacy successful records without a hash are refreshed once. A site with no lastmod cannot advertise later edits reliably; periodic explicit revalidation is a separate operational decision.

Optional Wikipedia/Wikimedia enrichment remains in the code, but is disabled in the scheduled workflow to prioritize reliable content delivery and avoid slow, unrelated enrichment calls. Existing images are preserved on update.

Review every job summary and its `report-*.json` artifact. Reports include discovered/fetched, processed, confirmed, failed, deferred counts and a delivery manifest. `backlog` means a bounded run succeeded but there is still work outstanding; it does not mean all historical content is up to date. Dispatch another bounded run to continue. A failed page, invalid content, unconfirmed BigQuery delivery, or state-write failure must be investigated, not marked as complete.

No changes here rebrand historical article text or purge search results. Those findings remain separate in `docs/investigations/2026-10-20-search-data-cleanup-and-pipeline-findings.md`.

## Tests

Install the two requirements files plus PyYAML in an isolated Python environment, then run `python -m unittest discover -s tests -v`. Tests cover pagination failures, permanent errors, safe retry/resume, state ordering, dry-run isolation, sitemap traversal, identity collisions, verified MERGE delivery, and independent workflow jobs. Live delivery verification is additionally required before declaring the production pipeline healthy.
