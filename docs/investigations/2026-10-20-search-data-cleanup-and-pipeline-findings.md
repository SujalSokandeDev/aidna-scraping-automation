# Search Data Cleanup and Pipeline Findings

Recorded: 20 October 2026 (Asia/Calcutta).

## Scope and Status

This is a discovery/handoff note, not authorization to change production content. The search-content cleanup is a separate proposed task: no separate cleanup ticket has been created in this work, and no BigQuery UPDATE/DELETE, Vertex document update/deletion, reimport or reconciliation has been performed.

Related pipeline ticket: [AI.DNA #11 - Audit and harden AI.DNA scraping, state tracking and BigQuery delivery](https://github.com/Ztudium/AI.DNA/issues/11).

The pipeline ticket concerns reliable extraction, state tracking and delivery. Correcting old branding/ownership information already indexed by search is related but distinct. Do not mark either complete merely because these findings have been recorded.

## Current Connection

```text
Strapi CMS / website content
  -> scraping pipeline
  -> BigQuery output tables
  -> Vertex AI Search connector/import and indexing
  -> frontend search results and generated answers
```

Supabase is the scraper's state/checkpoint store, not the main content output. The pipeline does not directly update individual Vertex search documents.

### Frontend Search Configuration

Verified against the frontend-master repository's latest `main` during this review:

| Site | Configured Vertex engine ID |
| --- | --- |
| Wisdomia | `wisdomia-semantic-search_1784088359644` |
| CitiesABC / BusinessABC | `ai-dna-unified-search_1770209650725` |

Configured project number: `336443899767`; location: `global`; collection: `default_collection`; serving configuration: `default_search`. Shared defaults can be overridden by deployed `VERTEX_PROJECT_ID`, `VERTEX_ENGINE_ID` and `VERTEX_SERVING_CONFIG`. Repository configuration was checked; deployed environment overrides were not independently inspected. The user confirmed the intended search selection.

### BigQuery and Connected Search Sources

The user supplied this exact table for the read-only record review:

`ztudiumplatforms.strapi_content_data.unified_cms_content_combined`

| Table | Current GitHub scraping workflow | Connected search source shown in user's screenshot |
| --- | --- | --- |
| `unified_cms_content_combined` | CMS output: BusinessABC/CitiesABC and SportsABC | Ztudium Unified Content BigQuery Datastore - 2 |
| `unified_all_cms_content` | WordPress output | Ztudium Unified Content BigQuery Datastore |
| `unified_cms_only` | Not targeted by the current workflow; remains a CMS code fallback if `BIGQUERY_TABLE` is absent | Also connected under the older BigQuery connector |
| `unified_content_v2` | Not targeted by this workflow | Not shown as connected in the supplied search screenshot |
| `unified_sports_content` | Not targeted by this workflow; SportsABC currently goes to the combined CMS table | Not shown as connected in the supplied search screenshot |

The search also has an active Cloud Storage source named **Ztudium Global Datastore**. Correcting one BigQuery table does not prove the same old information has disappeared from every connected source.

The GitHub workflow gets its project from `GCP_PROJECT_ID`, uses dataset `strapi_content_data`, and explicitly sets its two output table names. GitHub masks the actual project secret in logs. The user's BigQuery table path establishes the reviewed table's project; the complete connector project/dataset mappings still need verification. Table existence and a recent sync timestamp alone do not prove complete end-to-end indexing.

## Search Cleanup Findings

Source document: **External (Extra) Test Findings - Manual.docx**, supplied by the user. It reports inaccurate platform/ownership answers in hero search and avatars. One answer cites **Ztudium Group - Knowledge Base & Company Profile**, pages 4 and 9. This citation is a lead to investigate, not proof that those documents exist in the reviewed BigQuery table or that every historical statement in them must be deleted.

### Confirmed Business Clarification

The user clarified that **Openbusinesscouncil has changed to BusinessABC**. Current-brand references should use **BusinessABC**, with links mapped to the corresponding `businessabc.net` destinations. Do not delete all records mentioning the former name or blindly swap domains: page paths may differ. Preservation of historically appropriate names should be included in the eventual correction policy.

### User-Reported Read-Only Query Results

These results were pasted by the user after running the supplied SELECT query. They have not been independently rerun against BigQuery here. The query searches serialized title, slug, source, content type, attributes and external images; it counts mentions anywhere in those fields, not verified factual errors. One row can match several concerns, and repeated logical IDs can occur across sources/types.

| Concern | Matching rows | Sample ID / source / type | Interpretation |
| --- | ---: | --- | --- |
| Openbusinesscouncil references | 2,294 | `1684` / `21` / `influencer` | Diane Von Furstenberg sample contains a Bill Gates wiki link on the former domain. A link migration candidate, not grounds to delete the influencer record. |
| `sportsabc.org` references | 79 | `7924` / `[14, 6]` / `post` | "Year in Review Roundup and Vision for 2025" includes a historical 2024 platform list. Review present-day naming separately from historical statements and internal CMS endpoint names. |
| Blockimpact references | 19 | `3201` / `0` / `post` | Amrita Sen / Dinis Guarda article explicitly says the software was sold to Glance Technologies. This is historical context, not the incorrect claim of current ownership. |
| Old 10-platform claims | 2 | `2237` / `0` / `company` | Odakyu Electric Railway sample concerns railway platforms. Confirmed false positive for that sample; inspect the other match rather than assuming both are ownership claims. |
| `fashionabc.com` references | 2 | `3713` / `0` / `post` | Samir Ceric / Blocksport article includes an old-domain link. Verify and correct its destination to the appropriate `fashionabc.org` URL. |
| Knowledge Base / Company Profile references | 0 | None | The wording was not found in this table. It may exist in another source, under different wording, or as a Cloud Storage document. |

Do not add these counts together as a distinct-record total. They are review counts, not the number of records proven to require updates. No bulk deletion is justified by these results.

### Proposed Correction Rules, Not Yet Applied

1. Update inaccurate current-brand references from Openbusinesscouncil to BusinessABC, using verified old-to-new URL mappings.
2. Check the two FashionABC old-domain matches and correct links where appropriate.
3. Inspect SportsABC references in context. Do not globally rename legitimate historical content or the SportsABC CMS source configuration.
4. Keep legitimate Blockimpact history; correct only unsupported current-platform/current-ownership claims.
5. Narrow platform-count detection to organizational ownership statements. Exclude unrelated uses such as railway platforms.
6. Locate the exact cited Knowledge Base / Company Profile source and other copies contributing to the incorrect answer before deciding which fields/pages need correction.

Before updates: obtain an approved replacement/mapping list, preview affected fields, preserve a backup, and identify rows by source + content type + ID rather than ID alone. Keep duplicated top-level fields and the relevant JSON `attributes` fields consistent. Verify the schema/encoding before editing nested content; do not globally replace arbitrary serialized JSON or URLs.

## Updating the Vertex Indexed Copy

Prompt instructions can influence generated answers but are not a reliable deletion mechanism. They do not remove outdated source documents, result cards or citations.

Google documents individual document **update (`PATCH`)** and **delete (`DELETE`)** APIs. Whether direct writes are permitted for the actual connector-managed entity store must be checked before choosing that route; API availability is not proof that this specific connector permits direct mutation.

For document imports:

- `INCREMENTAL` adds or updates documents with the same stable ID. Removing a BigQuery row is not itself an incremental deletion instruction.
- `FULL` reconciliation compares the imported source with the target store and removes documents missing from the source. It must not be run against a partial source that omits documents meant to remain.
- Automatically generated IDs based on content can change when content changes. Inspect the configured document-ID mapping before relying on an in-place update.
- One-time ingestion requires an explicit refresh. Periodic connectors follow their configured synchronization schedule and supported operations; do not assume a manual reimport is allowed.

The screenshots show connected sources and sync timestamps, not the full connector schedule, reconciliation/deletion semantics or document-ID mapping. Therefore automatic deletion propagation has **not** been confirmed for this setup.

Permanent corrections must address the authoritative CMS/source or a reviewed pipeline normalization/exclusion rule, the correct BigQuery record(s), and the Vertex indexed copy through its supported mechanism. Otherwise a later import/replay could restore old values. Content duplicated in another connected table or Cloud Storage may still appear until that source is corrected too. Verify the exact previously failing query and its citations afterward; do not assume visibility changes are instantaneous.

### Official References Reviewed

- [Create a search data store: BigQuery one-time versus periodic ingestion](https://docs.cloud.google.com/generative-ai-app-builder/docs/create-data-store-es#import-from-bigquery)
- [Refresh data: INCREMENTAL, FULL, and document-ID behavior](https://docs.cloud.google.com/generative-ai-app-builder/docs/refresh-data)
- [Update a document](https://docs.cloud.google.com/generative-ai-app-builder/docs/reference/rest/v1/projects.locations.collections.dataStores.branches.documents/patch)
- [Delete a document](https://docs.cloud.google.com/generative-ai-app-builder/docs/reference/rest/v1/projects.locations.collections.dataStores.branches.documents/delete)

## Pipeline #11: Actual Progress Versus Remaining Work

### Available / Verified

- The user reported restoring the paused Supabase account/project. The 20 October CMS run subsequently logged tracking activity and successful BigQuery inserts. A separate controlled read/write/checkpoint test has not yet been performed as part of #11 implementation.
- The latest reviewed run reports 983 BusinessABC/CitiesABC records and 97 SportsABC records inserted, totaling 1,080. These are log-reported successful inserts, not a complete independent BigQuery reconciliation or proof that every collection was fully extracted.
- Local branch `fix/fmr-sitemap-entrypoints` changes **only** FashionABC and FreedomX from `/sitemap_index.xml` to `/sitemap.xml`. HedgeThink, IntelligentHQ and TradersDNA retain their existing URLs and source identities.
- Live read-only checks: both corrected root URLs returned HTTP 200 and valid sitemap-index XML. FashionABC listed 14 child sitemaps; FreedomX listed 3. A sample `posts.xml?slice=0` child returned HTTP 200 and a URL set with 2,383 and 118 content URLs respectively. This was bounded endpoint validation, not a full scrape/publish test.
- Three local regression tests passed: corrected endpoints, unchanged remaining endpoints, and preservation of all five source tags. `git diff --check` passed.
- No scraping changes have been committed, pushed or deployed. The remote scheduled workflow does not yet contain these local URL fixes. No BigQuery rows, Supabase tracking history or Vertex records were changed during the sitemap checks.

### Still Outstanding / Not Claimed Fixed

- CMS API `400` errors and incomplete pagination/extraction despite a successful job exit need diagnosis. Earlier 502/503 observations are historical evidence to revalidate, not permanent root causes assumed fixed by unpausing Supabase.
- The 20 October WordPress job hit its three-hour timeout while processing HedgeThink. It did not complete all five sites. Need bounded/resumable processing and reliable progress reporting.
- The workflow's WordPress dependency on CMS can skip unrelated work after a CMS failure and needs review, including manual `wp` target behavior.
- Supabase managers catch several tracking failures and return fallback values. Need explicit storage readiness checks, visible failures and controlled checkpoint validation.
- WordPress can fall back to dry-run mode if BigQuery initialization fails. Production must not report no-upload execution as successful delivery.
- Need controlled tests of upload failure, partial results, modified-record refresh, pagination, stable identities, duplicates and replay/recovery. Streaming insert IDs alone must not be described as permanent uniqueness enforcement.
- CMS log artifact upload reported no files at the configured path. Notification/result handling must accurately distinguish success, partial extraction, failure, skip and cancellation.
- Neeraj's server-side sitemap/sub-sitemap redirection work is a separate dependency. Local scraper entry-point fixes do not implement those redirects.
- BigQuery-to-Vertex indexing configuration, exact source mappings and a controlled search-result verification remain separate checks.

Latest reviewed run: [20 October 2026 - AIDNA Nightly Scrape](https://github.com/SujalSokandeDev/aidna-scraping-automation/actions/runs/39365317341). CMS job was marked successful with the above extraction errors; WordPress was cancelled at its timeout. Do not describe the overall pipeline as repaired or verified end to end yet.

## Suggested Next Session

1. Resume #11 with the Supabase checkpoint/readiness test and current CMS error diagnosis.
2. Verify the corrected sitemap discovery in a bounded scrape, retaining existing state and content.
3. Harden independent execution, resumability, upload/state ordering and truthful reporting; run controlled end-to-end tests before release.
4. Scope a separate search-data correction ticket using the findings above. Approve mappings and inspect actual Vertex connector settings before any production data edits.

No additional implementation, ticket comments/status changes or deployments are implied by saving this note.
