-- =============================================================================
-- AIDNA Scraping Automation — Supabase Schema
-- Run this entire script in Supabase SQL Editor (https://supabase.com/dashboard)
-- =============================================================================

-- =============================================================================
-- TABLE 1: aidna_cms_scraped_records
-- Tracks CMS (BusinessABC + SportsABC) Strapi records for incremental scraping
-- Replaces: cms_incremental_pipeline/data/cms_records.db → scraped_records
-- =============================================================================

CREATE TABLE IF NOT EXISTS aidna_cms_scraped_records (
    record_id       TEXT PRIMARY KEY,                           -- e.g. "businessabc_post_123"
    source          TEXT NOT NULL,                              -- e.g. "BusinessABC/CitiesABC" or "SportsABC"
    cms_key         TEXT NOT NULL,                              -- e.g. "businessabc" or "sportsabc"
    collection      TEXT NOT NULL,                              -- e.g. "post", "city", "athletes"
    strapi_id       TEXT NOT NULL,                              -- Original Strapi record ID
    title           TEXT,                                       -- Record title for display
    published_at    TIMESTAMPTZ,                                -- Strapi publish timestamp
    first_seen      TIMESTAMPTZ DEFAULT NOW(),                  -- When pipeline first discovered this record
    last_scraped    TIMESTAMPTZ,                                -- Last successful/failed scrape attempt
    scrape_status   TEXT DEFAULT 'pending'                      -- 'pending', 'success', 'failed'
                    CHECK (scrape_status IN ('pending', 'success', 'failed', 'skipped')),
    content_hash    TEXT,                                       -- Hash of content for change detection
    bigquery_id     TEXT,                                       -- ID of record in BigQuery
    error_message   TEXT                                        -- Error message if scrape failed
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_cms_records_source ON aidna_cms_scraped_records(source);
CREATE INDEX IF NOT EXISTS idx_cms_records_cms_key ON aidna_cms_scraped_records(cms_key);
CREATE INDEX IF NOT EXISTS idx_cms_records_collection ON aidna_cms_scraped_records(collection);
CREATE INDEX IF NOT EXISTS idx_cms_records_status ON aidna_cms_scraped_records(scrape_status);
CREATE INDEX IF NOT EXISTS idx_cms_records_published_at ON aidna_cms_scraped_records(published_at);
CREATE INDEX IF NOT EXISTS idx_cms_records_last_scraped ON aidna_cms_scraped_records(last_scraped);

-- Disable RLS (pipeline is a trusted backend service, no user auth)
ALTER TABLE aidna_cms_scraped_records ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow all access for service role" ON aidna_cms_scraped_records
    FOR ALL USING (true) WITH CHECK (true);


-- =============================================================================
-- TABLE 2: aidna_wp_scraped_urls
-- Tracks WordPress article URLs across 5 sites for incremental scraping
-- Replaces: wordpress_scraping_pipeline/data/scraped_urls.db → scraped_urls
-- =============================================================================

CREATE TABLE IF NOT EXISTS aidna_wp_scraped_urls (
    url             TEXT PRIMARY KEY,                           -- Full article URL
    source          TEXT NOT NULL,                              -- e.g. "WordPress/FashionABC"
    sitemap_url     TEXT,                                       -- Sitemap where URL was found
    first_seen      TIMESTAMPTZ DEFAULT NOW(),                  -- When pipeline first discovered this URL
    last_scraped    TIMESTAMPTZ,                                -- Last scrape attempt timestamp
    scrape_status   TEXT DEFAULT 'pending'                      -- 'pending', 'success', 'failed', 'skipped'
                    CHECK (scrape_status IN ('pending', 'success', 'failed', 'skipped')),
    content_hash    TEXT,                                       -- Hash for change detection
    last_modified   TIMESTAMPTZ,                                -- lastmod from sitemap XML
    scrape_count    INTEGER DEFAULT 0,                          -- Number of scrape attempts
    error_message   TEXT,                                       -- Error message if failed
    bigquery_id     TEXT                                        -- ID of record in BigQuery
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_wp_urls_source ON aidna_wp_scraped_urls(source);
CREATE INDEX IF NOT EXISTS idx_wp_urls_status ON aidna_wp_scraped_urls(scrape_status);
CREATE INDEX IF NOT EXISTS idx_wp_urls_last_scraped ON aidna_wp_scraped_urls(last_scraped);

-- Disable RLS
ALTER TABLE aidna_wp_scraped_urls ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow all access for service role" ON aidna_wp_scraped_urls
    FOR ALL USING (true) WITH CHECK (true);


-- =============================================================================
-- TABLE 3: aidna_pipeline_checkpoints
-- Unified checkpoint tracking for both CMS and WordPress pipeline runs
-- Replaces: both pipelines' checkpoints.db → checkpoints
-- =============================================================================

CREATE TABLE IF NOT EXISTS aidna_pipeline_checkpoints (
    id              SERIAL PRIMARY KEY,
    crawl_id        TEXT UNIQUE NOT NULL,                       -- Unique run ID
    pipeline        TEXT NOT NULL                               -- 'cms' or 'wp'
                    CHECK (pipeline IN ('cms', 'wp')),
    source          TEXT,                                       -- Source being scraped
    mode            TEXT,                                       -- 'incremental' or 'full'
    start_time      TIMESTAMPTZ DEFAULT NOW(),                  -- Run start
    end_time        TIMESTAMPTZ,                                -- Run end
    total_items     INTEGER DEFAULT 0,                          -- Total items to process
    processed_items INTEGER DEFAULT 0,                          -- Items processed so far
    successful_items INTEGER DEFAULT 0,                         -- Items successfully scraped
    failed_items    INTEGER DEFAULT 0,                          -- Items that failed
    status          TEXT DEFAULT 'running'                      -- 'running', 'completed', 'failed', 'cancelled'
                    CHECK (status IN ('running', 'completed', 'failed', 'cancelled')),
    last_item_id    TEXT                                        -- Last processed record_id/url
);

CREATE INDEX IF NOT EXISTS idx_checkpoints_pipeline ON aidna_pipeline_checkpoints(pipeline);
CREATE INDEX IF NOT EXISTS idx_checkpoints_status ON aidna_pipeline_checkpoints(status);
CREATE INDEX IF NOT EXISTS idx_checkpoints_start_time ON aidna_pipeline_checkpoints(start_time);

ALTER TABLE aidna_pipeline_checkpoints ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow all access for service role" ON aidna_pipeline_checkpoints
    FOR ALL USING (true) WITH CHECK (true);


-- =============================================================================
-- TABLE 4: aidna_pipeline_run_state
-- Tracks last run status for each pipeline (replaces last_crawl_state.json)
-- =============================================================================

CREATE TABLE IF NOT EXISTS aidna_pipeline_run_state (
    pipeline            TEXT PRIMARY KEY                        -- 'cms', 'wp', or 'both'
                        CHECK (pipeline IN ('cms', 'wp', 'both')),
    last_started_at     TIMESTAMPTZ,
    last_completed_at   TIMESTAMPTZ,
    last_mode           TEXT,                                   -- 'incremental' or 'full'
    last_result         TEXT,                                   -- 'running', 'completed', 'failed'
    last_message        TEXT DEFAULT '',
    shutdown_requested  BOOLEAN DEFAULT FALSE,                  -- Only used for 'both'
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Seed initial rows so UPSERT always works
INSERT INTO aidna_pipeline_run_state (pipeline) VALUES ('cms')
ON CONFLICT (pipeline) DO NOTHING;
INSERT INTO aidna_pipeline_run_state (pipeline) VALUES ('wp')
ON CONFLICT (pipeline) DO NOTHING;
INSERT INTO aidna_pipeline_run_state (pipeline) VALUES ('both')
ON CONFLICT (pipeline) DO NOTHING;

ALTER TABLE aidna_pipeline_run_state ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow all access for service role" ON aidna_pipeline_run_state
    FOR ALL USING (true) WITH CHECK (true);


-- =============================================================================
-- RPC FUNCTIONS — For aggregation queries (GROUP BY, COUNT, etc.)
-- Called from Python via supabase.rpc('function_name', params)
-- =============================================================================

-- Get CMS stats (optionally filtered by source/collection)
CREATE OR REPLACE FUNCTION aidna_cms_get_stats(
    p_source TEXT DEFAULT NULL,
    p_collection TEXT DEFAULT NULL
)
RETURNS JSON
LANGUAGE plpgsql
AS $$
DECLARE
    result JSON;
BEGIN
    SELECT json_build_object(
        'total_records', COALESCE(COUNT(*), 0),
        'pending', COALESCE(SUM(CASE WHEN scrape_status = 'pending' THEN 1 ELSE 0 END), 0),
        'success', COALESCE(SUM(CASE WHEN scrape_status = 'success' THEN 1 ELSE 0 END), 0),
        'failed', COALESCE(SUM(CASE WHEN scrape_status = 'failed' THEN 1 ELSE 0 END), 0),
        'skipped', COALESCE(SUM(CASE WHEN scrape_status = 'skipped' THEN 1 ELSE 0 END), 0),
        'last_scraped', MAX(last_scraped)
    ) INTO result
    FROM aidna_cms_scraped_records
    WHERE (p_source IS NULL OR source = p_source)
      AND (p_collection IS NULL OR collection = p_collection);

    RETURN result;
END;
$$;


-- Get CMS breakdown by source/collection
CREATE OR REPLACE FUNCTION aidna_cms_get_breakdown()
RETURNS JSON
LANGUAGE plpgsql
AS $$
DECLARE
    result JSON;
BEGIN
    SELECT json_object_agg(
        key, val
    ) INTO result
    FROM (
        SELECT
            source || '/' || collection AS key,
            json_build_object(
                'source', source,
                'collection', collection,
                'total', COUNT(*),
                'success', SUM(CASE WHEN scrape_status = 'success' THEN 1 ELSE 0 END),
                'pending', SUM(CASE WHEN scrape_status = 'pending' THEN 1 ELSE 0 END),
                'failed', SUM(CASE WHEN scrape_status = 'failed' THEN 1 ELSE 0 END),
                'last_scraped', MAX(last_scraped)
            ) AS val
        FROM aidna_cms_scraped_records
        GROUP BY source, collection
    ) sub;

    RETURN COALESCE(result, '{}'::json);
END;
$$;


-- Get WordPress stats (optionally filtered by source)
CREATE OR REPLACE FUNCTION aidna_wp_get_stats(p_source TEXT DEFAULT NULL)
RETURNS JSON
LANGUAGE plpgsql
AS $$
DECLARE
    result JSON;
BEGIN
    SELECT json_build_object(
        'total_urls', COALESCE(COUNT(*), 0),
        'pending', COALESCE(SUM(CASE WHEN scrape_status = 'pending' THEN 1 ELSE 0 END), 0),
        'success', COALESCE(SUM(CASE WHEN scrape_status = 'success' THEN 1 ELSE 0 END), 0),
        'failed', COALESCE(SUM(CASE WHEN scrape_status = 'failed' THEN 1 ELSE 0 END), 0),
        'skipped', COALESCE(SUM(CASE WHEN scrape_status = 'skipped' THEN 1 ELSE 0 END), 0),
        'last_scraped', MAX(last_scraped)
    ) INTO result
    FROM aidna_wp_scraped_urls
    WHERE (p_source IS NULL OR source = p_source);

    RETURN result;
END;
$$;


-- Get WordPress breakdown by site/source
CREATE OR REPLACE FUNCTION aidna_wp_get_breakdown()
RETURNS JSON
LANGUAGE plpgsql
AS $$
DECLARE
    result JSON;
BEGIN
    SELECT json_object_agg(
        source, val
    ) INTO result
    FROM (
        SELECT
            source,
            json_build_object(
                'total', COUNT(*),
                'success', SUM(CASE WHEN scrape_status = 'success' THEN 1 ELSE 0 END),
                'pending', SUM(CASE WHEN scrape_status = 'pending' THEN 1 ELSE 0 END),
                'failed', SUM(CASE WHEN scrape_status = 'failed' THEN 1 ELSE 0 END),
                'last_scraped', MAX(last_scraped)
            ) AS val
        FROM aidna_wp_scraped_urls
        GROUP BY source
    ) sub;

    RETURN COALESCE(result, '{}'::json);
END;
$$;


-- =============================================================================
-- DONE! You should see 4 tables in Table Editor:
--   1. aidna_cms_scraped_records
--   2. aidna_wp_scraped_urls
--   3. aidna_pipeline_checkpoints
--   4. aidna_pipeline_run_state
-- And 4 RPC functions under Database → Functions
-- =============================================================================
