# AIDNA Scraping Automation

Automated nightly scraping of CMS (BusinessABC/SportsABC) and WordPress content via GitHub Actions with Supabase state tracking.

## Quick Setup

### 1. Create Supabase Tables

Open [Supabase SQL Editor](https://supabase.com/dashboard/project/wqvtdlefonhnudybdyii/sql) and paste the contents of [`supabase_schema.sql`](supabase_schema.sql). Click **Run**.

You should see 4 tables in Table Editor:
- `aidna_cms_scraped_records`
- `aidna_wp_scraped_urls`
- `aidna_pipeline_checkpoints`
- `aidna_pipeline_run_state`

### 2. Create GitHub Repository

```bash
# Create new repo on GitHub named: aidna-scraping-automation
# Then locally:
cd "d:\Ztudium\AIDNA\AIDNA Combined\aidna-scraping-automation"
git init
git remote add origin https://github.com/YOUR_USERNAME/aidna-scraping-automation.git
```

### 3. Copy Pipeline Code

Copy the pipeline folders into this repo folder:

```bash
# From: d:\Ztudium\AIDNA\AIDNA Combined\
xcopy /E /I "cms_incremental_pipeline" "aidna-scraping-automation\cms_incremental_pipeline"
xcopy /E /I "wordpress_scraping_pipeline" "aidna-scraping-automation\wordpress_scraping_pipeline"
```

### 4. Replace Database Managers

Replace the SQLite database managers with the Supabase versions:

```bash
copy cms_database_manager.py cms_incremental_pipeline\backend\core\database_manager.py
copy wp_database_manager.py wordpress_scraping_pipeline\backend\core\database_manager.py
```

### 5. Update requirements.txt

Add `supabase>=2.0.0` to both:
- `cms_incremental_pipeline/requirements.txt`
- `wordpress_scraping_pipeline/backend/requirements.txt`

### 6. Add SUPABASE_URL and SUPABASE_KEY to both `.env` files

```
SUPABASE_URL=https://wqvtdlefonhnudybdyii.supabase.co
SUPABASE_KEY=your_anon_key_here
```

### 7. Configure GitHub Secrets

Follow [`GITHUB_SECRETS.md`](GITHUB_SECRETS.md) — create all 8 secrets in:
**Settings → Secrets and variables → Actions → New repository secret**

### 8. Push and Test

```bash
git add .
git commit -m "Initial setup: pipelines + GitHub Actions"
git push -u origin main
```

Then go to **Actions → AIDNA Nightly Scrape → Run workflow** to test manually.

## How It Works

```
Every night at midnight UTC (5:30 AM IST):

1. CMS Pipeline runs (BusinessABC + SportsABC)
   → Fetches new records from Strapi GraphQL APIs
   → Tracks state in Supabase (aidna_cms_scraped_records)
   → Inserts new content into BigQuery

2. WordPress Pipeline runs (5 sites)
   → Crawls sitemaps for new URLs
   → Tracks state in Supabase (aidna_wp_scraped_urls)
   → Scrapes articles, cleans HTML, adds Wikimedia images
   → Inserts into BigQuery

3. Notification job reports success/failure
```

## Files in This Repo

| File | Purpose |
|------|---------|
| `.github/workflows/nightly-scrape.yml` | GitHub Actions workflow (cron + manual) |
| `supabase_schema.sql` | Supabase tables, indexes, RPC functions |
| `GITHUB_SECRETS.md` | Secrets reference with values |
| `cms_database_manager.py` | Supabase-backed DB manager for CMS pipeline |
| `wp_database_manager.py` | Supabase-backed DB manager for WordPress pipeline |
| `cms_incremental_pipeline/` | CMS pipeline code (copied from main project) |
| `wordpress_scraping_pipeline/` | WordPress pipeline code (copied from main project) |
