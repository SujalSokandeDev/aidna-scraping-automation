# WordPress Multi-Site Scraping Pipeline

A production-grade WordPress scraping system that extracts content from 5 WordPress sites, cleans and augments it with Wikimedia images, and stores in BigQuery.

## Features

- **Multi-Site Support**: Scrapes 5 configured WordPress sites via sitemaps
- **Incremental Scraping**: Only scrapes NEW URLs (saves time and resources)
- **Content Cleaning**: Extracts plain text from HTML (no HTML tags in output)
- **Wikimedia Augmentation**: Fetches 2 relevant images from Wikimedia Commons per article
- **BigQuery Storage**: Inserts into existing `unified_all_cms_content` table
- **URL Tracking**: SQLite database tracks all scraped URLs
- **Checkpoints**: Resume capability for interrupted scrapes
- **Web Dashboard**: Flask UI for monitoring and control
- **Real-time Logs**: Live log streaming in web UI

## Configured Sites

| Site | URL | Source Tag |
|------|-----|------------|
| FashionABC | https://www.fashionabc.org/ | WordPress/FashionABC |
| FreedomX | https://freedomx.com/ | WordPress/FreedomX |
| HedgeThink | http://www.hedgethink.com/ | WordPress/HedgeThink |
| IntelligentHQ | https://www.intelligenthq.com/ | WordPress/IntelligentHQ |
| TradersDNA | http://www.tradersdna.com/ | WordPress/TradersDNA |

## Installation

### 1. Clone/Copy Project
```bash
cd d:/Ztudium/AIDNA/AIDNA Combined/wordpress_scraping_pipeline
```

### 2. Create Virtual Environment (Recommended)
```bash
python -m venv venv
venv\Scripts\activate  # Windows
# source venv/bin/activate  # Linux/Mac
```

### 3. Install Dependencies
```bash
cd backend
pip install -r requirements.txt
```

### 4. Configure Environment
```bash
# Copy example config
cd ..
copy .env.example .env

# Edit .env with your settings
notepad .env
```

### 5. Setup BigQuery (Required for data insertion)
1. Create a GCP service account with BigQuery permissions
2. Download the JSON key file
3. Save as `data/service-account.json`
4. Update `GCP_PROJECT_ID` in `.env`

## Usage

### CLI Pipeline

```bash
cd backend

# Check for new URLs (dry run)
python pipeline.py --check-new

# Scrape only new URLs (incremental)
python pipeline.py --mode incremental

# Scrape single site
python pipeline.py --mode incremental --site FashionABC

# Full re-scrape (all URLs)
python pipeline.py --mode full

# Show statistics
python pipeline.py --stats

# Dry run (no BigQuery insert)
python pipeline.py --mode incremental --dry-run
```

### Web Dashboard

```bash
cd backend
python app.py
```

Then open `http://localhost:5000` in your browser.

**Dashboard Features:**
- Overview statistics (total URLs, success, pending, failed)
- Per-site breakdown with success rates
- One-click scraping controls
- Check for new URLs button
- Real-time progress tracking
- Log viewer with filtering

## Project Structure

```
wordpress_scraping_pipeline/
├── backend/
│   ├── core/
│   │   ├── __init__.py
│   │   ├── database_manager.py   # SQLite URL tracking
│   │   ├── sitemap_crawler.py    # Parse sitemaps
│   │   ├── content_scraper.py    # Extract article content
│   │   ├── content_cleaner.py    # HTML to plain text
│   │   ├── wikimedia_augmenter.py # Fetch Wikimedia images
│   │   └── bigquery_manager.py   # BigQuery insertion
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── config.py             # Configuration loader
│   │   └── logger.py             # Logging setup
│   ├── app.py                    # Flask web application
│   ├── pipeline.py               # CLI orchestrator
│   └── requirements.txt
├── frontend/
│   ├── static/
│   │   ├── css/styles.css
│   │   └── js/dashboard.js
│   └── templates/
│       ├── index.html            # Dashboard
│       ├── site_detail.html      # Per-site view
│       └── logs.html             # Log viewer
├── data/                         # Auto-created
│   ├── scraped_urls.db           # URL tracking database
│   ├── checkpoints.db            # Pipeline checkpoints
│   └── service-account.json      # YOU PROVIDE THIS
├── logs/                         # Auto-created
│   └── wordpress_pipeline.log
├── .env.example
├── .env                          # YOU CREATE THIS
└── README.md
```

## Configuration Options

| Variable | Description | Default |
|----------|-------------|---------|
| `GCP_PROJECT_ID` | Google Cloud project ID | Required |
| `BIGQUERY_DATASET` | BigQuery dataset name | strapi_content_data |
| `BIGQUERY_TABLE` | BigQuery table name | unified_all_cms_content |
| `REQUEST_DELAY` | Delay between requests (seconds) | 1.5 |
| `MAX_RETRIES` | Max retry attempts | 3 |
| `WIKIMEDIA_MAX_IMAGES` | Images to fetch per article | 2 |
| `INSERT_BATCH_SIZE` | Records per BigQuery batch | 20 |
| `FLASK_PORT` | Web UI port | 5000 |

## URL Filtering

The pipeline automatically skips non-content URLs:
- `/tag/*` - Tag archives
- `/category/*` - Category archives  
- `/author/*` - Author pages
- `/page/\d+` - Pagination
- URLs with `?` - Query parameters
- `/feed/` - RSS feeds

Only article/content URLs are scraped.

## BigQuery Schema

Records are inserted into `unified_all_cms_content` with this structure:

```json
{
  "id": "FashionABC_abc123",
  "source": "WordPress/FashionABC",
  "content_type": "post",
  "attributes": {
    "title": "Article Title",
    "content": "Plain text content (NO HTML)",
    "categories": ["Category1"],
    "tags": ["tag1"],
    "images": [{"url": "...", "alt": "..."}]
  },
  "external_images": [
    {"source": "wikimedia_commons", "image_url": "..."}
  ],
  "title": "Article Title",
  "published_at": "2024-01-15T10:00:00Z"
}
```

## Troubleshooting

### "No module named 'google.cloud'"
```bash
pip install google-cloud-bigquery
```

### BigQuery authentication error
1. Verify `service-account.json` path in `.env`
2. Check service account has BigQuery permissions
3. Try running with `--dry-run` flag to test without BigQuery

### Sitemap fetch errors
- Check if site is accessible from your network
- Try increasing `REQUEST_TIMEOUT` in `.env`
- Some sites may block automated requests

### Web UI not loading
1. Ensure Flask is running: `python app.py`
2. Check port 5000 is not in use
3. Try `http://127.0.0.1:5000` instead of localhost

## License

Internal project - not for redistribution.
