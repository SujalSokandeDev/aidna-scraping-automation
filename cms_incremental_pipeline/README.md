# CMS Incremental Pipeline

A standalone pipeline for incrementally scraping BusinessABC and SportsABC CMS content via Strapi GraphQL API and storing in BigQuery.

## Features

- **Incremental Scraping**: Only process new records by tracking `published_at` dates
- **SQLite Tracking**: Local database tracks each record's status (pending/success/failed)
- **BigQuery Integration**: Inserts into unified CMS content table with source column
- **Web Dashboard**: Flask-based UI for monitoring and control
- **CLI Support**: Command-line interface for automation

## Project Structure

```
cms_incremental_pipeline/
├── backend/
│   ├── core/
│   │   ├── database_manager.py   # SQLite record tracking
│   │   ├── cms_fetcher.py        # Strapi GraphQL client
│   │   ├── content_processor.py  # Content cleaning
│   │   └── bigquery_manager.py   # BigQuery insertion
│   ├── utils/
│   │   ├── config.py             # Configuration
│   │   └── logger.py             # Logging setup
│   ├── app.py                    # Flask dashboard
│   └── pipeline.py               # CLI orchestrator
├── frontend/
│   ├── templates/                # HTML templates
│   └── static/                   # CSS/JS assets
├── data/                         # SQLite databases + credentials
├── logs/                         # Log files
├── .env.example                  # Configuration template
├── requirements.txt              # Python dependencies
└── README.md
```

## Setup

### 1. Install Dependencies

```bash
cd cms_incremental_pipeline
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
cp .env.example .env
# Edit .env with your API tokens
```

### 3. Add Service Account

Place your `service-account.json` in the `data/` folder.

## Usage

### CLI Commands

```bash
# Run diagnostics (test connections)
python backend/pipeline.py --diagnostics

# Check for new records (dry run)
python backend/pipeline.py --check-new

# Incremental scrape (only new records)
python backend/pipeline.py --mode incremental

# Full scrape (all records)
python backend/pipeline.py --mode full

# Scrape specific source
python backend/pipeline.py --mode incremental --source businessabc

# View statistics
python backend/pipeline.py --stats

# Dry run (no BigQuery insert)
python backend/pipeline.py --mode incremental --dry-run
```

### Web Dashboard

```bash
python backend/app.py
# Open http://localhost:5001
```

## CMS Sources

| Source | Tag | Collections |
|--------|-----|-------------|
| BusinessABC | `BusinessABC/CitiesABC` | post, city, company, education, investor, place, influencer, knowledgebase |
| SportsABC | `SportsABC` | athletes, teams, stadiums, sports, federations, nationality, post |

## BigQuery Schema

The pipeline inserts into `unified_cms_content_combined` table:

| Column | Type | Description |
|--------|------|-------------|
| id | STRING | Strapi record ID |
| source | STRING | Source tag (e.g., "BusinessABC/CitiesABC") |
| content_type | STRING | Collection type (e.g., "post") |
| attributes | JSON | Structured content attributes |
| external_images | JSON | Wikimedia augmented images |
| title | STRING | Record title |
| slug | STRING | URL slug |
| locale | STRING | Content locale |
| published_at | TIMESTAMP | Strapi publish date |
| updated_at | TIMESTAMP | Last update date |
| created_at | TIMESTAMP | Creation date |
| processing_timestamp | TIMESTAMP | When processed by pipeline |

## How Incremental Scraping Works

1. **First Run**: Fetches ALL records, adds to SQLite tracking, inserts to BigQuery
2. **Subsequent Runs**:
   - Fetches records from API
   - Compares against SQLite database
   - Only processes records marked as "pending"
   - Updates status to "success" after BigQuery insert

## License

Internal use only - AIDNA Project
