# odis-php

pure PHP implementation of a crawler, search page and dashboard to get info from ODIS-arch compatible sources.

## Usage

The application provides a command-line interface for crawling and indexing metadata.

### 1. Crawling and Indexing

```bash
# Complete crawl
php bin/console app:odis:crawl

# Parallel crawl for better performance
php bin/console app:odis:crawl --parallel --concurrency 5

# Targeted crawl (by ID)
php bin/console app:odis:crawl 3215 3125

# Clear Elasticsearch index before starting (mandatory for mapping fixes)
php bin/console app:odis:crawl --clear-index
```

### 2. Maintenance and Reset

```bash
# Clear all crawl statistics and report data
php bin/console app:odis:clear-stats

# Clear EVERYTHING (Search index + Stats)
php bin/console app:odis:clear-stats --all
```

For more details on available options, run:
```bash
php bin/console app:odis:crawl --help
php bin/console app:odis:clear-stats --help
```

### 3. Search and Dashboard

- **Search Interface**: Accessible at `/search`
- **Crawler Dashboard**: Accessible at `/dashboard`
- **Documentation**: A detailed usage guide is available on the home page (`/`)

## Configuration

Configure your environment variables in `.env` or `.env.local`:

- `ELASTICSEARCH_URL`: URL of your Elasticsearch instance
- `ELASTICSEARCH_USER`: Elasticsearch username
- `ELASTICSEARCH_PASSWORD`: Elasticsearch password
- `DATABASE_URL`: Connection string for the PostgreSQL database
