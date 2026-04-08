# odis-php

pure PHP implementation of a crawler, search page and dashboard to get info from ODIS-arch compatible sources.

## Requirements

- **PHP**: 8.4 or higher
- **PHP Extensions**:
    - `ctype` (standard)
    - `iconv` (standard)
    - `simplexml` (required for XML parsing)
    - `pdo_sqlite` or `pdo_pgsql` (depending on the database used)
    - `intl` (recommended for internationalization)
    - `mbstring` (standard for string manipulation)
    - `curl` (required for HTTP requests to Elasticsearch and ODIS)
- **Database**: SQLite (default) or PostgreSQL
- **Elasticsearch**: 8.0 or higher (v9.3 recommended)
- **Composer**: 2.x

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/your-repo/odis-php.git
cd odis-php
```

### 2. Install dependencies

```bash
composer install
```

### 3. Environment configuration

Copy `.env` to `.env.local` and configure your settings:

```bash
cp .env .env.local
```

Edit `.env.local` to match your environment, especially database and Elasticsearch settings:

```env
# Database
DATABASE_URL="sqlite:///%kernel.project_dir%/var/data.db"
# Or for PostgreSQL:
# DATABASE_URL="postgresql://user:password@127.0.0.1:5432/db_name?serverVersion=16&charset=utf8"

# Elasticsearch
ELASTICSEARCH_URL=https://localhost:9200
ELASTICSEARCH_USER=your_username
ELASTICSEARCH_PASSWORD=your_password
```

### 4. Database Setup

Create the database (skip this if using SQLite, as it's created automatically) and run migrations:

If you don't have migrations yet, you can initialize the schema with:

```bash
php bin/console doctrine:schema:create
```

```bash
# Only if using PostgreSQL:
php bin/console doctrine:database:create

# For both SQLite and PostgreSQL:
php bin/console doctrine:migrations:migrate
```

### 5. Elasticsearch Setup

Ensure Elasticsearch is running and accessible at the URL configured in your `.env`.
The application will automatically create the required index (`odis_metadata`) and mappings when you first run the crawler with the `--clear-index` option:

```bash
php bin/console app:odis:crawl --clear-index --limit 1
```

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

## Troubleshooting

### Elasticsearch: No alive nodes. All the X nodes seem to be down.

If you see this error when running the crawler or opening the search page, it means the application cannot reach Elasticsearch.

Checklist:
- Verify service is running: `curl -k https://localhost:9200` (or `http://` if you don’t use TLS). You should get a JSON response.
- Verify credentials: Check `ELASTICSEARCH_USER` and `ELASTICSEARCH_PASSWORD` match your cluster.
- Verify URL and scheme:
  - If you use self-signed TLS, either install a valid certificate or disable verification via `setSSLVerification(false)` (already configured by default in `config/services.yaml`).
  - If your ES is plain HTTP, set `ELASTICSEARCH_URL=http://host:9200`.
- Verify network/firewall: Port 9200 must be reachable from the PHP host.
- If running Docker/Compose, ensure you use the container hostname (e.g. `http://elasticsearch:9200`) from within other containers, not `localhost`.
- If ES requires a specific CA or cloud ID, configure them accordingly in the Symfony service or environment.

After fixing connectivity, re-run:
```
php bin/console app:odis:crawl --clear-index --limit 1
```
This ensures mappings are recreated correctly before indexing.
