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


To verify Elasticsearch is correctly configured and reachable:
```bash
# Using curl (from host)
curl -k -u "$ELASTICSEARCH_USER:$ELASTICSEARCH_PASSWORD" "$ELASTICSEARCH_URL"

# Should return a JSON with "tagline": "You Know, for Search"
```

The application will automatically create the required index (`odis_metadata`) and mappings when you first run the crawler if you haven't initialized it yet.

**Important**:

To avoid Out-of-Memory (OOM) issues during index creation on some servers,
do not use the crawler's `--clear-index` option for initial setup.
Instead, use the dedicated management command:

```bash
# Initialize the index and mappings (safe, low memory)
php bin/console app:odis:index:init
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

# Initialize or Recreate Elasticsearch index only
php bin/console app:odis:index:init
php bin/console app:odis:index:init --recreate

# Export index mappings to a JSON file (useful for manual curl setup)
php bin/console app:odis:index:export mappings.json
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

#### Step 1: Verify service health (CLI)
Run this from the same host to see if the service itself is up:
```bash
curl -k -u "user:pass" "https://localhost:9200"
```
Expect a JSON response with `"tagline": "You Know, for Search"`. If this fails, the issue is at the OS/Network/Docker level.

#### Step 2: Verify PHP Environment Variables
Symfony might be reading a different configuration than you expect. Check which `.env` files are loaded:
```bash
php bin/console debug:dotenv
```
Verify the exact values Symfony is using for Elasticsearch:
```bash
php bin/console debug:container --env-vars | grep ELASTICSEARCH_
```

#### Step 3: Check for Common Issues
- **Protocol Mismatch**: Using `https://` in `.env.local` when the server only supports `http://` (or vice-versa) is a very common cause of "No alive nodes".
- **Port/Host**: Ensure port 9200 is open and the hostname is correct (use `elasticsearch` instead of `localhost` if running inside Docker Compose).
- **Credentials**: Double-check `ELASTICSEARCH_USER` and `ELASTICSEARCH_PASSWORD`.

After fixing connectivity, re-run:
```bash
php bin/console app:odis:index:init --recreate
```
This ensures mappings are correctly applied.
