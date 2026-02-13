# tap-appstoreconnect

Singer tap for Apple App Store Connect Analytics API, built with the Meltano Singer SDK.

## ✅ Status: OPERATIONAL

This tap is fully functional and tested. See [`SUCCESS.md`](SUCCESS.md) for latest run results.

## 🚀 Quick Start

**All configuration is in `meltano.yml` - no `.env` or `config.json` needed!**

```bash
# Just run it - everything is already configured!
meltano run tap-appstoreconnect target-clickhouse
```

**To change settings:**
1. Edit `meltano.yml` (has comments explaining each option)
2. Run the command above

**For detailed guides:**

- **[RUN.md](RUN.md)** - 2-minute quick start
- **[CONFIG_GUIDE.md](CONFIG_GUIDE.md)** - All configuration options explained
- **[MELTANO_USAGE.md](MELTANO_USAGE.md)** - Complete Meltano reference
- **[SUCCESS.md](SUCCESS.md)** - Latest run results

## 📚 Documentation

**Configuration & Setup:**
- **[CONFIG_GUIDE.md](CONFIG_GUIDE.md)** - All settings in meltano.yml (NEW!)
- **[RUN.md](RUN.md)** - Quick start guide
- **[MELTANO_USAGE.md](MELTANO_USAGE.md)** - Complete Meltano reference

**Troubleshooting & Status:**
- **[SUCCESS.md](SUCCESS.md)** - Current operational status
- **[BUGFIX.md](BUGFIX.md)** - Known issues and fixes
- **[PRIVATE_KEY_FIX.md](PRIVATE_KEY_FIX.md)** - JWT authentication issues
- **[SETUP_SUMMARY.md](SETUP_SUMMARY.md)** - What was changed and why
- **[INDEX.md](INDEX.md)** - Documentation navigation guide

## Features

**Data Extraction:**
- First-time downloads
- Redownloads
- Updates
- Deletions
- Total Sessions
- Total Active Devices
- Calculated metrics (Avg Sessions Per Device, User Loss Rate)

**Sync Behavior:**
- ✅ **Full table sync** (always extracts from `start_date`)
- ✅ **Overwrites target table** each run
- ✅ **Historical backfill** from any start date
- ⚡ ~45 seconds per run
- 🔁 Run anytime - always get fresh data

**Authentication:**
- JWT authentication with ES256 algorithm
- Automatic token generation and refresh
- Handles gzipped CSV segments from App Store Connect API

## Installation

### From Source (Development)

```bash
# Clone repository
cd tap-appstoreconnect

# Install dependencies using uv (recommended)
uv sync

# Or using pip
pip install -e .
```

### From PyPI (when published)

```bash
pip install tap-appstoreconnect
```

## Configuration

Create a `config.json` file with the following structure:

```json
{
  "issuer_id": "de6ee153-778d-444d-bc05-8fd0bc34de4f",
  "key_id": "W4U2LTT7BY",
  "private_key": "-----BEGIN PRIVATE KEY-----\nYOUR_PRIVATE_KEY_HERE\n-----END PRIVATE KEY-----",
  "app_id": "6463405199",
  "start_date": "2024-01-01",
  "end_date": "2024-12-31"
}
```

### Configuration Parameters

| Parameter | Required | Description | Default |
|-----------|----------|-------------|---------|
| `issuer_id` | Yes | App Store Connect API Issuer ID | - |
| `key_id` | Yes | App Store Connect API Key ID | - |
| `private_key` | Yes | Private key for JWT signing (PEM format) | - |
| `app_id` | Yes | Apple App ID (e.g., "6463405199") | - |
| `start_date` | No | Start date for data extraction (YYYY-MM-DD) | 30 days ago |
| `end_date` | No | End date for data extraction (YYYY-MM-DD) | yesterday |
| `request_timeout` | No | HTTP request timeout in seconds | 300 |

### Getting App Store Connect API Credentials

1. Log in to [App Store Connect](https://appstoreconnect.apple.com/)
2. Go to **Users and Access** > **Integrations** > **Keys**
3. Click **Generate API Key** or select existing key
4. Download the private key (`.p8` file)
5. Note down the **Issuer ID** and **Key ID**

## Usage

### Discovery Mode

Discover available streams and their schemas:

```bash
tap-appstoreconnect --config config.json --discover
```

### Sync Mode

Extract data:

```bash
# Full sync (uses dates from config)
tap-appstoreconnect --config config.json

# Incremental sync (uses state file)
tap-appstoreconnect --config config.json --state state.json

# Pipe to target (e.g., Postgres)
tap-appstoreconnect --config config.json | target-postgres --config target_config.json
```

### With Meltano

Add to your `meltano.yml`:

```yaml
plugins:
  extractors:
    - name: tap-appstoreconnect
      namespace: tap_appstoreconnect
      pip_url: tap-appstoreconnect
      executable: tap-appstoreconnect
      capabilities:
        - catalog
        - discover
        - state
      settings:
        - name: issuer_id
          kind: password
        - name: key_id
          kind: password
        - name: private_key
          kind: password
        - name: app_id
        - name: start_date
        - name: end_date
```

Then run:

```bash
meltano install
meltano run tap-appstoreconnect target-postgres
```

## Available Streams

### app_analytics

Main stream containing daily app analytics metrics.

**Primary Key:** `date`  
**Replication Method:** Incremental (based on `date`)

**Schema:**

| Field | Type | Description |
|-------|------|-------------|
| `date` | string (date) | Date of the metrics (YYYY-MM-DD) |
| `first_time_download` | integer | Number of first-time downloads |
| `redownload` | integer | Number of redownloads |
| `updates` | integer | Number of app updates |
| `deletions` | integer | Number of app deletions |
| `total_sessions` | integer | Total number of app sessions |
| `total_active_devices` | integer | Total number of active devices |
| `avg_sessions_per_device` | number | Average sessions per device |
| `user_loss_rate_percent` | number | Percentage of users lost |

## Development

### Run Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=tap_appstoreconnect

# Run specific test
pytest tests/test_core.py::TestTapAppStoreConnect::test_stream_discovery
```

### Code Quality

```bash
# Type checking
mypy tap_appstoreconnect

# Linting
ruff check tap_appstoreconnect

# Formatting
ruff format tap_appstoreconnect
```

## Architecture

The tap follows the Meltano Singer SDK architecture:

1. **Authentication**: JWT tokens are generated using ES256 algorithm
2. **Report Discovery**: Finds available analytics reports in App Store Connect
3. **Instance Fetching**: Retrieves report instances (daily data snapshots)
4. **Segment Download**: Downloads gzipped CSV segments
5. **Data Parsing**: Extracts and aggregates metrics from CSV data
6. **State Management**: Tracks last synced date for incremental updates

## Troubleshooting

### Common Issues

See [`BUGFIX.md`](BUGFIX.md) for detailed troubleshooting of common issues.

### Authentication Errors

**Problem:** JWT token generation fails
- **Solution:** Check [`PRIVATE_KEY_FIX.md`](PRIVATE_KEY_FIX.md) for proper private key format
- Verify `issuer_id`, `key_id`, and `private_key` are correct
- Ensure private key is in PEM format with proper line breaks
- Check that API key has not expired in App Store Connect

### No Data Returned

- Verify `app_id` is correct
- Check that date range has available data
- Ensure app has analytics data available in App Store Connect

### 404 Segments Not Found

**Problem:** Some report instances return 404 when fetching segments
- **Solution:** This is normal - instances may still be processing
- See [`BUGFIX.md`](BUGFIX.md) for explanation
- Tap automatically skips these and continues

### Meltano Configuration

**Problem:** `meltano run` command not working
- **Solution:** See [`MELTANO_USAGE.md`](MELTANO_USAGE.md) for complete setup guide
- Use `.env` file for credentials
- Don't use `--config` flag with `meltano run`

### Rate Limiting

The tap respects Apple's API rate limits. If you encounter rate limiting:
- Reduce the date range in your sync
- Increase `request_timeout` in config
- Wait before retrying

## Recent Fixes

- ✅ **404 segments error** - Added graceful handling for processing instances
- ✅ **Private key format** - Fixed multiline environment variable format  
- ✅ **Meltano configuration** - Created proper `.env` and `meltano.yml` setup

See [`SUCCESS.md`](SUCCESS.md) for complete list of fixes and current status.

## License

Apache License 2.0

## Support

For issues and questions, please open an issue on GitHub.
