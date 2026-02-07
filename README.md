# Polymarket Trading System

A comprehensive data pipeline and analysis system for Polymarket trading data. This system collects market information, order-filled events, and processes them into structured trade data for quantitative analysis.

## Overview

This pipeline performs three main operations:

1. **Market Data Collection** - Fetches all Polymarket markets with metadata
2. **Order Event Scraping** - Collects order-filled events from Goldsky subgraph
3. **Trade Processing** - Transforms raw order events into structured trade data

## Google Colab Setup

### First-Time Setup (Push to GitHub)

Before using the notebook in Colab, push your code to GitHub:

```bash
cd /path/to/spread
git init
git add .
git commit -m "Initial commit"
git branch -M main
git remote add origin https://github.com/Sageder/spread.git
git push -u origin main
```

### Quick Start

The notebook is designed to work seamlessly with Google Colab without manual file uploads:

1. **Upload** `spread.ipynb` to Google Colab
2. **Run Section 0 cells** - This will automatically:
   - Clone the repository from GitHub
   - Install all dependencies
   - Verify utility imports
3. **Continue with Section 1 onwards** as normal

### Private Repositories

If your repository is private, you'll need a GitHub Personal Access Token:

1. Generate token at: https://github.com/settings/tokens
2. In the notebook's Section 0, Cell 1, uncomment and set:
   ```python
   GITHUB_TOKEN = "ghp_your_token_here"
   REPO_URL = f"https://{GITHUB_TOKEN}@github.com/Sageder/spread.git"
   ```

### First-Time Setup

- Authenticate with GCP (cell in Section 2) to access data stored in GCS bucket: `spread-eth-oxford`
- Data files (markets, trades) will be downloaded automatically from GCS

### Repository Structure

```
spread/
├── notebook.ipynb              # Main analysis notebook (Colab-ready)
├── poly_utils/                 # Market data utilities
│   └── utils.py               # Market loading & token update functions
├── update_utils/               # Data pipeline modules
│   ├── update_markets.py      # Polymarket API data fetching
│   ├── update_goldsky.py      # Goldsky GraphQL scraping
│   └── process_live.py        # Trade data processing
├── backtrader_plotting/        # Custom plotting library
│   ├── schemes/               # Plotting themes (Blackly, Tradimo)
│   ├── bokeh/                 # Bokeh integration
│   └── analyzer_tables/       # Performance analyzers
├── pyproject.toml             # Project dependencies
└── update_all.py              # Batch runner for data pipeline
```

### Development Workflow

1. Make changes to utility modules locally
2. Commit and push to GitHub
3. In Colab: Re-run Section 0 to pull latest changes
4. **No need to re-upload files!**

### Data Directories (Auto-Generated)

These directories are created automatically and excluded from git:

- `data/` - Market parquet files (downloaded from GCS)
- `goldsky/` - Raw order data from subgraph (downloaded from GCS)
- `processed/` - Processed trade parquet files (downloaded from GCS)

## Local Installation

This project uses [UV](https://docs.astral.sh/uv/) for fast, reliable package management.

### Install UV

```bash
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# Or with pip
pip install uv
```

### Install Dependencies

```bash
# Install all dependencies
uv sync

# Install with development dependencies (Jupyter, etc.)
uv sync --extra dev
```

## Quick Start (Local)

```bash
# Run with UV (recommended)
uv run python update_all.py

# Or activate the virtual environment first
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
python update_all.py
```

This will sequentially run all three pipeline stages:
- Update markets from Polymarket API
- Update order-filled events from Goldsky
- Process new orders into trades

## Data Files

### markets.parquet
Market metadata including:
- Market question, outcomes, and tokens
- Creation/close times and slugs
- Trading volume and condition IDs
- Negative risk indicators

**Fields**: `createdAt`, `id`, `question`, `answer1`, `answer2`, `neg_risk`, `market_slug`, `token1`, `token2`, `condition_id`, `volume`, `ticker`, `closedTime`

### goldsky/orderFilled.parquet
Raw order-filled events with:
- Maker/taker addresses and asset IDs
- Fill amounts and transaction hashes
- Unix timestamps

**Fields**: `timestamp`, `maker`, `makerAssetId`, `makerAmountFilled`, `taker`, `takerAssetId`, `takerAmountFilled`, `transactionHash`

### processed/trades.parquet
Structured trade data including:
- Market ID mapping and trade direction
- Price, USD amount, and token amount
- Maker/taker roles and transaction details

**Fields**: `timestamp`, `market_id`, `maker`, `taker`, `nonusdc_side`, `maker_direction`, `taker_direction`, `price`, `usd_amount`, `token_amount`, `transactionHash`

## Pipeline Stages

### 1. Update Markets (`update_markets.py`)

Fetches all markets from Polymarket API in chronological order.

**Features**:
- Automatic resume from last offset (idempotent)
- Rate limiting and error handling
- Batch fetching (500 markets per request)

**Usage**:
```bash
uv run python -c "from update_utils.update_markets import update_markets; update_markets()"
```

### 2. Update Goldsky (`update_goldsky.py`)

Scrapes order-filled events from Goldsky subgraph API.

**Features**:
- Resumes from last cursor state automatically
- Handles GraphQL queries with pagination
- Deduplicates events

**Usage**:
```bash
uv run python -c "from update_utils.update_goldsky import update_goldsky; update_goldsky()"
```

### 3. Process Live Trades (`process_live.py`)

Processes raw order events into structured trades.

**Features**:
- Maps asset IDs to markets using token lookup
- Calculates prices and trade directions
- Identifies BUY/SELL sides
- Handles missing markets by discovering them from trades
- Incremental processing from last checkpoint

**Usage**:
```bash
uv run python -c "from update_utils.process_live import process_live; process_live()"
```

## Dependencies

Dependencies are managed via `pyproject.toml` and installed automatically with `uv sync`.

**Key Libraries**:
- `polars` - Fast DataFrame operations
- `pandas` - Data manipulation
- `gql` - GraphQL client for Goldsky
- `requests` - HTTP requests to Polymarket API
- `flatten-json` - JSON flattening for nested responses
- `backtrader` - Backtesting framework
- `matplotlib` - Plotting and visualization

## Features

### Resumable Operations
All stages automatically resume from where they left off:
- **Markets**: Tracks offset for pagination
- **Goldsky**: Saves cursor state to `goldsky/cursor_state.json`
- **Processing**: Finds last processed transaction hash

### Error Handling
- Automatic retries on network failures
- Rate limit detection and backoff
- Server error (500) handling
- Graceful fallbacks for missing data

### Missing Market Discovery
The processing stage automatically discovers markets that weren't in the initial dataset and fetches them via the Polymarket API.

## Analysis Examples

### Loading Data (Colab/Jupyter)

```python
import polars as pl
from poly_utils.utils import get_markets, PLATFORM_WALLETS

# Load markets
markets_df = pl.read_parquet("data/markets.parquet")

# Load trades
trades_df = pl.read_parquet("data/trades.parquet")
```

### Filtering by Market

```python
# Search for markets by keyword
trump_markets = markets_df.filter(
    pl.col('question').str.contains('Trump', literal=False)
).sort('volume', descending=True)

# Get trades for specific market
market_trades = trades_df.filter(
    pl.col('market_id') == '0xdd22472e552920b8438158ea7238bfadfa4f736aa4cee91a6b86c39ead110917'
)
```

### Platform Wallet Analysis

```python
# Platform wallets are pre-defined
print(PLATFORM_WALLETS)

# Filter trades excluding platform
non_platform_trades = trades_df.filter(
    ~pl.col('maker').is_in(PLATFORM_WALLETS)
)
```

## Notes

- All amounts are normalized to standard decimal format (divided by 10^6)
- Timestamps are in UTC
- Platform wallets are tracked in `poly_utils/utils.py`
- Negative risk markets are flagged in the market data
- GCS bucket `spread-eth-oxford` contains pre-processed data snapshots

## Troubleshooting

**Issue**: Markets not found during processing
**Solution**: Run `update_markets()` first, or let `process_live()` auto-discover them

**Issue**: GCS authentication fails in Colab
**Solution**: Re-run the GCS authentication cell in Section 2

**Issue**: Import errors in Colab
**Solution**: Ensure Section 0 completed successfully - check that repository was cloned

**Issue**: Rate limiting from Polymarket API
**Solution**: The pipeline handles this automatically with exponential backoff

## License

Go wild with it
