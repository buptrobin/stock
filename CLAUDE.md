# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Python-based stock and fund price tracking system that integrates with Feishu (Lark) Bitable to manage investment portfolio data. The project fetches real-time prices for US stocks and Chinese funds.

## Development Setup

This project requires Python >=3.13 and uses `uv` as the package manager.

### Running the Application

Run the main entry point:
```bash
python main.py
```

Run the Feishu Bitable integration (fetches prices from Bitable records):
```bash
python feishu_bitable.py
```

Or with uv:
```bash
uv run python feishu_bitable.py
```

### Installing Dependencies

```bash
uv sync
```

## Architecture

### Core Components

**`feishu_bitable.py`** - Main integration module with two key responsibilities:

1. **Feishu Bitable API Client** (`FeishuBitable` class):
   - Authenticates with Feishu API using app credentials
   - CRUD operations for Bitable records (search, add, batch add, update, delete)
   - Base API endpoint: `https://open.feishu.cn/open-apis`

2. **Price Fetching**:
   - `get_us_stock_price(ticker, exchange)`: Fetches US stock prices from tsanghi.com API (uses demo token)
   - `get_china_fund_price(fund_code)`: Fetches Chinese fund prices from Yahoo Finance API
   - Logic: numeric codes are treated as Chinese funds, alphanumeric as US stocks

### Authentication Flow

The `FeishuBitable` class requires:
- `app_id` and `app_secret`: Feishu app credentials
- `app_token`: Bitable app identifier
- `table_id`: Specific table identifier

Access token is obtained automatically on initialization via `_get_access_token()`.

### Main Execution Flow (feishu_bitable.py:195-278)

1. Initialize client with hardcoded credentials
2. Search all records in the configured Bitable
3. Extract unique stock/fund codes from "代号" field (supports list format)
4. For each code:
   - If numeric: fetch Chinese fund price via Yahoo Finance
   - If alphanumeric: fetch US stock price via tsanghi.com
5. Print prices to console

## Important Notes

- **Credentials**: App credentials are currently hardcoded in `feishu_bitable.py:198-201` (should be externalized)
- **API Limitations**: US stock API uses a demo token with usage restrictions
- **Error Handling**: Functions print errors but don't halt execution on individual failures
- **Data Format**: Feishu "代号" field expects list of objects with "text" keys
- No tests or linting configured yet
