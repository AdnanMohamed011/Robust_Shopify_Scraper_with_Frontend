# Product Tracker

A multi-store Shopify product tracker: scrapes collections on a schedule,
stores results in a database, and only records a history entry when a
price or stock status actually changes — which is what makes "price drop"
/ "back in stock" alerts possible.

## What's actually implemented

- Async scraper (`app/scraper.py`) — concurrent requests, retry with
  backoff, handle normalization/deduping.
- SQLite database with SQLAlchemy models (`app/models.py`) — Store,
  Collection, Product, PriceHistory.
- Change-detection ingest (`app/ingest.py`) — new/updated/unchanged
  logic, tested and verified.
- REST API (`app/main.py`) — add stores/collections, trigger scrapes,
  list products, get price history, get recent alerts.
- Scheduler (`app/scheduler.py`) — auto-scrapes every 6 hours.
- Dashboard (`static/dashboard.html`) — plain HTML/JS, no build step,
  shows products, alerts, and a price-history chart per product.

This was tested end-to-end with synthetic scrape data (see the test run
in our conversation) — insert, price-drop detection, and no-op on repeat
scrapes all verified. Live scraping against real Shopify stores couldn't
be tested from this sandbox because its network only allows a fixed set
of domains (pypi, npm, github, etc.) — it will work normally on your own
machine or a real server, exactly like your original script did.

## Running it

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Then open `http://localhost:8000` for the dashboard, or
`http://localhost:8000/docs` for the interactive API docs.

## Quick start via API

```bash
# Add a store
curl -X POST http://localhost:8000/stores \
  -H "Content-Type: application/json" \
  -d '{"name":"Sana Safinaz","base_url":"https://sanasafinaz.com"}'

# Add a collection to track (store_id from the response above)
curl -X POST http://localhost:8000/stores/1/collections \
  -H "Content-Type: application/json" \
  -d '{"handle":"unstitched-fabric-1"}'

# Trigger a scrape
curl -X POST http://localhost:8000/scrape/1
```

