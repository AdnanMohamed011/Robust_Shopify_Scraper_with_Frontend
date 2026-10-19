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
cd shopify-tracker
dir
pip install -r requirements.txt
python -m uvicorn app.main:app
```

Then open `http://localhost:8000` for the dashboard, or
`http://localhost:8000/docs` for the interactive API docs.

## Quick start via API

```bash
# Add a store
curl -X POST http://localhost:8000/stores \
  -H "Content-Type: application/json" \
  -d '{"name":"Sana Safinaz","base_url":"https://sanasafinaz.com"}'


curl -X POST http://localhost:8000/stores/1/collections \
  -H "Content-Type: application/json" \
  -d '{"handle":"unstitched-fabric-1"}'

# Trigger a scrape
curl -X POST http://localhost:8000/scrape/1
```

## What's NOT here yet (needed for a real multi-tenant SaaS)

Being straight about scope — this is the tracking engine, not a
billable product yet. To sell this to other people you'd still need:

1. **User accounts & auth** — right now anyone hitting the API can see
   everyone's data. You'd add a `User` model, JWT or session auth, and
   scope every query by `user_id`.
2. **Billing** — Stripe subscriptions, usage limits (e.g. free tier =
   1 store / 2 collections, paid tiers = more).
3. **Notifications** — the `/alerts` endpoint exists but nothing pushes
   it anywhere yet. Add email (SES/Resend) or a Discord/Slack webhook
   so customers get pinged instead of having to check a dashboard.
4. **Deployment** — this runs on SQLite on your machine right now. For
   production, move to Postgres (one-line change in `database.py`) and
   deploy on Railway/Render/Fly.io — all have cheap always-on tiers.
5. **Rate limiting / politeness per store** — right now every store is
   scraped independently on the same schedule. If you're tracking a lot
   of stores, you'll want to stagger scrapes and respect each store's
   rate limits more deliberately than the current fixed concurrency cap.

Items 1–4 are the real path from "working tool" to "SaaS you can charge
for." Happy to build any of them next — auth + Stripe is usually the
highest-leverage next step since it's what turns this from a personal
tool into a product.
