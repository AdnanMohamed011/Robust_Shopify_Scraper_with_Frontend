        groups.setdefault(_fallback_handle(p), []).append(p)

    if limit is not None:
        # Trim to the first N *groups* (not raw rows) so a "safe first test"
        # sync still creates whole products with all their variants, rather
        # than half-creating one.
        groups = dict(list(groups.items())[:limit])

    domain = store.shopify_domain

    # Refuse to sync with an unresolved FX rate rather than silently
    # falling back to 1.0 -- "or 1.0" here used to mean a store whose
    # currency was never successfully detected (or the target currency
    # settings changed since) would get its raw source-currency prices
    # pushed as if they were already in the target currency, e.g. a real
    # 15,000 PKR item landing on the live Shopify store as "$15,000".
    # fx_rate_to_target IS correctly 1.0 (not None) for a store that
    # genuinely already sells in the target currency -- see
    # scraper.fetch_fx_rate's own from_currency == to_currency check --
    # so this only blocks the actual unresolved case, not a same-currency
    # store working as intended.
    settings = db.query(AppSettings).filter(AppSettings.id == 1).first()
    target_currency = settings.target_currency if settings else "USD"
    if store.currency and store.currency != target_currency and not store.fx_rate_to_target:
        msg = (
            f"Exchange rate for {store.currency} -> {target_currency} hasn't resolved yet -- "
            f"sync refused rather than pushing raw {store.currency} numbers as {target_currency}. "
            f"Should clear up on the next scheduled scrape; if it keeps happening, this store's "
            f"/cart.json may be unreachable or blocked from this server."
        )
        logger.error(f"Sync blocked for store {store_id} ({store.name}): {msg}")
        store.last_sync_status = "error"
        store.last_sync_summary = msg
        store.last_sync_at = datetime.now(timezone.utc)
        db.commit()
        summary["errors"] += 1
        _finish_progress(store_id, "error")
        return summary

    fx_rate = store.fx_rate_to_target or 1.0

    _init_progress(store_id, len(groups))

    async with aiohttp.ClientSession() as session:
        try:
            token = await _ensure_access_token(session, store, db)
            location_id = await sc.get_primary_location_id(session, domain, token)
            publication_id = await sc.get_online_store_publication_id(session, domain, token)

            # Every product synced from this source store gets added to a
            # collection named after the store (e.g. "Charizma") -- without
            # this, products were created and published fine but sat with
            # zero collection membership, so the storefront's per-brand
            # pages ("Shop by Brand" -> Charizma) showed "No products
            # found" despite the products genuinely existing and being
            # live. Resolved once per run and cached on the store row so
            # future syncs skip the lookup entirely.
            collection_id = store.dest_collection_id
            if not collection_id:
                collection_id = await sc.get_or_create_collection(session, domain, token, store.name)
                store.dest_collection_id = collection_id
                db.commit()
        except sc.ShopifyAPIError as e:
            logger.error(f"Sync setup failed for store {store_id} ({store.name}): {e}")
            store.last_sync_status = "error"
            store.last_sync_summary = str(e)
            store.last_sync_at = datetime.now(timezone.utc)
            db.commit()
            summary["errors"] += 1
            _finish_progress(store_id, "error")
            return summary

        for handle, variants in groups.items():
            group_counts = await _sync_one_group(
                session, domain, token, location_id, publication_id, collection_id,
                store, variants, handle, fx_rate, db,
            )
            for k, v in group_counts.items():
                summary[k] += v
            _advance_progress(store_id, handle, group_counts)

    total = summary["matched"] + summary["created"] + summary["not_found"] + summary["errors"]
    store.last_sync_at = datetime.now(timezone.utc)
    store.last_sync_status = "ok" if summary["errors"] == 0 else "error"
    store.last_sync_summary = (
        f"{summary['matched']} updated, {summary['created']} created, "
        f"{summary['not_found']} not found, {summary['errors']} errors (of {total})"
    )
    db.commit()
    _finish_progress(store_id, "done")
    return summary


async def sync_store_standalone(store_id: int, limit: int | None = None) -> dict:
    """Same as sync_store, but opens and closes its own DB session --
    used by the manual /stores/{id}/sync endpoint, which runs as a bare
    background task with no request-scoped session to reuse.

    Unlike the scheduled path in main.py (which logs its own completion
    line), nothing else logs the result of a manual sync -- add it here so
    a manual test run shows up in the terminal instead of finishing
    silently with no visible confirmation either way."""
    db = SessionLocal()
    try:
        summary = await sync_store(db, store_id, limit=limit)
        logger.info(f"Manual sync complete for store {store_id}: {summary}")
        return summary
    except Exception:
        logger.exception(f"Manual sync crashed for store {store_id}")
        _finish_progress(store_id, "error")
        raise
    finally:
        db.close()