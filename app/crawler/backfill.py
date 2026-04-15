async def run_backfill(skip_prices: bool = False) -> dict[str, int]:
    """Execute the full historical backfill.

    Fetches markets in batches and saves each batch immediately,
    so data is preserved even if the API returns an error.
    """
    stats: dict[str, int] = {
        "events": 0,
        "tags": 0,
        "markets": 0,
        "price_snapshots": 0,
        "price_errors": 0,
    }

    async with httpx.AsyncClient() as http:
        # ── Tags (fast, do first) ───────────────────────────────
        logger.info("Backfilling tags...")
        tags = await gamma.fetch_tags(http)
        async with get_session_factory()() as session:
            stats["tags"] = await upsert_tags(session, tags)
            await session.commit()
        logger.info("Upserted %d tags", stats["tags"])

        # ── Markets (fetch and save in batches) ─────────────────
        logger.info("Backfilling closed markets...")
        offset = 0
        page_size = 100
        while True:
            try:
                page = await gamma.fetch_closed_markets(http, limit=page_size, offset=offset)
            except Exception as exc:
                logger.warning("Stopped fetching at offset %d: %s", offset, exc)
                break

            if not page:
                break

            # Save this batch immediately
            try:
                async with get_session_factory()() as session:
                    count = await upsert_markets(session, page)
                    await session.commit()
                    stats["markets"] += count
            except Exception as exc:
                logger.error("Failed to save batch at offset %d: %s", offset, exc)

            offset += page_size

            if offset % 10000 == 0:
                logger.info("Progress: %d markets fetched and saved", stats["markets"])

        logger.info("Upserted %d markets total", stats["markets"])

        # ── Events (capped — not critical) ─────────────────────
        logger.info("Backfilling events (max 5000)...")
        events: list[dict[str, Any]] = []
        ev_offset = 0
        while ev_offset < 5000:
            try:
                page = await gamma.fetch_closed_events(http, limit=100, offset=ev_offset)
            except Exception:
                break
            if not page:
                break
            events.extend(page)
            ev_offset += 100
        async with get_session_factory()() as session:
            stats["events"] = await upsert_events(session, events)
            await session.commit()
        logger.info("Upserted %d events", stats["events"])

    logger.info("Backfill complete: %s", stats)
    return stats
