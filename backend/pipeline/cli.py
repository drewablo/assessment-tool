"""CLI for managing data pipelines and database operations.

Usage:
    python -m pipeline.cli init-db           # Create tables and PostGIS extension
    python -m pipeline.cli ingest-census     # Ingest ACS 5-Year data
    python -m pipeline.cli ingest-schools    # Ingest NCES PSS data
    python -m pipeline.cli ingest-elder-care # Ingest CMS provider data
    python -m pipeline.cli ingest-housing    # Ingest all HUD housing steps
    python -m pipeline.cli ingest-hud-property
    python -m pipeline.cli ingest-hud-tenant
    python -m pipeline.cli ingest-hud-qct
    python -m pipeline.cli ingest-all        # Run all ingestion pipelines
    python -m pipeline.cli ingest-hud-foundation --source-family ... --file ... --dataset-year ...
    python -m pipeline.cli status            # Show pipeline run status
"""

import argparse
import asyncio
import logging
import sys
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)


async def cmd_init_db():
    from db.connection import init_db
    print("Initializing database (creating tables + PostGIS extension)...")
    await init_db()
    print("Database initialized successfully.")


async def cmd_ingest_census(args):
    from pipeline.ingest_census import _ingest_acs_data_async
    states = args.states.split(",") if args.states else None
    print(f"Ingesting Census ACS {args.vintage} data...")
    result = await _ingest_acs_data_async(vintage=args.vintage, states=states)
    print(f"Done: {result}")


async def cmd_ingest_schools():
    from pipeline.ingest_schools import _ingest_pss_async
    print("Ingesting NCES PSS school data...")
    result = await _ingest_pss_async()
    print(f"Done: {result}")


async def cmd_ingest_elder_care():
    from pipeline.ingest_elder_care import _ingest_cms_async
    print("Ingesting CMS elder care data...")
    result = await _ingest_cms_async()
    print(f"Done: {result}")


async def cmd_ingest_housing():
    from pipeline.ingest_housing import _ingest_housing_all_async
    print("Ingesting HUD housing (property + tenant + qct)...")
    result = await _ingest_housing_all_async()
    print(f"Done: {result}")


async def cmd_ingest_hud_property():
    from pipeline.ingest_housing import _ingest_hud_property_async
    print("Ingesting HUD LIHTC property data...")
    result = await _ingest_hud_property_async()
    print(f"Done: {result}")


async def cmd_ingest_hud_tenant():
    from pipeline.ingest_housing import _ingest_hud_tenant_async
    print("Ingesting HUD LIHTC tenant data...")
    result = await _ingest_hud_tenant_async()
    print(f"Done: {result}")


async def cmd_ingest_hud_qct():
    from pipeline.ingest_housing import _ingest_hud_qct_async
    print("Ingesting HUD QCT/DDA data...")
    result = await _ingest_hud_qct_async()
    print(f"Done: {result}")


async def cmd_ingest_all(args):
    await cmd_ingest_census(args)
    await cmd_ingest_schools()
    await cmd_ingest_elder_care()
    await cmd_ingest_housing()
    print("All pipelines complete.")


async def cmd_ingest_hud_foundation(args):
    from pathlib import Path

    from db.connection import async_session_factory
    from pipeline.ingest_hud_foundation import (
        build_deterministic_joins,
        create_ingest_run,
        normalize_lihtc_property,
        normalize_lihtc_tenant,
        normalize_qct_dda,
        snapshot_raw_source,
        validate_snapshot_contract,
    )

    source_file = Path(args.file)
    async with async_session_factory() as session:
        run = await create_ingest_run(
            session,
            source_family=args.source_family,
            source_identifier=args.source_identifier,
            dataset_year=args.dataset_year,
            source_version=args.source_version,
        )
        snap = await snapshot_raw_source(
            session,
            run=run,
            source_uri=args.source_identifier,
            source_file=source_file,
            schema_version=args.schema_version,
        )
        errors = await validate_snapshot_contract(session, snap)
        if errors:
            run.status = "failed"
            run.error_message = "; ".join(errors)
            await session.commit()
            print(f"Validation failed: {errors}")
            return

        if args.source_family == "lihtc_property":
            loaded = await normalize_lihtc_property(session, snap, dataset_year=args.dataset_year)
        elif args.source_family == "lihtc_tenant":
            loaded = await normalize_lihtc_tenant(session, snap, dataset_year=args.dataset_year)
        else:
            loaded = await normalize_qct_dda(session, snap)

        joins = await build_deterministic_joins(session, designation_year=args.dataset_year)
        run.status = "success"
        run.records_seen = loaded
        run.records_loaded = loaded
        run.finished_at = run.finished_at or datetime.now(timezone.utc).replace(tzinfo=None)
        run.metadata_json = {"joins_inserted": joins}
        await session.commit()

    print({"loaded": loaded, "joins_inserted": joins})


async def cmd_status():
    from db.connection import async_session_factory
    from pipeline.base import get_latest_run
    from sqlalchemy import select, func
    from db.models import PipelineRun, CensusTract, CompetitorSchoolRecord, CompetitorElderCare, CompetitorHousing

    async with async_session_factory() as session:
        # Table counts
        for model, label in [
            (CensusTract, "Census Tracts"),
            (CompetitorSchoolRecord, "Schools"),
            (CompetitorElderCare, "Elder Care Facilities"),
            (CompetitorHousing, "Housing Projects"),
        ]:
            result = await session.execute(select(func.count()).select_from(model))
            count = result.scalar()
            print(f"  {label}: {count:,} records")

        print()

        # Latest pipeline runs
        pipelines = ["census_acs", "nces_pss", "cms_elder_care", "hud_lihtc_property", "hud_lihtc_tenant", "hud_qct_dda"]
        for name in pipelines:
            run = await get_latest_run(session, name)
            if run:
                print(f"  {name}: last success at {run.finished_at}, "
                      f"{run.records_processed or 0} processed, "
                      f"{run.records_inserted or 0} inserted")
            else:
                print(f"  {name}: no successful runs")


def main():
    parser = argparse.ArgumentParser(description="Feasibility Platform Data Pipeline CLI")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("init-db", help="Initialize database schema")

    census_parser = sub.add_parser("ingest-census", help="Ingest Census ACS data")
    census_parser.add_argument("--vintage", default="2022", help="ACS vintage year")
    census_parser.add_argument("--states", default=None, help="Comma-separated state FIPS codes")

    sub.add_parser("ingest-schools", help="Ingest NCES PSS data")
    sub.add_parser("ingest-elder-care", help="Ingest CMS provider data")
    sub.add_parser("ingest-housing", help="Ingest all HUD housing datasets")
    sub.add_parser("ingest-hud-property", help="Ingest HUD LIHTC property ZIP")
    sub.add_parser("ingest-hud-tenant", help="Ingest HUD LIHTC tenant workbook")
    sub.add_parser("ingest-hud-qct", help="Ingest HUD QCT/DDA workbook")

    all_parser = sub.add_parser("ingest-all", help="Run all ingestion pipelines")
    all_parser.add_argument("--vintage", default="2022")
    all_parser.add_argument("--states", default=None)

    sub.add_parser("status", help="Show pipeline and data status")

    hud_parser = sub.add_parser("ingest-hud-foundation", help="Ingest HUD raw snapshot and normalize")
    hud_parser.add_argument("--source-family", required=True, choices=["lihtc_property", "lihtc_tenant", "qct_dda"])
    hud_parser.add_argument("--source-identifier", required=True, help="Source URL or file identifier")
    hud_parser.add_argument("--file", required=True, help="Path to local raw source file")
    hud_parser.add_argument("--dataset-year", required=True, type=int)
    hud_parser.add_argument("--source-version", default=None)
    hud_parser.add_argument("--schema-version", default=None)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "init-db":
        asyncio.run(cmd_init_db())
    elif args.command == "ingest-census":
        asyncio.run(cmd_ingest_census(args))
    elif args.command == "ingest-schools":
        asyncio.run(cmd_ingest_schools())
    elif args.command == "ingest-elder-care":
        asyncio.run(cmd_ingest_elder_care())
    elif args.command == "ingest-housing":
        asyncio.run(cmd_ingest_housing())
    elif args.command == "ingest-all":
        asyncio.run(cmd_ingest_all(args))
    elif args.command == "ingest-hud-property":
        asyncio.run(cmd_ingest_hud_property())
    elif args.command == "ingest-hud-tenant":
        asyncio.run(cmd_ingest_hud_tenant())
    elif args.command == "ingest-hud-qct":
        asyncio.run(cmd_ingest_hud_qct())
    elif args.command == "status":
        asyncio.run(cmd_status())
    elif args.command == "ingest-hud-foundation":
        asyncio.run(cmd_ingest_hud_foundation(args))


if __name__ == "__main__":
    main()
