"""Celery application for scheduled data refresh pipelines."""

import os

from celery import Celery
from celery.schedules import crontab

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

celery_app = Celery(
    "feasibility_pipeline",
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=[
        "pipeline.ingest_census",
        "pipeline.ingest_schools",
        "pipeline.ingest_elder_care",
        "pipeline.ingest_housing",
        "pipeline.ingest_hud_section202",
    ],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="US/Eastern",
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_soft_time_limit=3600,   # 1 hour soft limit
    task_time_limit=7200,        # 2 hour hard limit
)

celery_app.conf.beat_schedule = {
    # ACS 5-Year data: released annually in December; refresh in January
    "refresh-census-acs": {
        "task": "pipeline.ingest_census.ingest_acs_data",
        "schedule": crontab(month_of_year="1", day_of_month="15", hour="2", minute="0"),
        "args": (),
    },
    # NCES PSS: released biennially; check monthly, pipeline is idempotent
    "refresh-nces-pss": {
        "task": "pipeline.ingest_schools.ingest_pss_data",
        "schedule": crontab(month_of_year="*/3", day_of_month="1", hour="3", minute="0"),
        "args": (),
    },
    # CMS Provider Data: updated quarterly
    "refresh-cms-providers": {
        "task": "pipeline.ingest_elder_care.ingest_cms_data",
        "schedule": crontab(month_of_year="*/3", day_of_month="10", hour="4", minute="0"),
        "args": (),
    },
    # HUD LIHTC: updated annually
    "refresh-hud-lihtc": {
        "task": "pipeline.ingest_housing.ingest_lihtc_data",
        "schedule": crontab(month_of_year="3", day_of_month="1", hour="5", minute="0"),
        "args": (),
    },
    # HUD Section 202: updated infrequently; check quarterly
    "refresh-hud-section-202": {
        "task": "pipeline.ingest_hud_section202.ingest_hud_section202",
        "schedule": crontab(month_of_year="*/3", day_of_month="15", hour="5", minute="30"),
        "args": (),
    },
}
