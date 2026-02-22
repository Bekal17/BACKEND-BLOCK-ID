"""
BlockID pipeline scheduler — run full pipeline daily at midnight (Asia/Jakarta) via APScheduler.

Usage:
  py -m backend_blockid.tools.pipeline_scheduler           # start scheduler (runs daily at 00:00)
  py -m backend_blockid.tools.pipeline_scheduler --run-now # run once, then exit
"""

from __future__ import annotations

import argparse
import sys

from apscheduler.schedulers.blocking import BlockingScheduler
from pytz import timezone

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

RUN_HOUR = 0
RUN_MINUTE = 0

def run_pipeline() -> int:
    """Execute the full pipeline. Returns exit code (0 = success)."""
    from backend_blockid.tools.run_full_pipeline import main

    return main()


def job_pipeline() -> None:
    """Scheduled job: log start, run pipeline, log end."""
    logger.info("pipeline_scheduler_job_start")
    try:
        exit_code = run_pipeline()
        if exit_code == 0:
            logger.info("pipeline_scheduler_job_end", success=True)
        else:
            logger.warning("pipeline_scheduler_job_end", success=False, exit_code=exit_code)
    except Exception as e:
        logger.exception("pipeline_scheduler_job_error", error=str(e))
        raise


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run BlockID full pipeline daily at midnight (Asia/Jakarta)."
    )
    parser.add_argument(
        "--run-now",
        action="store_true",
        help="Run pipeline once immediately, then exit.",
    )
    args = parser.parse_args()

    if args.run_now:
        logger.info("pipeline_scheduler_manual_run_start")
        exit_code = run_pipeline()
        logger.info("pipeline_scheduler_manual_run_end", exit_code=exit_code)
        return exit_code

    scheduler = BlockingScheduler()
    scheduler.add_job(
        job_pipeline,
        "cron",
        hour=RUN_HOUR,
        minute=RUN_MINUTE,
        timezone=timezone("Asia/Jakarta"),
        id="blockid_full_pipeline",
    )
    logger.info("pipeline_scheduler_started", run_time="00:00 Asia/Jakarta daily")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("pipeline_scheduler_stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
