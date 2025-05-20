"""
Utility module for tracking cron job execution in Supabase.
"""

import os
import time
from typing import Any, Dict, Optional, Tuple, Union


def start_job(supabase_client, job_name: str) -> Tuple[Optional[str], bool]:
    """
    Start tracking a cron job run.

    Args:
        supabase_client: Initialized Supabase client
        job_name: Name of the job (e.g., "singapore_caselaw_scraper")

    Returns:
        Tuple of (job_run_id, success)
    """
    try:
        start_time = time.strftime("%Y-%m-%d %H:%M:%S%z")
        response = (
            supabase_client.table("cron_job_runs")
            .insert(
                {
                    "job_name": job_name,
                    "start_time": start_time,
                    "status": "started",
                    "new_cases_found": 0,
                    "pages_processed": 0,
                }
            )
            .execute()
        )

        if response.data:
            job_run_id = response.data[0]["id"]
            print(f"Cron job '{job_name}' started. Run ID: {job_run_id}")
            return job_run_id, True
        else:
            print(f"Error: Could not create cron_job_runs record for {job_name}.")
            return None, False
    except Exception as e:
        print(f"Error starting cron job tracking: {e}")
        return None, False


def complete_job(supabase_client, job_run_id: str, metrics: Dict[str, Any]) -> bool:
    """
    Mark a job as completed with metrics.

    Args:
        supabase_client: Initialized Supabase client
        job_run_id: ID of the job run from start_job
        metrics: Dictionary of metrics (e.g., {"new_cases_found": 10, "pages_processed": 5})

    Returns:
        Success status (bool)
    """
    if not job_run_id:
        print("Warning: Cannot complete job, no job_run_id provided.")
        return False

    try:
        update_data = {
            "end_time": time.strftime("%Y-%m-%d %H:%M:%S%z"),
            "status": "completed",
            **metrics,
        }

        response = (
            supabase_client.table("cron_job_runs")
            .update(update_data)
            .eq("id", job_run_id)
            .execute()
        )

        return bool(response.data)
    except Exception as e:
        print(f"Error completing cron job: {e}")
        return False


def fail_job(
    supabase_client, job_run_id: str, metrics: Dict[str, Any], error_message: str
) -> bool:
    """
    Mark a job as failed with error details.

    Args:
        supabase_client: Initialized Supabase client
        job_run_id: ID of the job run from start_job
        metrics: Dictionary of metrics (e.g., {"new_cases_found": 10, "pages_processed": 5})
        error_message: Description of the error that caused the failure

    Returns:
        Success status (bool)
    """
    if not job_run_id:
        print("Warning: Cannot fail job, no job_run_id provided.")
        return False

    try:
        update_data = {
            "end_time": time.strftime("%Y-%m-%d %H:%M:%S%z"),
            "status": "failed",
            "error_message": error_message,
            **metrics,
        }

        response = (
            supabase_client.table("cron_job_runs")
            .update(update_data)
            .eq("id", job_run_id)
            .execute()
        )

        return bool(response.data)
    except Exception as e:
        print(f"Error marking cron job as failed: {e}")
        return False
