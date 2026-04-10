"""
Smoke tests for the ingestion pipeline.

Run with:  pytest tests/test_smoke.py -v
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

# Resolve paths relative to the repo root, regardless of where pytest is invoked.
REPO_ROOT = Path(__file__).parent.parent
TEST_DATA = REPO_ROOT / "test_data"
CONFIG_DIR = str(REPO_ROOT)


def _run(file_name: str, **kwargs) -> dict:
    """Helper: run the pipeline against a test_data fixture into a temp dir."""
    from pipeline import run_pipeline

    file_path = str(TEST_DATA / file_name)
    with tempfile.TemporaryDirectory() as tmp:
        return run_pipeline(
            file_paths=[file_path],
            domain="trade",
            config_dir=CONFIG_DIR,
            output_dir=tmp,
            **kwargs,
        )


# ---------------------------------------------------------------------------
# Basic pipeline success
# ---------------------------------------------------------------------------

class TestInvoicesExact:
    def test_job_status_success(self):
        summary = _run("invoices_exact.csv")
        assert summary["Job_Status"] in ("SUCCESS", "SUCCESS_WITH_EXCEPTIONS"), (
            f"Expected success, got {summary['Job_Status']}: {summary.get('Reason')}"
        )

    def test_canonical_rows_produced(self):
        summary = _run("invoices_exact.csv")
        assert summary["Total_Canonical_Rows_Written"] > 0, "No canonical rows produced"

    def test_canonical_dataframe_non_empty(self):
        summary = _run("invoices_exact.csv")
        frames = summary.get("_dataframes", {}).get("canonical", {})
        assert frames, "No canonical DataFrames in summary"
        total = sum(len(df) for df in frames.values())
        assert total > 0


class TestCustomersExact:
    def test_job_status_success(self):
        summary = _run("customers_exact.csv")
        assert summary["Job_Status"] in ("SUCCESS", "SUCCESS_WITH_EXCEPTIONS")

    def test_canonical_rows_produced(self):
        summary = _run("customers_exact.csv")
        assert summary["Total_Canonical_Rows_Written"] > 0


# ---------------------------------------------------------------------------
# Batch input (two files, one job)
# ---------------------------------------------------------------------------

class TestMultiFileBatch:
    def test_batch_job_not_failed(self):
        from pipeline import run_pipeline

        files = [
            str(TEST_DATA / "invoices_exact.csv"),
            str(TEST_DATA / "customers_exact.csv"),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            summary = run_pipeline(
                file_paths=files,
                domain="trade",
                config_dir=CONFIG_DIR,
                output_dir=tmp,
            )
        assert summary["Job_Status"] != "FAILED", summary.get("Reason")

    def test_batch_canonical_rows_from_both_files(self):
        from pipeline import run_pipeline

        files = [
            str(TEST_DATA / "invoices_exact.csv"),
            str(TEST_DATA / "customers_exact.csv"),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            summary = run_pipeline(
                file_paths=files,
                domain="trade",
                config_dir=CONFIG_DIR,
                output_dir=tmp,
            )
        assert summary["Total_Canonical_Rows_Written"] > 0

    def test_invoice_only_not_blocked_by_customer_mandatory(self):
        """An invoice-only file must not be blocked for missing customer-table mandatory fields."""
        summary = _run("invoices_exact.csv")
        # If blocked, the blocked columns would all be from TRD_CUSTOMER (or similar).
        # The job must not be BLOCKED purely because of a table that wasn't targeted.
        assert summary["Job_Status"] != "BLOCKED", (
            f"Invoice-only file was BLOCKED — likely by mandatory fields "
            f"from an untargeted table. Blocked: {summary.get('Blocked_Mandatory_Columns')}"
        )


# ---------------------------------------------------------------------------
# Output files written with UTF-8-BOM
# ---------------------------------------------------------------------------

class TestOutputEncoding:
    def test_canonical_csv_has_bom(self):
        """Canonical CSVs must start with the UTF-8 BOM so Windows tools open them correctly."""
        from pipeline import run_pipeline

        with tempfile.TemporaryDirectory() as tmp:
            summary = run_pipeline(
                file_paths=[str(TEST_DATA / "invoices_exact.csv")],
                domain="trade",
                config_dir=CONFIG_DIR,
                output_dir=tmp,
            )
            canonical_paths = summary.get("_output_files", {}).get("canonical", {})
            assert canonical_paths, "No canonical output files found"
            for tbl, path in canonical_paths.items():
                with open(path, "rb") as fh:
                    bom = fh.read(3)
                assert bom == b"\xef\xbb\xbf", (
                    f"{tbl} CSV at {path} is missing UTF-8 BOM (got {bom!r})"
                )
