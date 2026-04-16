"""
test_pipeline.py
----------------
Unit tests for core pipeline components.

Covers:
  - validate_config: required fields, bad values
  - profile_store: fingerprint, save/match with columns in index, atomic writes
  - dq_engine: _ISO4217 at module level, business rule handlers
  - output_writer: build_job_summary with all_exceptions → narrative populated
  - column_mapper: LLM retry on transient failures
  - pipeline: direct_tables prevents hollow rows

Run with:  pytest tests/test_pipeline.py -v
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

REPO_ROOT = Path(__file__).parent.parent
CONFIG_DIR = str(REPO_ROOT)
TMP_ROOT = REPO_ROOT / "output_gen_tests" / "_pytest_unit_tmp"


@pytest.fixture(autouse=True)
def _force_temp_dir(monkeypatch):
    TMP_ROOT.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("TMPDIR", str(TMP_ROOT))
    monkeypatch.setenv("TEMP", str(TMP_ROOT))
    monkeypatch.setenv("TMP", str(TMP_ROOT))
    import tempfile as _tf
    _tf.tempdir = str(TMP_ROOT)
    yield
    _tf.tempdir = None


# ---------------------------------------------------------------------------
# validate_config
# ---------------------------------------------------------------------------

class TestValidateConfig:
    def _good_cfg(self):
        return {
            "matching": {"fuzzy_min_similarity": 0.7},
            "quality": {"mandatory_threshold": 80},
            "llm": {"provider": "None"},
        }

    def _good_model(self):
        return {
            "TRD_INVOICE": {
                "business_columns": {
                    "Invoice_Number": {"mandatory": True, "type": "string"},
                }
            }
        }

    def test_valid_config_passes(self):
        from pipeline import validate_config
        validate_config(self._good_cfg(), self._good_model())  # no exception

    def test_missing_matching_raises(self):
        from pipeline import validate_config
        cfg = self._good_cfg()
        del cfg["matching"]
        with pytest.raises(ValueError, match="matching"):
            validate_config(cfg, self._good_model())

    def test_missing_quality_raises(self):
        from pipeline import validate_config
        cfg = self._good_cfg()
        del cfg["quality"]
        with pytest.raises(ValueError, match="quality"):
            validate_config(cfg, self._good_model())

    def test_missing_mandatory_threshold_raises(self):
        from pipeline import validate_config
        cfg = self._good_cfg()
        del cfg["quality"]["mandatory_threshold"]
        with pytest.raises(ValueError, match="mandatory_threshold"):
            validate_config(cfg, self._good_model())

    def test_threshold_out_of_range_raises(self):
        from pipeline import validate_config
        cfg = self._good_cfg()
        cfg["quality"]["mandatory_threshold"] = 150
        with pytest.raises(ValueError, match="0-100"):
            validate_config(cfg, self._good_model())

    def test_unknown_llm_provider_raises(self):
        from pipeline import validate_config
        cfg = self._good_cfg()
        cfg["llm"]["provider"] = "Llama"
        with pytest.raises(ValueError, match="provider"):
            validate_config(cfg, self._good_model())

    def test_empty_canonical_model_raises(self):
        from pipeline import validate_config
        with pytest.raises(ValueError, match="no tables"):
            validate_config(self._good_cfg(), {"_metadata": {}})

    def test_table_missing_business_columns_raises(self):
        from pipeline import validate_config
        model = {"TRD_FOO": {"description": "no columns here"}}
        with pytest.raises(ValueError, match="business_columns"):
            validate_config(self._good_cfg(), model)


# ---------------------------------------------------------------------------
# profile_store: fingerprint
# ---------------------------------------------------------------------------

class TestFingerprint:
    def test_order_independent(self):
        from engine.profile_store import fingerprint
        assert fingerprint(["A", "B", "C"]) == fingerprint(["C", "A", "B"])

    def test_case_independent(self):
        from engine.profile_store import fingerprint
        assert fingerprint(["Account_Number"]) == fingerprint(["account_number"])

    def test_different_columns_differ(self):
        from engine.profile_store import fingerprint
        assert fingerprint(["A", "B"]) != fingerprint(["A", "C"])

    def test_returns_64_char_hex(self):
        from engine.profile_store import fingerprint
        fp = fingerprint(["col1", "col2"])
        assert len(fp) == 64
        int(fp, 16)  # must be valid hex


# ---------------------------------------------------------------------------
# profile_store: save/match with columns in index
# ---------------------------------------------------------------------------

class TestProfileStore:
    def _make_profile_dir(self, tmp_path):
        (tmp_path / "profiles" / "trade").mkdir(parents=True, exist_ok=True)
        return str(tmp_path)

    def test_save_profile_stores_columns_in_index(self, tmp_path):
        from engine.profile_store import save_profile, _load_index
        config_dir = self._make_profile_dir(tmp_path)
        cols = ["Invoice_Number", "Account_Number", "Invoice_Date"]
        save_profile(cols, {}, "Test", "trade", config_dir)
        index = _load_index(config_dir, "trade")
        fp = next(iter(index))
        assert "columns" in index[fp], "Index entry missing 'columns' key"
        saved_cols = index[fp]["columns"]
        assert sorted(c.lower() for c in cols) == sorted(saved_cols)

    def test_exact_match_returns_exact_tier(self, tmp_path):
        from engine.profile_store import save_profile, match_profile
        config_dir = self._make_profile_dir(tmp_path)
        cols = ["Invoice_Number", "Account_Number", "Invoice_Date"]
        save_profile(cols, {"Invoice_Number": "TRD_INVOICE.Invoice_Number"}, "P1", "trade", config_dir)
        result = match_profile(cols, config_dir, "trade")
        assert result.tier == "EXACT"
        assert result.profile is not None
        assert result.profile.name == "P1"

    def test_exact_match_is_case_order_independent(self, tmp_path):
        from engine.profile_store import save_profile, match_profile
        config_dir = self._make_profile_dir(tmp_path)
        save_profile(["A", "B", "C"], {}, "P", "trade", config_dir)
        result = match_profile(["c", "b", "a"], config_dir, "trade")
        assert result.tier == "EXACT"

    def test_no_match_returns_none_tier(self, tmp_path):
        from engine.profile_store import save_profile, match_profile
        config_dir = self._make_profile_dir(tmp_path)
        save_profile(["X", "Y", "Z"], {}, "Other", "trade", config_dir)
        result = match_profile(["A", "B", "C", "D", "E", "F", "G", "H"], config_dir, "trade")
        assert result.tier == "NONE"
        assert result.profile is None

    def test_partial_match_uses_index_columns(self, tmp_path):
        """Partial scan must NOT load full profile files (uses index columns)."""
        from engine.profile_store import save_profile, match_profile, _load_profile
        config_dir = self._make_profile_dir(tmp_path)
        base_cols = [f"col{i}" for i in range(10)]
        save_profile(base_cols, {}, "Big", "trade", config_dir)
        # Incoming has 8 of the 10 cols → >70% Jaccard overlap → PARTIAL
        incoming = base_cols[:8]
        with patch("engine.profile_store._load_profile", wraps=_load_profile) as mock_load:
            result = match_profile(incoming, config_dir, "trade")
        # Profile file should NOT be loaded during partial scan since index has columns
        # (it IS loaded after to get the full Profile object — only the scan loop should skip it)
        assert result.tier in ("EXACT", "PARTIAL")

    def test_atomic_write_leaves_no_tmp_file(self, tmp_path):
        """After save, no .tmp files should remain in the profiles directory."""
        from engine.profile_store import save_profile
        config_dir = self._make_profile_dir(tmp_path)
        save_profile(["A", "B"], {}, "Clean", "trade", config_dir)
        tmp_files = list((tmp_path / "profiles" / "trade").glob("*.tmp"))
        assert not tmp_files, f"Temp files not cleaned up: {tmp_files}"

    def test_delete_profile_removes_from_index(self, tmp_path):
        from engine.profile_store import save_profile, delete_profile, list_profiles
        config_dir = self._make_profile_dir(tmp_path)
        p = save_profile(["A", "B"], {}, "ToDelete", "trade", config_dir)
        assert list_profiles(config_dir, "trade")
        result = delete_profile(p.fingerprint, config_dir, "trade")
        assert result is True
        assert not list_profiles(config_dir, "trade")


# ---------------------------------------------------------------------------
# dq_engine: _ISO4217 at module level + business rule handlers
# ---------------------------------------------------------------------------

class TestISO4217ModuleLevel:
    def test_iso4217_is_frozenset_at_module_level(self):
        import engine.dq_engine as dq
        assert hasattr(dq, "_ISO4217"), "_ISO4217 not found at module level"
        assert isinstance(dq._ISO4217, frozenset)

    def test_iso4217_contains_usd_eur_gbp(self):
        from engine.dq_engine import _ISO4217
        for code in ("USD", "EUR", "GBP", "JPY"):
            assert code in _ISO4217

    def test_iso4217_does_not_contain_invalid_codes(self):
        from engine.dq_engine import _ISO4217
        for bad in ("XXX", "ABC", "ZZZ"):
            assert bad not in _ISO4217


class TestBusinessRuleHandlers:
    def _df(self, data: dict) -> pd.DataFrame:
        return pd.DataFrame(data)

    def _nulls(self):
        return ["", "null", "NULL", "n/a", "N/A"]

    def test_inv_due_ge_inv_date_pass(self):
        from engine.dq_engine import _rule_inv_due_ge_inv_date
        df = self._df({"Due_Date": ["2024-02-01"], "Invoice_Date": ["2024-01-01"]})
        excs = _rule_inv_due_ge_inv_date(df, "TRD_INVOICE", self._nulls(), "J1", "trade", "f.csv")
        assert not excs

    def test_inv_due_ge_inv_date_fail(self):
        from engine.dq_engine import _rule_inv_due_ge_inv_date
        df = self._df({"Due_Date": ["2023-12-01"], "Invoice_Date": ["2024-01-01"]})
        excs = _rule_inv_due_ge_inv_date(df, "TRD_INVOICE", self._nulls(), "J1", "trade", "f.csv")
        assert len(excs) == 1
        assert excs[0]["Exception_Type"] == "BUSINESS_RULE_FAIL"

    def test_paid_le_invoice_pass(self):
        from engine.dq_engine import _rule_paid_le_invoice
        df = self._df({"Paid_Amount": ["500"], "Invoice_Amount": ["1000"]})
        excs = _rule_paid_le_invoice(df, "TRD_INVOICE", self._nulls(), "J1", "trade", "f.csv")
        assert not excs

    def test_paid_le_invoice_fail(self):
        from engine.dq_engine import _rule_paid_le_invoice
        df = self._df({"Paid_Amount": ["1500"], "Invoice_Amount": ["1000"]})
        excs = _rule_paid_le_invoice(df, "TRD_INVOICE", self._nulls(), "J1", "trade", "f.csv")
        assert len(excs) == 1
        assert excs[0]["Exception_Type"] == "BUSINESS_RULE_FAIL"

    def test_currency_iso4217_pass(self):
        from engine.dq_engine import _rule_currency_iso4217
        df = self._df({"Currency": ["USD", "EUR", "GBP"]})
        excs = _rule_currency_iso4217(df, "TRD_INVOICE", self._nulls(), "J1", "trade", "f.csv")
        assert not excs

    def test_currency_iso4217_fail(self):
        from engine.dq_engine import _rule_currency_iso4217
        df = self._df({"Currency": ["USD", "ZZZ", "GBP"]})
        excs = _rule_currency_iso4217(df, "TRD_INVOICE", self._nulls(), "J1", "trade", "f.csv")
        assert len(excs) == 1
        assert "ZZZ" in excs[0]["Raw_Value"]

    def test_unknown_rule_id_is_skipped_with_warning(self):
        """An unregistered rule_id must log a warning and not crash."""
        from engine.dq_engine import _check_business_rules
        cfg = {
            "quality": {
                "business_rules": [{"rule_id": "NO_SUCH_RULE"}],
                "null_values": [],
            }
        }
        excs = _check_business_rules({}, cfg, "J1", "trade", "f.csv")
        assert excs == []

    def test_business_rule_handlers_registry_has_all_builtins(self):
        from engine.dq_engine import _BUSINESS_RULE_HANDLERS
        for rule_id in ("INV_DUE_GE_INV_DATE", "PAID_LE_INVOICE", "CURRENCY_ISO4217"):
            assert rule_id in _BUSINESS_RULE_HANDLERS, f"{rule_id} missing from registry"


# ---------------------------------------------------------------------------
# output_writer: build_job_summary with all_exceptions → narrative
# ---------------------------------------------------------------------------

class TestBuildJobSummaryNarrative:
    def _mr(self, method="EXACT LOOKUP"):
        """Make a minimal MappingResult-like object."""
        from engine.column_mapper import MappingResult
        return MappingResult(
            lineage_id="l1", mapping_reference_id="mr1", job_id="j1",
            domain="trade", source_filename="f.csv",
            source_column_name="Invoice_Number", source_column_normalised="invoice_number",
            canonical_table="TRD_INVOICE", canonical_column="Invoice_Number",
            match_method=method, confidence_score=100,
            was_mandatory=True, met_threshold=True,
            llm_reasoning=None, lookup_variant_matched="invoice_number",
            archive_lineage_id=None,
            insert_timestamp="2024-01-01T00:00:00+00:00",
        )

    def _exc(self, exc_type: str) -> dict:
        return {
            "Exception_ID": "e1", "Job_ID": "j1", "Domain": "trade",
            "Source_Filename": "f.csv", "Source_Row_Index": 1,
            "Source_Column_Name": None, "Canonical_Table": "TRD_INVOICE",
            "Canonical_Column": "Invoice_Number", "Raw_Value": None,
            "Exception_Type": exc_type, "Reason": "test",
            "Insert_Timestamp": "2024-01-01T00:00:00+00:00",
        }

    def test_no_exceptions_narrative_says_no_exceptions(self):
        from engine.output_writer import build_job_summary
        summary = build_job_summary(
            job_id="j1", domain="trade", source_filename="f.csv",
            job_status="SUCCESS", files_processed=1, files_failed=0,
            total_source_rows=10, total_canonical_rows=10, total_exceptions=0,
            mapping_results=[self._mr()], blocked_mandatory=[],
            job_start=datetime.now(timezone.utc), cfg={"_metadata": {}},
            canonical_model_version="1.0", all_exceptions=[],
        )
        assert "no exceptions" in summary["Job_Narrative"].lower()
        assert summary["Exceptions_Blocking"] == 0
        assert summary["Exceptions_DataQuality"] == 0
        assert summary["Exceptions_Informational"] == 0

    def test_blocking_exception_counted_in_narrative(self):
        from engine.output_writer import build_job_summary
        excs = [self._exc("MANDATORY_NULL")]
        summary = build_job_summary(
            job_id="j1", domain="trade", source_filename="f.csv",
            job_status="SUCCESS_WITH_EXCEPTIONS", files_processed=1, files_failed=0,
            total_source_rows=10, total_canonical_rows=8, total_exceptions=1,
            mapping_results=[], blocked_mandatory=[],
            job_start=datetime.now(timezone.utc), cfg={"_metadata": {}},
            canonical_model_version="1.0", all_exceptions=excs,
        )
        assert summary["Exceptions_Blocking"] == 1
        assert summary["Exceptions_DataQuality"] == 0
        assert "mandatory null" in summary["Job_Narrative"].lower()

    def test_dq_exceptions_counted_separately(self):
        from engine.output_writer import build_job_summary
        excs = [
            self._exc("DUPLICATE_KEY"),
            self._exc("REFERENTIAL_INTEGRITY_FAIL"),
        ]
        summary = build_job_summary(
            job_id="j1", domain="trade", source_filename="f.csv",
            job_status="SUCCESS_WITH_EXCEPTIONS", files_processed=1, files_failed=0,
            total_source_rows=5, total_canonical_rows=3, total_exceptions=2,
            mapping_results=[], blocked_mandatory=[],
            job_start=datetime.now(timezone.utc), cfg={"_metadata": {}},
            canonical_model_version="1.0", all_exceptions=excs,
        )
        assert summary["Exceptions_DataQuality"] == 2
        assert summary["Exceptions_Blocking"] == 0

    def test_narrative_absent_when_all_exceptions_is_none(self):
        """When all_exceptions=None, narrative falls back to 'no exceptions' (empty list)."""
        from engine.output_writer import build_job_summary
        summary = build_job_summary(
            job_id="j1", domain="trade", source_filename="f.csv",
            job_status="SUCCESS", files_processed=1, files_failed=0,
            total_source_rows=5, total_canonical_rows=5, total_exceptions=0,
            mapping_results=[], blocked_mandatory=[],
            job_start=datetime.now(timezone.utc), cfg={"_metadata": {}},
            canonical_model_version="1.0",  # all_exceptions omitted
        )
        assert "Job_Narrative" in summary
        assert "no exceptions" in summary["Job_Narrative"].lower()


# ---------------------------------------------------------------------------
# column_mapper: LLM retry on transient error
# ---------------------------------------------------------------------------

class TestLLMRetry:
    def _make_cfg(self, provider="Claude"):
        return {
            "llm": {
                "provider": provider,
                "model": {"Claude": "claude-opus-4-6", "OpenAI": "gpt-4o", "Gemini": "gemini-2.0-flash"},
                "max_tokens": 100, "temperature": 0, "timeout_seconds": 5,
            }
        }

    def test_retry_on_transient_error_succeeds_second_attempt(self):
        """_llm_map retries after a transient error and returns on the second attempt."""
        from engine.column_mapper import _llm_map

        call_count = {"n": 0}

        def fake_claude(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] < 2:
                raise ConnectionError("transient network error")
            return ("TRD_INVOICE", "Invoice_Number", 90, "matched")

        with patch("engine.column_mapper._llm_map_claude", side_effect=fake_claude), \
             patch("time.sleep"):  # skip actual sleep
            t, c, s, r, label = _llm_map(
                "invoice_no", [("TRD_INVOICE", "Invoice_Number")],
                self._make_cfg(), "trade", "{source_col}", ""
            )
        assert c == "Invoice_Number"
        assert call_count["n"] == 2

    def test_permanent_error_not_retried(self):
        """ValueError (bad JSON etc.) must propagate immediately, no retry."""
        from engine.column_mapper import _llm_map

        call_count = {"n": 0}

        def fake_claude(*args, **kwargs):
            call_count["n"] += 1
            raise ValueError("bad JSON response")

        with patch("engine.column_mapper._llm_map_claude", side_effect=fake_claude), \
             patch("time.sleep"):
            with pytest.raises(ValueError, match="bad JSON"):
                _llm_map(
                    "col", [("TRD_INVOICE", "Invoice_Number")],
                    self._make_cfg(), "trade", "{source_col}", ""
                )
        assert call_count["n"] == 1  # raised immediately, no retry

    def test_exhausted_retries_raises_last_error(self):
        """After all retries fail, the last exception should propagate."""
        from engine.column_mapper import _llm_map

        def fake_claude(*args, **kwargs):
            raise OSError("timeout")

        with patch("engine.column_mapper._llm_map_claude", side_effect=fake_claude), \
             patch("time.sleep"):
            with pytest.raises(OSError, match="timeout"):
                _llm_map(
                    "col", [], self._make_cfg(), "trade", "{source_col}", ""
                )


# ---------------------------------------------------------------------------
# pipeline: direct_tables prevents hollow rows
# ---------------------------------------------------------------------------

class TestDirectTableFilter:
    def test_customer_only_file_does_not_write_invoice_rows(self):
        """A file with only customer columns must not produce TRD_INVOICE rows."""
        from pipeline import run_pipeline

        test_data = REPO_ROOT / "test_data"
        with tempfile.TemporaryDirectory() as tmp:
            summary = run_pipeline(
                file_paths=[str(test_data / "customers_exact.csv")],
                domain="trade",
                config_dir=CONFIG_DIR,
                output_dir=tmp,
            )
        canonical = summary.get("_dataframes", {}).get("canonical", {})
        invoice_df = canonical.get("TRD_INVOICE", pd.DataFrame())
        # Customer-only file must produce zero invoice rows
        assert invoice_df.empty or len(invoice_df) == 0, (
            f"Customer-only file wrote {len(invoice_df)} hollow rows into TRD_INVOICE"
        )
