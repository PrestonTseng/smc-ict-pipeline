from __future__ import annotations

import os
import subprocess
from pathlib import Path


def _fake_uv(tmp_path: Path, outputs: list[str]) -> tuple[Path, Path]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(parents=True)
    counter = tmp_path / "counter"
    responses = tmp_path / "responses"
    responses.write_text("\n".join(outputs) + "\n")
    uv = bin_dir / "uv"
    uv.write_text(
        "#!/usr/bin/env python3\n"
        "import os\n"
        "from pathlib import Path\n"
        "counter=Path(os.environ['FAKE_UV_COUNTER'])\n"
        "responses=Path(os.environ['FAKE_UV_RESPONSES']).read_text().splitlines()\n"
        "attempt=int(counter.read_text()) if counter.exists() else 0\n"
        "counter.write_text(str(attempt+1))\n"
        "print(responses[min(attempt, len(responses)-1)])\n"
    )
    uv.chmod(0o755)
    return bin_dir, counter


def _run_analysis_script(tmp_path: Path, outputs: list[str]):
    bin_dir, counter = _fake_uv(tmp_path, outputs)
    env = os.environ | {
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "FAKE_UV_COUNTER": str(counter),
        "FAKE_UV_RESPONSES": str(tmp_path / "responses"),
        "SMC_ICT_RETRY_DELAY": "0",
        "SMC_ICT_MAX_LOCK_ATTEMPTS": "3",
        "SMC_ICT_LOG_DIR": str(tmp_path / "logs"),
        "SMC_ICT_EVIDENCE_DIR": str(tmp_path / "evidence"),
    }
    result = subprocess.run(
        ["bash", "scripts/scheduled-analysis.sh"],
        cwd=Path(__file__).parents[1],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    return result, counter


def test_scheduled_analysis_retries_lock_collision_then_builds_casebook(tmp_path: Path):
    result, counter = _run_analysis_script(
        tmp_path,
        [
            '{"status":"SKIPPED_LOCKED"}',
            '{"status":"SKIPPED_LOCKED"}',
            '{"status":"NO_SETUP","dataset_version":"ds-x"}',
            '{"eligible_cases":20,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}',
        ],
    )
    assert result.returncode == 0
    assert counter.read_text() == "4"


def test_scheduled_analysis_reports_persistent_lock_collision(tmp_path: Path):
    result, counter = _run_analysis_script(tmp_path, ['{"status":"SKIPPED_LOCKED"}'])
    assert result.returncode != 0
    assert counter.read_text() == "3"
    assert "remained locked" in result.stderr


def test_scheduled_analysis_rejects_failed_or_unknown_success_receipt(tmp_path: Path):
    for index, receipt in enumerate(('{"status":"FAILED","error":"boom"}', '{"status":"UNKNOWN"}')):
        result, counter = _run_analysis_script(tmp_path / f"bad-analysis-status-{index}", [receipt])
        assert result.returncode != 0
        assert "invalid analysis status" in result.stderr
        assert counter.read_text() == "1"


def test_scheduled_ingest_retries_lock_collision(tmp_path: Path):
    bin_dir, counter = _fake_uv(
        tmp_path,
        [
            '{"status":"SKIPPED_LOCKED"}',
            '{"status":"COMMITTED","dataset_version":"ds-x"}',
        ],
    )
    env = os.environ | {
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "FAKE_UV_COUNTER": str(counter),
        "FAKE_UV_RESPONSES": str(tmp_path / "responses"),
        "SMC_ICT_RETRY_DELAY": "0",
        "SMC_ICT_MAX_LOCK_ATTEMPTS": "3",
        "SMC_ICT_LOG_DIR": str(tmp_path / "logs"),
    }
    result = subprocess.run(
        ["bash", "scripts/scheduled-ingest.sh"],
        cwd=Path(__file__).parents[1],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert counter.read_text() == "2"


def test_scheduled_ingest_reports_persistent_lock_collision(tmp_path: Path):
    bin_dir, counter = _fake_uv(tmp_path, ['{"status":"SKIPPED_LOCKED"}'])
    env = os.environ | {
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "FAKE_UV_COUNTER": str(counter),
        "FAKE_UV_RESPONSES": str(tmp_path / "responses"),
        "SMC_ICT_RETRY_DELAY": "0",
        "SMC_ICT_MAX_LOCK_ATTEMPTS": "3",
        "SMC_ICT_LOG_DIR": str(tmp_path / "logs"),
    }
    result = subprocess.run(
        ["bash", "scripts/scheduled-ingest.sh"],
        cwd=Path(__file__).parents[1],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
    assert counter.read_text() == "3"
    assert "remained locked" in result.stderr


def test_scheduled_wrappers_reject_invalid_retry_configuration(tmp_path: Path):
    for script in ("scheduled-ingest.sh", "scheduled-analysis.sh"):
        for index, (attempts, delay) in enumerate(
            (("0", "0"), ("-1", "0"), ("wat", "0"), ("1", "-1"))
        ):
            case = tmp_path / f"case-{script}-{index}"
            bin_dir, counter = _fake_uv(case, ["{}"])
            env = os.environ | {
                "PATH": f"{bin_dir}:{os.environ['PATH']}",
                "FAKE_UV_COUNTER": str(counter),
                "FAKE_UV_RESPONSES": str(case / "responses"),
                "SMC_ICT_MAX_LOCK_ATTEMPTS": attempts,
                "SMC_ICT_RETRY_DELAY": delay,
            }
            result = subprocess.run(
                ["bash", f"scripts/{script}"],
                cwd=Path(__file__).parents[1],
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )
            assert result.returncode != 0
            assert "invalid retry configuration" in result.stderr
            assert not counter.exists()


def test_scheduled_wrappers_reject_mixed_or_invalid_receipts(tmp_path: Path):
    for script in ("scheduled-ingest.sh", "scheduled-analysis.sh"):
        for index, receipt in enumerate(
            (
                'warning: {"status":"SKIPPED_LOCKED"}\\n{"status":"COMMITTED"}',
                '{"status":"COMMITTED"}\\n{"status":"COMMITTED"}',
                "not-json",
            )
        ):
            case = tmp_path / f"mixed-{script}-{index}"
            bin_dir, counter = _fake_uv(case, [receipt])
            env = os.environ | {
                "PATH": f"{bin_dir}:{os.environ['PATH']}",
                "FAKE_UV_COUNTER": str(counter),
                "FAKE_UV_RESPONSES": str(case / "responses"),
                "SMC_ICT_MAX_LOCK_ATTEMPTS": "3",
                "SMC_ICT_RETRY_DELAY": "0",
            }
            result = subprocess.run(
                ["bash", f"scripts/{script}"],
                cwd=Path(__file__).parents[1],
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )
            assert result.returncode != 0
            assert "invalid JSON receipt" in result.stderr
            assert counter.read_text() == "1"


def test_scheduled_analysis_rejects_invalid_casebook_receipt(tmp_path: Path):
    valid_analysis = '{"status":"NO_SETUP","dataset_version":"ds-x"}'
    invalid_receipts = (
        'warning-before-{"eligible_cases":20,"sha256":"' + "a" * 64 + '"}',
        '{"eligible_cases":20,"sha256":"' + "a" * 64 + '"}\\n{}',
        "[]",
        '{"eligible_cases":20}',
        '{"eligible_cases":true,"sha256":"' + "a" * 64 + '"}',
        '{"eligible_cases":20,"sha256":"short"}',
    )
    for index, receipt in enumerate(invalid_receipts):
        result, counter = _run_analysis_script(
            tmp_path / f"casebook-{index}", [valid_analysis, receipt]
        )
        assert result.returncode != 0
        assert "invalid JSON receipt from SMC/ICT casebook" in result.stderr
        assert counter.read_text() == "2"
