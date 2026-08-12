import json
import subprocess
from pathlib import Path


def test_daily_summary_reports_active_v2_denominator_separately(tmp_path: Path):
    evidence = tmp_path / "var" / "evidence"
    evidence.mkdir(parents=True)
    casebook = {
        "schema_version": "2",
        "cases": [
            {
                "strategy_version": "v1-4h-1h-5m",
                "dataset_version": "ds-v1",
                "cutoff": 1,
                "analysis_boundary": None,
                "status": "NO_SETUP",
                "failed_gate": "smc_4h_structure",
            },
            {
                "strategy_version": "v2-1d-4h-1h",
                "dataset_version": "ds-v2",
                "cutoff": 7_199_999,
                "analysis_boundary": 7_199_999,
                "status": "NO_SETUP",
                "failed_gate": "smc_1d_regime",
            },
        ],
        "summary": {
            "eligible_runs": 2,
            "ineligible_runs": 0,
            "eligible_cases": 2,
            "unique_dataset_cutoffs": 2,
            "status_counts": {"NO_SETUP": 2},
            "failed_gate_counts": {"smc_4h_structure": 1, "smc_1d_regime": 1},
            "milestone_target": 20,
            "milestone_remaining": 18,
        },
    }
    (evidence / "casebook.json").write_text(json.dumps(casebook))
    script = Path(__file__).parents[1] / "scripts" / "daily-summary.py"

    result = subprocess.run(
        ["python3", str(script)], cwd=tmp_path, capture_output=True, text=True, check=False
    )

    assert result.returncode == 0
    assert "Active v2 eligible rows: 1/20 (remaining 19)" in result.stdout
    assert "unique closed-1H boundaries: 1" in result.stdout
    assert "legacy v1 rows preserved: 1" in result.stdout
    assert "latest v2 cutoff ms: 7199999" in result.stdout
    assert "Statuses: NO_SETUP=1" in result.stdout
    assert "Failed gates: smc_1d_regime=1" in result.stdout
    assert "smc_4h_structure" not in result.stdout
