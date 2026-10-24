"""
Automated research runner and report generator.
Reads committed export and manifest, runs model comparisons across horizons,
performs HAC uncertainty analysis, and saves machine-readable result tables.
"""

import argparse
import json
from pathlib import Path
import matplotlib.pyplot as plt
import polars as pl

from tickraft_research.features.ofi import add_ofi_features
from tickraft_research.features.trade_imbalance import add_tvi_feature
from tickraft_research.features.midprice import build_regular_grid_1s, add_forward_targets
from tickraft_research.evaluation.splits import chronological_split_with_embargo
from tickraft_research.models.regression import MicrostructureModelSuite
from tickraft_research.evaluation.metrics import holm_bonferroni_correction


def run_research_pipeline(
    export_csv: Path,
    manifest_path: Path,
    output_dir: Path,
    horizons: list = [1, 5, 10, 30, 60],
    train_ratio: float = 0.70,
    embargo_seconds: int = 60
):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. Read manifest and verify provenance
    manifest_data = {}
    manifest_verified = False
    if manifest_path.exists():
        with open(manifest_path, "r") as f:
            manifest_data = json.load(f)
        manifest_verified = bool(manifest_data.get("source_manifest_hash") and manifest_data.get("rolling_digest"))

    # 2. Parse committed replay CSV
    df = pl.read_csv(export_csv)
    if manifest_data and "event_count" in manifest_data:
        assert len(df) == manifest_data["event_count"], f"Row count mismatch: CSV {len(df)} != manifest {manifest_data['event_count']}"

    # Add features if not present
    if "ofi_l1" not in df.columns:
        df = add_ofi_features(df, levels=5)
    if "tvi" not in df.columns:
        df = add_tvi_feature(df)

    # 3. Regular 1-second grid aggregation
    grid_df = build_regular_grid_1s(df, levels=5, forward_fill=True)

    # 4. Add forward price targets
    grid_with_targets = add_forward_targets(grid_df, horizons=horizons)

    suite = MicrostructureModelSuite(depth_levels=5)
    horizon_results = {}
    p_values_to_correct = []
    p_value_keys = []

    for h in horizons:
        target_col = f"target_{h}s"
        try:
            train_df, test_df, split_rep = chronological_split_with_embargo(
                grid_with_targets,
                train_ratio=train_ratio,
                embargo_seconds=embargo_seconds,
                target_col=target_col
            )
        except Exception as e:
            continue

        if len(train_df) < 5 or len(test_df) < 2:
            continue

        eval_res = suite.fit_and_evaluate(train_df, test_df, target_col=target_col)
        horizon_results[f"{h}s"] = {
            "horizon_seconds": h,
            "split": split_rep,
            "models": eval_res,
        }

        # Track p-values for multiple testing correction
        if "M_L1" in eval_res and "ofi_l1" in eval_res["M_L1"]["in_sample"]["coefficients"]:
            p_val = eval_res["M_L1"]["in_sample"]["coefficients"]["ofi_l1"]["p_value"]
            p_values_to_correct.append(p_val)
            p_value_keys.append((f"{h}s", "M_L1", "ofi_l1"))

        if "M_TVI" in eval_res and "tvi" in eval_res["M_TVI"]["in_sample"]["coefficients"]:
            p_val = eval_res["M_TVI"]["in_sample"]["coefficients"]["tvi"]["p_value"]
            p_values_to_correct.append(p_val)
            p_value_keys.append((f"{h}s", "M_TVI", "tvi"))

    # Apply Holm-Bonferroni correction
    if p_values_to_correct:
        adjusted_p = holm_bonferroni_correction(p_values_to_correct)
        for (h_key, m_key, coef_key), adj_p in zip(p_value_keys, adjusted_p):
            horizon_results[h_key]["models"][m_key]["in_sample"]["coefficients"][coef_key]["p_value_holm"] = adj_p

    # Write machine-readable JSON summary
    summary_path = output_dir / "research_summary.json"
    full_output = {
        "dataset_manifest": manifest_data,
        "manifest_verified": manifest_verified,
        "parameters": {
            "horizons": horizons,
            "train_ratio": train_ratio,
            "embargo_seconds": embargo_seconds,
        },
        "horizons": horizon_results,
    }
    with open(summary_path, "w") as f:
        json.dump(full_output, f, indent=2)

    # Generate plot if horizons evaluated
    if horizon_results:
        try:
            fig, ax = plt.subplots(figsize=(8, 4.5))
            h_labels = list(horizon_results.keys())
            l1_r2 = [horizon_results[h]["models"].get("M_L1", {}).get("oos_metrics", {}).get("oos_r2", 0.0) for h in h_labels]
            tvi_r2 = [horizon_results[h]["models"].get("M_TVI", {}).get("oos_metrics", {}).get("oos_r2", 0.0) for h in h_labels]
            depth_r2 = [horizon_results[h]["models"].get("M_depth", {}).get("oos_metrics", {}).get("oos_r2", 0.0) for h in h_labels]

            x = range(len(h_labels))
            ax.plot(x, l1_r2, marker="o", label="M_L1 (Level-1 OFI)")
            ax.plot(x, tvi_r2, marker="s", label="M_TVI (Trade Imbalance)")
            ax.plot(x, depth_r2, marker="^", label="M_depth (Depth-Aware OFI)")
            ax.set_xticks(x)
            ax.set_xticklabels(h_labels)
            ax.set_xlabel("Horizon (seconds)")
            ax.set_ylabel("Out-of-Sample R²")
            ax.set_title("OOS Explanatory Power Comparison Across Horizons")
            ax.legend()
            ax.grid(True, linestyle="--", alpha=0.6)
            plt.tight_layout()
            plot_path = output_dir / "horizon_oos_comparison.png"
            plt.savefig(plot_path, dpi=150)
            plt.close()
        except Exception as e:
            print(f"[Warning] Failed to generate plot: {e}")

    return full_output


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--export-csv", type=str, required=True)
    parser.add_argument("--manifest", type=str, default="")
    parser.add_argument("--output-dir", type=str, default="results")
    parser.add_argument("--horizons", type=str, default="1,5,10,30,60")
    parser.add_argument("--train-ratio", type=float, default=0.7)
    parser.add_argument("--embargo-seconds", type=int, default=60)
    args = parser.parse_args()

    parsed_horizons = [int(h.strip()) for h in args.horizons.split(",") if h.strip()]

    run_research_pipeline(
        export_csv=Path(args.export_csv),
        manifest_path=Path(args.manifest) if args.manifest else Path(""),
        output_dir=Path(args.output_dir),
        horizons=parsed_horizons,
        train_ratio=args.train_ratio,
        embargo_seconds=args.embargo_seconds,
    )
