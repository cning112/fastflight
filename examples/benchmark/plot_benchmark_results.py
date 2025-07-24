from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

# Set clean, readable style
plt.style.use("default")
plt.rcParams["figure.figsize"] = (12, 8)
plt.rcParams["font.size"] = 12


def load_benchmark_data(filename: str = "benchmark_results.csv") -> pd.DataFrame:
    """Load and validate benchmark data"""
    try:
        df = pd.read_csv(filename)
        # Support both old and new column names
        column_mapping = {
            "throughput_MBps": "avg_throughput_mbps",
            "type": "mode",
            "average_latency": "avg_latency_seconds",
        }

        # Rename columns if old format is detected
        for old_name, new_name in column_mapping.items():
            if old_name in df.columns and new_name not in df.columns:
                df = df.rename(columns={old_name: new_name})

        required_columns = [
            "delay_per_row",
            "concurrent_requests",
            "avg_throughput_mbps",
            "mode",
            "rows_per_batch",
            "avg_latency_seconds",
        ]

        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            print(f"Available columns: {list(df.columns)}")
            raise ValueError(f"Missing required columns: {missing_cols}")

        print(f"✅ Loaded {len(df)} benchmark records from {filename}")
        return df
    except FileNotFoundError:
        raise FileNotFoundError(f"Benchmark results file '{filename}' not found. Run benchmark first.") from None
    except Exception as e:
        raise RuntimeError(f"Error loading benchmark data: {e}") from e


def plot_performance_improvement(df: pd.DataFrame) -> None:
    """Complete throughput analysis in one comprehensive chart"""

    # Ensure plots directory exists
    output_dir = Path("plots")
    output_dir.mkdir(exist_ok=True)

    # Calculate throughput improvements
    improvements = []

    for delay in df["delay_per_row"].unique():
        for batch_size in df["rows_per_batch"].unique():
            for concurrency in df["concurrent_requests"].unique():
                sync_data = df[
                    (df["delay_per_row"] == delay)
                    & (df["rows_per_batch"] == batch_size)
                    & (df["concurrent_requests"] == concurrency)
                    & (df["mode"] == "sync")
                ]
                async_data = df[
                    (df["delay_per_row"] == delay)
                    & (df["rows_per_batch"] == batch_size)
                    & (df["concurrent_requests"] == concurrency)
                    & (df["mode"] == "async")
                ]

                if len(sync_data) == 1 and len(async_data) == 1:
                    sync_throughput = sync_data["avg_throughput_mbps"].iloc[0]
                    async_throughput = async_data["avg_throughput_mbps"].iloc[0]

                    improvement = ((async_throughput / sync_throughput) - 1) * 100

                    # Create delay label
                    if delay == 1e-6:
                        delay_label = "Low Delay (1µs per row)"
                    elif delay == 1e-5:
                        delay_label = "High Delay (10µs per row)"
                    else:
                        delay_label = f"{delay:.0e}s per row delay"

                    improvements.append(
                        {
                            "delay_per_row": delay,
                            "delay_label": delay_label,
                            "rows_per_batch": batch_size,
                            "concurrent_requests": concurrency,
                            "improvement_pct": improvement,
                            "sync_throughput": sync_throughput,
                            "async_throughput": async_throughput,
                        }
                    )

    if not improvements:
        print("⚠️ No improvement data to plot")
        return

    improvement_df = pd.DataFrame(improvements)

    # Get unique delay values
    delay_values = sorted(improvement_df["delay_per_row"].unique())
    n_delays = len(delay_values)

    # Create comprehensive plot: 2 rows x (n_delays + 1) columns
    fig = plt.figure(figsize=(7 * (n_delays + 1), 12))
    fig.suptitle("FastFlight Throughput Analysis: Async vs Sync", fontsize=18, fontweight="bold")

    # Top row: Raw throughput line plots for each delay
    for i, delay in enumerate(delay_values):
        ax_top = plt.subplot(2, n_delays + 1, i + 1)

        df_filtered = df[df["delay_per_row"] == delay]
        delay_label = f"{delay:.0e}s per row"

        sns.lineplot(
            data=df_filtered,
            x="concurrent_requests",
            y="avg_throughput_mbps",
            hue="mode",
            style="rows_per_batch",
            markers=True,
            dashes=False,
            ax=ax_top,
        )

        ax_top.set_title(f"Throughput vs Concurrency\n({delay_label})")
        ax_top.set_xlabel("Concurrent Requests")
        ax_top.set_ylabel("Throughput (MB/s)")
        ax_top.legend(title="Mode / Batch Size")
        ax_top.grid(True, alpha=0.3)

    # Bottom row: Improvement heatmaps for each delay
    for i, delay in enumerate(delay_values):
        ax_bottom = plt.subplot(2, n_delays + 1, i + 1 + n_delays + 1)

        delay_data = improvement_df[improvement_df["delay_per_row"] == delay]
        delay_label = delay_data["delay_label"].iloc[0]

        pivot_data = delay_data.pivot_table(
            values="improvement_pct", index="concurrent_requests", columns="rows_per_batch", aggfunc="mean"
        )

        sns.heatmap(
            pivot_data,
            annot=True,
            fmt=".1f",
            cmap="RdYlGn",
            center=0,
            cbar_kws={"label": "Improvement (%)"},
            ax=ax_bottom,
        )

        ax_bottom.set_title(f"{delay_label}\nAsync Improvement (%)")
        ax_bottom.set_xlabel("Rows per Batch (k)")
        ax_bottom.set_ylabel("Concurrent Requests")

    # Summary chart spanning both rows on the right
    summary_data = (
        improvement_df.groupby(["delay_label"])
        .agg({"sync_throughput": "mean", "async_throughput": "mean", "improvement_pct": "mean"})
        .reset_index()
    )

    # Use gridspec for the summary plot to span both rows
    ax_summary = plt.subplot(1, n_delays + 1, n_delays + 1)

    x_pos = np.arange(len(summary_data))
    width = 0.35

    ax_summary.bar(x_pos - width / 2, summary_data["sync_throughput"], width, label="Sync", color="orange", alpha=0.7)
    ax_summary.bar(x_pos + width / 2, summary_data["async_throughput"], width, label="Async", color="green", alpha=0.7)

    ax_summary.set_xlabel("Delay Scenario")
    ax_summary.set_ylabel("Average Throughput (MB/s)")
    ax_summary.set_title("Overall Summary\n(Green should be higher)")
    ax_summary.set_xticks(x_pos)
    ax_summary.set_xticklabels([label.replace(" (", "\n(") for label in summary_data["delay_label"]])
    ax_summary.legend()
    ax_summary.grid(True, alpha=0.3, axis="y")

    # Add value labels and improvement percentages
    for i, (sync_val, async_val, improvement) in enumerate(
        zip(
            summary_data["sync_throughput"],
            summary_data["async_throughput"],
            summary_data["improvement_pct"],
            strict=False,
        )
    ):
        ax_summary.text(
            i - width / 2,
            sync_val + max(summary_data["sync_throughput"]) * 0.02,
            f"{sync_val:.0f}",
            ha="center",
            va="bottom",
            fontweight="bold",
        )
        ax_summary.text(
            i + width / 2,
            async_val + max(summary_data["async_throughput"]) * 0.02,
            f"{async_val:.0f}",
            ha="center",
            va="bottom",
            fontweight="bold",
        )
        # Add improvement percentage between the bars
        ax_summary.text(
            i,
            max(async_val, sync_val) + max(summary_data["async_throughput"]) * 0.06,
            f"+{improvement:.0f}%",
            ha="center",
            va="bottom",
            fontweight="bold",
            color="red",
            fontsize=12,
        )

    plt.tight_layout()

    # Save single comprehensive chart
    output_file = output_dir / "complete_throughput_analysis.png"
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()

    # Print summary
    overall_avg = improvement_df["improvement_pct"].mean()
    overall_min = improvement_df["improvement_pct"].min()
    overall_max = improvement_df["improvement_pct"].max()

    print(f"📊 Generated complete throughput analysis: {output_file}")
    print("\n🚀 SUMMARY:")
    print(f"   Overall average: Async is {overall_avg:.0f}% faster")
    print(f"   Range: {overall_min:.0f}% to {overall_max:.0f}% improvement")

    for _, row in summary_data.iterrows():
        print(
            f"   {row['delay_label']}: {row['improvement_pct']:.0f}% faster "
            f"({row['sync_throughput']:.0f} → {row['async_throughput']:.0f} MB/s)"
        )


def generate_summary_stats(df: pd.DataFrame) -> None:
    """Generate and print summary statistics"""
    print("\n📈 Benchmark Summary Statistics:")
    print("=" * 50)

    for mode in ["sync", "async"]:
        mode_data = df[df["mode"] == mode]
        if not mode_data.empty:
            avg_throughput = mode_data["avg_throughput_mbps"].mean()
            max_throughput = mode_data["avg_throughput_mbps"].max()

            print(f"{mode.upper():>5} Mode:")
            print(f"  Average Throughput: {avg_throughput:.2f} MB/s")
            print(f"  Peak Throughput: {max_throughput:.2f} MB/s")

    # Overall improvement
    sync_avg = df[df["mode"] == "sync"]["avg_throughput_mbps"].mean()
    async_avg = df[df["mode"] == "async"]["avg_throughput_mbps"].mean()

    if sync_avg > 0 and async_avg > 0:
        overall_improvement = ((async_avg / sync_avg) - 1) * 100
        print(f"\n🚀 Overall Async Improvement: {overall_improvement:+.1f}%")


if __name__ == "__main__":
    # Create output directory
    output_dir = Path("plots")
    output_dir.mkdir(exist_ok=True)

    try:
        # Load benchmark data
        df = load_benchmark_data("benchmark_results.csv")

        # Generate summary statistics
        generate_summary_stats(df)

        # Generate complete analysis in one chart
        plot_performance_improvement(df)

        print(f"\n✅ Complete analysis generated in '{output_dir}' directory")

    except Exception as e:
        print(f"❌ Error generating plots: {e}")
        raise
