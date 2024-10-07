import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

# Assuming the data is stored in a DataFrame
df = pd.read_csv("results.csv")  # Load the benchmark results

# Set the seaborn style for the plots
sns.set(style="whitegrid")


# Function to plot and save graphs for different batch_generation_delay values
def plot_throughput(df, delay_value, filename):
    # Filter data for the specific batch_generation_delay
    df_filtered = df[df["batch_generation_delay"] == delay_value]

    plt.figure(figsize=(10, 6))

    # Plot throughput as a function of concurrency level, with hue for sync/async
    sns.lineplot(
        x="concurrent_requests",
        y="throughput_MBps",
        hue="type",
        style="records_per_batch",
        data=df_filtered,
        markers=True,
        dashes=False,
    )

    plt.title(f"Throughput Comparison (batch_generation_delay = {delay_value} seconds)")
    plt.xlabel("Concurrency Requests")
    plt.ylabel("Throughput (MB/s)")
    plt.legend(title="Mode / Records per Batch")
    plt.grid(True)

    # Save the figure
    plt.savefig(filename)
    plt.close()


if __name__ == "__main__":
    # Generate and save plots for each delay value
    plot_throughput(df, 0.001, "throughput_1ms.png")
    plot_throughput(df, 0.01, "throughput_10ms.png")
    plot_throughput(df, 0.1, "throughput_100ms.png")
