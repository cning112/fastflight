import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

# Assuming the data is stored in a DataFrame
df = pd.read_csv("results.csv")  # Load the benchmark results

# Set the seaborn style for the plots
sns.set(style="whitegrid")


# Function to plot and save graphs for different delay_per_row values
def plot_throughput(df, delay_value, filename):
    # Filter data for the specific delay_per_row
    df_filtered = df[df["delay_per_row"] == delay_value]

    plt.figure(figsize=(10, 6))

    # Plot throughput as a function of concurrency level, with hue for sync/async
    sns.lineplot(
        x="concurrent_requests",
        y="throughput_MBps",
        hue="type",
        style="rows_per_batch",
        data=df_filtered,
        markers=True,
        dashes=False,
    )

    plt.title(f"Throughput Comparison (delay_per_row = {delay_value} seconds)")
    plt.xlabel("Concurrency Requests")
    plt.ylabel("Throughput (MB/s)")
    plt.legend(title="Mode / Records per Batch")
    plt.grid(True)

    # Save the figure
    plt.savefig(filename)
    plt.close()


if __name__ == "__main__":
    # Generate and save plots for each delay value
    plot_throughput(df, 1e-6, "throughput_1us.png")
    plot_throughput(df, 1e-5, "throughput_10us.png")
