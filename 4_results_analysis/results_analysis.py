import pandas as pd
import matplotlib.pyplot as plt
import os


def individual_video_plots(df, output_dir):
    # Basic sanity check
    expected_cols = {"video_id", "comments", "sentiment", "confidence"}
    if not expected_cols.issubset(df.columns):
        raise ValueError(f"CSV must contain columns: {expected_cols}")

    # === PREP OUTPUT DIR ===
    os.makedirs(output_dir, exist_ok=True)

    # === GROUP AND PLOT ===
    # Count each sentiment per video
    grouped = df.groupby(["video_id", "sentiment"]).size().unstack(fill_value=0)

    # Ensure all three sentiment categories exist
    for sentiment in ["positive", "neutral", "negative"]:
        if sentiment not in grouped.columns:
            grouped[sentiment] = 0

    # Sort columns in consistent order
    grouped = grouped[["positive", "neutral", "negative"]]

    # Loop through each video and plot
    chart_count = 0
    for video_id, row in grouped.iterrows():
        counts = row.values
        sentiments = row.index

        plt.figure(figsize=(6, 4))
        plt.bar(sentiments, counts, color=["green", "gray", "red"])
        plt.title(f"Sentiment Distribution for Video:\n{video_id}")
        plt.xlabel("Sentiment")
        plt.ylabel("Number of Comments")
        plt.tight_layout()

        # Replace problematic characters in filenames
        safe_name = "".join(c if c.isalnum() else "_" for c in video_id)
        plt.savefig(os.path.join(output_dir, f"{safe_name}.png"), dpi=150)
        plt.close()

        chart_count += 1
        print(f"📊 Chart {chart_count} saved: {safe_name}.png")

    print(f"✅ Total charts saved: {chart_count} to {output_dir}")


def summary_plots(df, output_dir):
    # Basic validation
    expected_cols = {"video_id", "comments", "sentiment", "confidence"}
    if not expected_cols.issubset(df.columns):
        raise ValueError(f"CSV must contain columns: {expected_cols}")

    # === MAP SENTIMENT TO NUMERIC VALUES ===
    sentiment_map = {
        "positive": 1,
        "neutral": 0,
        "negative": -1
    }
    df["sentiment_score"] = df["sentiment"].map(sentiment_map)

    # === COMPUTE AVERAGE SENTIMENT PER VIDEO ===
    avg_sentiment = df.groupby("video_id")["sentiment_score"].mean()

    # === PLOT ===
    plt.figure(figsize=(10, 5))
    avg_sentiment.plot(
        kind="bar",
        color=["green" if v > 0 else "red" if v < 0 else "gray" for v in avg_sentiment]
    )

    plt.title("Average Sentiment per Video")
    plt.xlabel("Video ID")
    plt.ylabel("Average Sentiment Score (-1 = Neg, 0 = Neutral, 1 = Pos)")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    output_path = os.path.join(output_dir, "average_sentiment_per_video.png")
    plt.savefig(output_path, dpi=150)
    plt.close()

    print(f"✅ Saved average sentiment chart to: {output_path}")



def main():
    # === CONFIG ===
    input_csv = "sentiment_analysis.csv"  # path to your CSV
    output_dir = "video_sentiment_charts"  # where PNGs will be saved

    # === LOAD DATA ===
    df = pd.read_csv(input_csv)

    # individual_video_plots(df, output_dir)


    # === CONFIG ===
    output_dir = "summary_plots"
    os.makedirs(output_dir, exist_ok=True)
    summary_plots(df, output_dir)



if __name__ == "__main__": 
    main()