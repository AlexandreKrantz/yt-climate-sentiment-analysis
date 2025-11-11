import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from transformers import pipeline

def analyze_sentiment(df):
    sentiment_analyzer = pipeline(
        "sentiment-analysis",
        model="cardiffnlp/twitter-roberta-base-sentiment-latest",
        max_length=512,
        truncation=True
    )

    # Prepare texts for batch processing
    texts = df['comment_text'].fillna("").astype(str).tolist()
    # Truncate texts to avoid token limit issues (using a shorter length for safety)
    texts = [text[:500] for text in texts]

    # Batch process all comments at once
    print("Analyzing sentiments in batches...")
    batch_size = 32  # Reduced batch size for better stability
    results = []

    for i in range(0, len(texts), batch_size):
        batch = texts[i:i+batch_size]
        try:
            batch_results = sentiment_analyzer(batch)
            results.extend(batch_results)
            print(f"Processed {min(i+batch_size, len(texts))}/{len(texts)} comments")
        except Exception as e:
            print(f"Error processing batch {i//batch_size}: {str(e)}")
            # Add neutral sentiment for failed analyses
            batch_results = [{'label': 'NEUTRAL', 'score': 0.0} for _ in batch]
            results.extend(batch_results)

    # Extract labels and scores
    df['sentiment'] = [r['label'] for r in results]
    df['confidence'] = [r['score'] for r in results]

    df.to_csv('sentiment_analysis.csv', index=False, encoding='utf-8')

    return df




def main():
    comments_df = pd.read_csv('video_comments.csv', encoding='utf-8')
    sentiment_df = analyze_sentiment(comments_df)

if __name__ == "__main__":
    main()