import csv
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import time
from private import API_KEY

def get_video_comments(youtube, video_id, max_comments=1000):
    """Fetch top-level comments for a video (no replies)."""
    comments = []
    next_page_token = None
    
    try:
        while len(comments) < max_comments:
            request = youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=min(100, max_comments - len(comments)),  # API max is 100 per request
                pageToken=next_page_token,
                textFormat='plainText'
            )
            
            response = request.execute()
            
            for item in response['items']:
                # Get only the top-level comment, not replies
                comment = item['snippet']['topLevelComment']['snippet']
                comments.append({
                    'comment_text': comment['textDisplay'],
                    'comment_author': comment['authorDisplayName'],
                    'comment_likes': comment.get('likeCount', 0),
                    'comment_date': comment['publishedAt']
                })
                
                if len(comments) >= max_comments:
                    break
            
            next_page_token = response.get('nextPageToken')
            
            if not next_page_token:
                break
            
            # Small delay to avoid rate limiting
            time.sleep(0.1)
        
        return comments
        
    except HttpError as e:
        print(f"Error fetching comments for video {video_id}: {e}")
        return []

def extract_comments_to_dataframe(input_file, output_file, api_key, max_comments=1000):
    """Read enhanced CSV and create a dataframe with one row per comment."""
    youtube = build('youtube', 'v3', developerKey=api_key)
    
    # Read the enhanced video data
    video_df = pd.read_csv(input_file)
    
    all_rows = []
    total_videos = len(video_df)
    
    for idx, video_row in video_df.iterrows():
        video_id = video_row['video_id']
        print(f"\nProcessing video {idx + 1}/{total_videos}: {video_id}")
        print(f"  Video: {video_row['video_name']}")
        
        # Get comments for this video
        comments = get_video_comments(youtube, video_id, max_comments)
        
        print(f"  Retrieved {len(comments)} comments")
        
        if comments:
            # Create a row for each comment, preserving all video data
            for comment in comments:
                row = video_row.to_dict()
                row['comment_text'] = comment['comment_text']
                row['comment_author'] = comment['comment_author']
                row['comment_likes'] = comment['comment_likes']
                row['comment_date'] = comment['comment_date']
                all_rows.append(row)
        else:
            # If no comments retrieved, create one row with empty comment
            row = video_row.to_dict()
            row['comment_text'] = None
            row['comment_author'] = None
            row['comment_likes'] = None
            row['comment_date'] = None
            all_rows.append(row)
    
    # Create dataframe and save to CSV
    result_df = pd.DataFrame(all_rows)
    result_df.to_csv(output_file, index=False, encoding='utf-8')
    
    print(f"\n{'='*60}")
    print(f"Extraction complete!")
    print(f"Total videos processed: {total_videos}")
    print(f"Total comments extracted: {len(all_rows)}")
    print(f"Output saved to: {output_file}")
    print(f"{'='*60}")
    
    return result_df

if __name__ == '__main__':
    # Configuration
    INPUT_FILE = 'video_list_enhanced.csv'
    OUTPUT_FILE = 'video_comments.csv'
    MAX_COMMENTS_PER_VIDEO = 1000
    
    if API_KEY == 'YOUR_API_KEY_HERE':
        print("Error: Please set your YouTube Data API key in the API_KEY variable.")
        print("\nTo get an API key:")
        print("1. Go to https://console.cloud.google.com/")
        print("2. Create a new project or select an existing one")
        print("3. Enable the YouTube Data API v3")
        print("4. Create credentials (API key)")
        print("5. Copy the API key and paste it in this script")
    else:
        df = extract_comments_to_dataframe(
            INPUT_FILE, 
            OUTPUT_FILE, 
            API_KEY, 
            MAX_COMMENTS_PER_VIDEO
        )