import csv
import re
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Replace with your YouTube Data API key
API_KEY = 'AIzaSyCGtBUDsSVTvdKu5WCc7GkMuMdyRrUAgT0'


def extract_video_id(url):
    """Extract video ID from various YouTube URL formats."""
    patterns = [
        r'(?:youtube\.com\/watch\?v=|youtu\.be\/|youtube\.com\/embed\/)([^&\n?#]+)',
        r'youtube\.com\/watch\?.*v=([^&\n?#]+)'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None

def get_video_data(youtube, video_id):
    """Fetch video and channel data from YouTube API."""
    try:
        # Get video details
        video_response = youtube.videos().list(
            part='snippet,statistics',
            id=video_id
        ).execute()
        
        if not video_response['items']:
            return None
        
        video = video_response['items'][0]
        channel_id = video['snippet']['channelId']
        
        # Get channel details for subscriber count
        channel_response = youtube.channels().list(
            part='statistics',
            id=channel_id
        ).execute()
        
        if not channel_response['items']:
            return None
        
        channel = channel_response['items'][0]
        
        return {
            'video_id': video_id,
            'video_name': video['snippet']['title'],
            'channel_name': video['snippet']['channelTitle'],
            'subscribers': channel['statistics'].get('subscriberCount', 'N/A'),
            'views': video['statistics'].get('viewCount', '0'),
            'comments': video['statistics'].get('commentCount', '0'),
            'likes': video['statistics'].get('likeCount', '0')
        }
        
    except HttpError as e:
        print(f"Error fetching data for video {video_id}: {e}")
        return None

def enrich_csv(input_file, output_file, api_key):
    """Read input CSV, enrich with YouTube data, and write to output CSV."""
    youtube = build('youtube', 'v3', developerKey=api_key)
    
    with open(input_file, 'r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        
        # Prepare output fieldnames
        fieldnames = reader.fieldnames + [
            'video_id', 'video_name', 'channel_name', 'subscribers', 
            'views', 'comments', 'likes'
        ]
        
        rows = []
        for row in reader:
            video_link = row.get('video_link', '').strip()
            
            if video_link:
                video_id = extract_video_id(video_link)
                
                if video_id:
                    print(f"Processing video: {video_id}")
                    data = get_video_data(youtube, video_id)
                    
                    if data:
                        row.update(data)
                        
                        # Filter: only include videos with at least 100 comments
                        try:
                            comment_count = int(data['comments'])
                            if comment_count >= 100:
                                rows.append(row)
                                print(f"  ✓ Added (comments: {comment_count})")
                            else:
                                print(f"  ✗ Skipped (comments: {comment_count} < 100)")
                        except (ValueError, TypeError):
                            print(f"  ✗ Skipped (invalid comment count)")
                    else:
                        print(f"  ✗ Failed to fetch data")
                else:
                    print(f"Could not extract video ID from: {video_link}")
            
            else:
                print(f"Empty video link found")
    
    # Write enriched data to output CSV
    with open(output_file, 'w', encoding='utf-8', newline='') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"\nEnrichment complete! Output saved to {output_file}")
    print(f"Total videos processed: {len(rows)} (filtered to videos with ≥100 comments)")

if __name__ == '__main__':
    # Configuration
    INPUT_FILE = 'video_list.csv'
    OUTPUT_FILE = 'video_list_enhanced.csv'
    
    if API_KEY == 'YOUR_API_KEY_HERE':
        print("Error: Please set your YouTube Data API key in the API_KEY variable.")
        print("\nTo get an API key:")
        print("1. Go to https://console.cloud.google.com/")
        print("2. Create a new project or select an existing one")
        print("3. Enable the YouTube Data API v3")
        print("4. Create credentials (API key)")
        print("5. Copy the API key and paste it in this script")
    else:
        enrich_csv(INPUT_FILE, OUTPUT_FILE, API_KEY)