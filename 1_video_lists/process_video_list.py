import csv
import re
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from private import API_KEY

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
        
        # Get channel details for subscriber count and country
        channel_response = youtube.channels().list(
            part='statistics,brandingSettings',
            id=channel_id
        ).execute()
        
        if not channel_response['items']:
            return None
        
        channel = channel_response['items'][0]
        
        # Extract country from brandingSettings
        country = 'N/A'
        if 'brandingSettings' in channel and 'channel' in channel['brandingSettings']:
            country = channel['brandingSettings']['channel'].get('country', 'N/A')
        
        return {
            'video_id': video_id,
            'video_name': video['snippet']['title'],
            'channel_name': video['snippet']['channelTitle'],
            'channel_country': country,
            'published_at': video['snippet']['publishedAt'],
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
            'video_id', 'video_name', 'channel_name', 'channel_country', 'published_at', 'subscribers', 
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
                            if comment_count < 100:
                                print(f"  ✗ Skipped (comments: {comment_count} < 100)")
                                continue
                        except (ValueError, TypeError):
                            print(f"  ✗ Skipped (invalid comment count)")
                            continue
                        
                        # Filter: only include videos published since 2023
                        try:
                            published_date = datetime.fromisoformat(data['published_at'].replace('Z', '+00:00'))
                            if published_date.year >= 2023:
                                rows.append(row)
                                print(f"  ✓ Added (comments: {comment_count}, published: {published_date.strftime('%Y-%m-%d')}, country: {data['channel_country']})")
                            else:
                                print(f"  ✗ Skipped (published: {published_date.strftime('%Y-%m-%d')} < 2023)")
                        except (ValueError, TypeError):
                            print(f"  ✗ Skipped (invalid publication date)")
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
    print(f"Total videos processed: {len(rows)} (filtered to videos with ≥100 comments published since 2023)")

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