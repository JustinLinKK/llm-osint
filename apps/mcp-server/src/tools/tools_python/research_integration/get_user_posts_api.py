#!/usr/bin/env python3
"""
Fetch all posts from a specific X (Twitter) user by username.
Uses X API v2 endpoints.

Usage:
    python get_user_posts.py <username> [--max-results N] [--output FILE]

Environment (via .env file or shell):
    X_BEARER_TOKEN: Your X API Bearer Token (required)
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
import hashlib


def load_env_file():
    """Load environment variables from .env file."""
    env_path = Path(__file__).parent / ".env"
    if not env_path.exists():
        return

    with open(env_path, "r") as f:
        for line in f:
            line = line.strip()
            # Skip empty lines and comments
            if not line or line.startswith("#"):
                continue
            # Parse KEY=VALUE
            if "=" in line:
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                # Remove quotes if present
                if (value.startswith('"') and value.endswith('"')) or \
                   (value.startswith("'") and value.endswith("'")):
                    value = value[1:-1]
                # Only set if not already in environment
                if key not in os.environ:
                    os.environ[key] = value


# Load .env file before accessing environment variables
load_env_file()

BASE_URL = "https://api.x.com/2"


def get_bearer_token():
    """Get bearer token from environment variable."""
    token = os.environ.get("X_BEARER_TOKEN")
    if not token:
        print("Error: X_BEARER_TOKEN environment variable is not set.", file=sys.stderr)
        print("Get your token from https://developer.x.com/", file=sys.stderr)
        sys.exit(1)
    return token


def make_request(url, bearer_token):
    """Make an authenticated GET request to the X API."""
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "User-Agent": "Python X API Client",
    }
    request = Request(url, headers=headers)

    try:
        with urlopen(request) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as e:
        error_body = e.read().decode("utf-8")
        print(f"HTTP Error {e.code}: {e.reason}", file=sys.stderr)
        print(f"Response: {error_body}", file=sys.stderr)
        sys.exit(1)
    except URLError as e:
        print(f"URL Error: {e.reason}", file=sys.stderr)
        sys.exit(1)


def get_user_by_username(username, bearer_token):
    """
    Step 1: Get user ID from username.
    Endpoint: GET /2/users/by/username/:username
    """
    url = f"{BASE_URL}/users/by/username/{username}"
    print(f"Fetching user info for @{username}...")

    data = make_request(url, bearer_token)

    if "errors" in data and not data.get("data"):
        print(f"Error: User @{username} not found.", file=sys.stderr)
        sys.exit(1)

    user = data["data"]
    print(f"Found user: {user['name']} (@{user['username']}) - ID: {user['id']}")
    return user


def get_user_tweets(user_id, bearer_token, max_results=None, include_media=False):
    """
    Step 2: Get all tweets from user by ID.
    Endpoint: GET /2/users/:id/tweets

    Note: Maximum 3200 most recent tweets are available.
    """
    all_tweets = []
    all_media = []
    pagination_token = None
    page = 1

    # Tweet fields to include
    tweet_fields = [
        "id", "text", "created_at", "author_id",
        "public_metrics", "entities", "referenced_tweets", "attachments"
    ]

    while True:
        # Build query parameters
        params = {
            "max_results": 10,  # Max per request
            "tweet.fields": ",".join(tweet_fields),
        }
        
        # Add media expansions if requested
        if include_media:
            params["expansions"] = "attachments.media_keys"
            params["media.fields"] = "url,preview_image_url,type,duration_ms,height,width,variants,public_metrics"

        if pagination_token:
            params["pagination_token"] = pagination_token

        url = f"{BASE_URL}/users/{user_id}/tweets?{urlencode(params)}"

        print(f"Fetching page {page}...", end=" ", flush=True)
        data = make_request(url, bearer_token)

        tweets = data.get("data", [])
        media = data.get("includes", {}).get("media", [])
        print(f"got {len(tweets)} tweets, {len(media)} media items")

        if not tweets:
            break

        all_tweets.extend(tweets)
        all_media.extend(media)

        # Check if we've reached the requested max
        if max_results and len(all_tweets) >= max_results:
            all_tweets = all_tweets[:max_results]
            break

        # Check for next page
        meta = data.get("meta", {})
        pagination_token = meta.get("next_token")

        if not pagination_token:
            break

        page += 1
        # Rate limit: be nice to the API
        time.sleep(0.5)

    # Filter media to only include items referenced by the final tweet set
    if include_media and all_tweets:
        # Collect all media_keys referenced by final tweets
        referenced_media_keys = set()
        for tweet in all_tweets:
            attachments = tweet.get("attachments", {})
            media_keys = attachments.get("media_keys", [])
            referenced_media_keys.update(media_keys)
        
        # Filter media to only those referenced
        all_media = [m for m in all_media if m.get("media_key") in referenced_media_keys]

    return all_tweets, all_media


def download_media_file(url, output_dir, filename):
    """Download a media file from URL."""
    try:
        output_path = output_dir / filename
        if output_path.exists():
            print(f"  Skipping {filename} (already exists)")
            return str(output_path)
        
        request = Request(url, headers={"User-Agent": "Python X API Client"})
        with urlopen(request) as response:
            data = response.read()
            with open(output_path, "wb") as f:
                f.write(data)
        print(f"  Downloaded {filename}")
        return str(output_path)
    except Exception as e:
        print(f"  Error downloading {filename}: {e}", file=sys.stderr)
        return None


def select_video_variant(variants, max_bitrate=800000):
    """Select video variant with resolution restriction.
    
    Args:
        variants: List of video variants from API
        max_bitrate: Maximum bitrate in bps (default 800000 = ~800 kbps for economy)
    
    Returns:
        URL of selected variant or None
    """
    if not variants:
        return None
    
    # Filter video variants (exclude m3u8 playlist format)
    video_variants = [v for v in variants if v.get("content_type") == "video/mp4"]
    
    if not video_variants:
        return None
    
    # Sort by bitrate and select the highest one under max_bitrate
    video_variants.sort(key=lambda v: v.get("bit_rate", 0))
    
    # Find the best variant under max_bitrate
    selected = None
    for variant in video_variants:
        if variant.get("bit_rate", 0) <= max_bitrate:
            selected = variant
        else:
            break
    
    # If no variant is under max_bitrate, use the lowest quality
    if not selected and video_variants:
        selected = video_variants[0]
    
    return selected.get("url") if selected else None


def download_media(media_list, tweets, output_dir, max_video_bitrate=800000):
    """Download media files for tweets.
    
    Args:
        media_list: List of media objects from API
        tweets: List of tweet objects
        output_dir: Directory to save media files
        max_video_bitrate: Maximum video bitrate in bps for economy
    
    Returns:
        Dictionary mapping media_keys to local file paths
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True)
    
    downloaded = {}
    
    print(f"\nDownloading media to {output_dir}/...")
    
    # Create a mapping of media_key to tweet_id for filename generation
    media_to_tweet = {}
    for tweet in tweets:
        attachments = tweet.get("attachments", {})
        media_keys = attachments.get("media_keys", [])
        for media_key in media_keys:
            if media_key not in media_to_tweet:
                media_to_tweet[media_key] = tweet["id"]
    
    # Iterate through all media items directly
    for media in media_list:
        media_key = media.get("media_key")
        if not media_key:
            continue
            
        media_type = media.get("type")
        
        # Use associated tweet_id or media_key for filename
        tweet_id = media_to_tweet.get(media_key, "unknown")
        
        # Generate filename
        file_ext = "jpg" if media_type == "photo" else "mp4"
        filename = f"{tweet_id}_{media_key}.{file_ext}"
        
        if media_type == "photo":
            url = media.get("url")
            if url:
                path = download_media_file(url, output_dir, filename)
                if path:
                    downloaded[media_key] = {
                        "type": "photo",
                        "path": path,
                        "url": url
                    }
        
        elif media_type in ["video", "animated_gif"]:
            variants = media.get("variants", [])
            url = select_video_variant(variants, max_video_bitrate)
            
            if url:
                path = download_media_file(url, output_dir, filename)
                if path:
                    downloaded[media_key] = {
                        "type": media_type,
                        "path": path,
                        "url": url,
                        "preview_url": media.get("preview_image_url")
                    }
    
    print(f"Downloaded {len(downloaded)} media files")
    return downloaded


def format_tweet(tweet, media_map=None):
    """Format a tweet for display.
    
    Args:
        tweet: Tweet object from API
        media_map: Optional dict mapping media_keys to downloaded file info
    """
    metrics = tweet.get("public_metrics", {})
    formatted = {
        "id": tweet["id"],
        "text": tweet["text"],
        "created_at": tweet.get("created_at", "N/A"),
        "likes": metrics.get("like_count", 0),
        "retweets": metrics.get("retweet_count", 0),
        "replies": metrics.get("reply_count", 0),
    }
    
    # Add media info if available
    if media_map:
        attachments = tweet.get("attachments", {})
        media_keys = attachments.get("media_keys", [])
        if media_keys:
            formatted["media"] = []
            for key in media_keys:
                if key in media_map:
                    formatted["media"].append(media_map[key])
    
    return formatted


def main():
    parser = argparse.ArgumentParser(
        description="Fetch all posts from a X (Twitter) user by username"
    )
    parser.add_argument("username", help="X username (without @)")
    parser.add_argument(
        "--max-results", "-n", type=int, default=10,
        help="Maximum number of tweets to fetch (default: 10, up to 3200)"
    )
    parser.add_argument(
        "--output", "-o", type=str, default=None,
        help="Output file path (JSON format). If not specified, prints to stdout. Default location: results/<username>_posts.json"
    )
    parser.add_argument(
        "--raw", action="store_true",
        help="Output raw API response instead of formatted data"
    )
    parser.add_argument(
        "--download-media", action="store_true",
        help="Download images and videos from tweets"
    )
    parser.add_argument(
        "--media-dir", type=str, default=None,
        help="Directory to save media files (default: <username>_media)"
    )
    parser.add_argument(
        "--max-video-bitrate", type=int, default=800000,
        help="Maximum video bitrate in bps for economy (default: 800000 = ~800 kbps)"
    )

    args = parser.parse_args()

    # Remove @ if user included it
    username = args.username.lstrip("@")

    bearer_token = get_bearer_token()

    # Step 1: Get user ID
    user = get_user_by_username(username, bearer_token)

    # Step 2: Get tweets
    print(f"\nFetching tweets for user ID {user['id']}...")
    tweets, media = get_user_tweets(
        user["id"], 
        bearer_token, 
        args.max_results,
        include_media=args.download_media
    )

    print(f"\nTotal tweets fetched: {len(tweets)}")
    print(f"Total media items found: {len(media)}")
    
    # Download media if requested
    media_map = None
    if args.download_media and media:
        media_dir = args.media_dir or f"{username}_media"
        media_map = download_media(
            media, 
            tweets, 
            media_dir,
            max_video_bitrate=args.max_video_bitrate
        )

    # Prepare output
    if args.raw:
        output_data = {"user": user, "tweets": tweets, "media": media}
    else:
        output_data = {
            "user": {
                "id": user["id"],
                "name": user["name"],
                "username": user["username"],
            },
            "tweet_count": len(tweets),
            "tweets": [format_tweet(t, media_map) for t in tweets],
        }
        if args.download_media and media_map:
            output_data["media_downloaded"] = len(media_map)

    # Output results
    json_output = json.dumps(output_data, indent=2, ensure_ascii=False)

    if args.output:
        # Ensure output directory exists
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(json_output)
        print(f"\nResults saved to: {args.output}")
    else:
        print("\n" + "=" * 50)
        print(json_output)


if __name__ == "__main__":
    main()
