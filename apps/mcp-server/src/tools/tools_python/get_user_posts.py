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
from contextlib import redirect_stdout
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode


def find_repo_root(start: Path):
    for parent in start.parents:
        if (parent / "apps").exists() and (parent / "infra").exists():
            return parent
    return None


def load_env_file():
    """Load environment variables from .env files (repo root, then local)."""
    candidates = []
    repo_root = find_repo_root(Path(__file__).resolve())
    if repo_root:
        candidates.append(repo_root / ".env")
    candidates.append(Path(__file__).parent / ".env")

    for env_path in candidates:
        if not env_path.exists():
            continue
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
        raise RuntimeError("X_BEARER_TOKEN environment variable is not set.")
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
        raise RuntimeError(f"HTTP Error {e.code}: {e.reason}. Response: {error_body}")
    except URLError as e:
        raise RuntimeError(f"URL Error: {e.reason}")


def get_user_by_username(username, bearer_token):
    """
    Step 1: Get user ID from username.
    Endpoint: GET /2/users/by/username/:username
    """
    url = f"{BASE_URL}/users/by/username/{username}"
    print(f"Fetching user info for @{username}...")

    data = make_request(url, bearer_token)

    if "errors" in data and not data.get("data"):
        raise RuntimeError(f"User @{username} not found.")

    user = data["data"]
    print(f"Found user: {user['name']} (@{user['username']}) - ID: {user['id']}")
    return user


def get_user_tweets(user_id, bearer_token, max_results=None):
    """
    Step 2: Get all tweets from user by ID.
    Endpoint: GET /2/users/:id/tweets

    Note: Maximum 3200 most recent tweets are available.
    """
    all_tweets = []
    pagination_token = None
    page = 1

    # Tweet fields to include
    tweet_fields = [
        "id", "text", "created_at", "author_id",
        "public_metrics", "entities", "referenced_tweets"
    ]

    while True:
        # Build query parameters
        params = {
            "max_results": 100,  # Max per request
            "tweet.fields": ",".join(tweet_fields),
        }

        if pagination_token:
            params["pagination_token"] = pagination_token

        url = f"{BASE_URL}/users/{user_id}/tweets?{urlencode(params)}"

        print(f"Fetching page {page}...", end=" ", flush=True)
        data = make_request(url, bearer_token)

        tweets = data.get("data", [])
        print(f"got {len(tweets)} tweets")

        if not tweets:
            break

        all_tweets.extend(tweets)

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

    return all_tweets


def format_tweet(tweet):
    """Format a tweet for display."""
    metrics = tweet.get("public_metrics", {})
    return {
        "id": tweet["id"],
        "text": tweet["text"],
        "created_at": tweet.get("created_at", "N/A"),
        "likes": metrics.get("like_count", 0),
        "retweets": metrics.get("retweet_count", 0),
        "replies": metrics.get("reply_count", 0),
    }


def cli_main():
    parser = argparse.ArgumentParser(
        description="Fetch all posts from a X (Twitter) user by username"
    )
    parser.add_argument("username", help="X username (without @)")
    parser.add_argument(
        "--max-results", "-n", type=int, default=None,
        help="Maximum number of tweets to fetch (default: all available, up to 3200)"
    )
    parser.add_argument(
        "--output", "-o", type=str, default=None,
        help="Output file path (JSON format). If not specified, prints to stdout."
    )
    parser.add_argument(
        "--raw", action="store_true",
        help="Output raw API response instead of formatted data"
    )

    args = parser.parse_args()

    # Remove @ if user included it
    username = args.username.lstrip("@")

    try:
        bearer_token = get_bearer_token()
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        print("Get your token from https://developer.x.com/", file=sys.stderr)
        sys.exit(1)

    # Step 1: Get user ID
    user = get_user_by_username(username, bearer_token)

    # Step 2: Get tweets
    print(f"\nFetching tweets for user ID {user['id']}...")
    tweets = get_user_tweets(user["id"], bearer_token, args.max_results)

    print(f"\nTotal tweets fetched: {len(tweets)}")

    # Prepare output
    if args.raw:
        output_data = {"user": user, "tweets": tweets}
    else:
        output_data = {
            "user": {
                "id": user["id"],
                "name": user["name"],
                "username": user["username"],
            },
            "tweet_count": len(tweets),
            "tweets": [format_tweet(t) for t in tweets],
        }

    # Output results
    json_output = json.dumps(output_data, indent=2, ensure_ascii=False)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(json_output)
        print(f"\nResults saved to: {args.output}")
    else:
        print("\n" + "=" * 50)
        print(json_output)


def read_stdin_payload():
    if sys.stdin.isatty():
        return None
    raw = sys.stdin.read()
    if not raw or not raw.strip():
        return None
    try:
        payload = json.loads(raw)
        return payload if isinstance(payload, dict) else None
    except json.JSONDecodeError:
        return None


def emit_mcp_result(ok, result=None, error=None):
    payload = {"ok": ok}
    if ok:
        payload["result"] = result
    else:
        payload["error"] = error or "Unknown error"
    sys.stdout.write(json.dumps(payload, ensure_ascii=True))
    sys.stdout.flush()


def mcp_main(payload):
    input_data = payload.get("input", {}) if isinstance(payload, dict) else {}
    username = (input_data.get("username") or "").lstrip("@")
    if not username:
        emit_mcp_result(False, error="Missing required input: username")
        return

    max_results = input_data.get("max_results")
    raw = bool(input_data.get("raw", False))

    try:
        with redirect_stdout(sys.stderr):
            bearer_token = get_bearer_token()
            user = get_user_by_username(username, bearer_token)
            tweets = get_user_tweets(user["id"], bearer_token, max_results)

            if raw:
                output_data = {"user": user, "tweets": tweets}
            else:
                output_data = {
                    "user": {
                        "id": user["id"],
                        "name": user["name"],
                        "username": user["username"],
                    },
                    "tweet_count": len(tweets),
                    "tweets": [format_tweet(t) for t in tweets],
                }

        emit_mcp_result(True, result=output_data)
    except Exception as exc:
        emit_mcp_result(False, error=str(exc))


if __name__ == "__main__":
    payload = read_stdin_payload()
    if payload and "input" in payload:
        mcp_main(payload)
    else:
        cli_main()
