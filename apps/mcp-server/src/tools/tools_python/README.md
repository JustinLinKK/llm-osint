# Python OSINT Tools

Python helpers exposed through the MCP Python bridge.

## Supported Platforms

| Platform | Script | Method |
|----------|--------|--------|
| X (Twitter) | `get_user_posts.py` | Official API v2 |
| Unified MCP wrapper | `unified_research_mcp.py` | `person_search` + research integration tools |

## Prerequisites

- Python 3.6+
- For X API approach: X API Bearer Token (https://developer.x.com/)
- For Browserbase approach: Browserbase account (https://browserbase.com/)

## Setup

1. Copy the example environment file (repo root preferred for MCP use):

```bash
cp apps/mcp-server/src/tools/tools_python/env.example .env
```

2. Edit `.env` and add your credentials:

```bash
# For X API approach
X_BEARER_TOKEN=your_bearer_token_here

# For Browserbase approach (X and LinkedIn)
BROWSERBASE_API_KEY=your_api_key_here
BROWSERBASE_PROJECT_ID=your_project_id_here

# For LinkedIn (auto-login)
LINKEDIN_EMAIL=your_email@example.com
LINKEDIN_PASSWORD=your_password_here
```

3. For Browserbase approach, install additional dependencies:

```bash
pip install browserbase playwright
playwright install chromium
```

> These scripts load environment variables from the repo root `.env` first, then a local `.env` next to the script.

## MCP Integration

These scripts can be called by the Node MCP server using the Python tool bridge.
Set `MCP_PYTHON_TOOLS` and (optionally) `PYTHON_BIN` in your environment.

Example:

```bash
MCP_PYTHON_TOOLS='[
  {
    "name": "person_search",
    "description": "Search web pages about a person and extract page content",
    "scriptPath": "apps/mcp-server/src/tools/tools_python/unified_research_mcp.py",
    "timeoutMs": 120000
  },
  {
    "name": "x_get_user_posts_api",
    "description": "Fetch X posts via official API v2 (research integration)",
    "scriptPath": "apps/mcp-server/src/tools/tools_python/unified_research_mcp.py",
    "timeoutMs": 180000
  },
  {
    "name": "linkedin_download_html_ocr",
    "description": "Download LinkedIn profile/activity HTML via Browserbase (research integration)",
    "scriptPath": "apps/mcp-server/src/tools/tools_python/unified_research_mcp.py",
    "timeoutMs": 300000
  },
  {
    "name": "google_serp_person_search",
    "description": "Search person via Google SERP service and archive HTML results (research integration)",
    "scriptPath": "apps/mcp-server/src/tools/tools_python/unified_research_mcp.py",
    "timeoutMs": 180000
  },
  {
    "name": "arxiv_search_and_download",
    "description": "Search arXiv and download papers (research integration)",
    "scriptPath": "apps/mcp-server/src/tools/tools_python/unified_research_mcp.py",
    "timeoutMs": 300000
  }
]'
```

MCP input payloads use JSON with the following fields:

- `person_search`: `{ "runId": "...", "name": "Barbara Liskov", "max_results": 5, "delay": 1.0, "request_timeout": 10.0, "download_dir": null, "seen_urls": null, "no_cache": false }`
- `x_get_user_posts_api`: `{ "runId": "...", "username": "openai", "max_results": 10, "download_media": false }`
- `linkedin_download_html_ocr`: `{ "runId": "...", "profile": "https://www.linkedin.com/in/username", "output_dir": "linkedin_html", "reset_session": false }`
- `google_serp_person_search`: `{ "runId": "...", "target_name": "Sam Altman", "max_results": 10, "output_dir": "results/sam_search" }`
- `arxiv_search_and_download`: `{ "runId": "...", "author": "Geoffrey Hinton", "max_results": 5, "output_dir": "results/arxiv_hinton" }`

## MCP Test Script

Use the MCP test runner after configuring `MCP_PYTHON_TOOLS` and credentials in `.env`:

```bash
RUN_ID=<optional-uuid> yarn tsx apps/mcp-server/scripts/test-python-tools.ts
```

---

## Approach 1: API (get_user_posts.py)

```bash
# Fetch all tweets from a user (up to 3200)
python get_user_posts.py elonmusk

# Fetch only the 10 most recent tweets
python get_user_posts.py elonmusk --max-results 10

# Save results to a JSON file
python get_user_posts.py elonmusk -o tweets.json

# Get raw API response (includes all fields)
python get_user_posts.py elonmusk --raw -o raw_tweets.json
```

### Options

| Option | Description |
|--------|-------------|
| `username` | X username to fetch posts from (without @) |
| `--max-results`, `-n` | Maximum number of tweets to fetch (default: all available) |
| `--output`, `-o` | Output file path (JSON format) |
| `--raw` | Output raw API response instead of formatted data |

## Output Format

```json
{
  "user": {
    "id": "123456789",
    "name": "Display Name",
    "username": "username"
  },
  "tweet_count": 100,
  "tweets": [
    {
      "id": "tweet_id",
      "text": "Tweet content...",
      "created_at": "2024-01-15T10:30:00.000Z",
      "likes": 42,
      "retweets": 5,
      "replies": 3
    }
  ]
}
```

### Limitations

- Maximum 3200 most recent tweets available per user (X API limitation)
- Requires valid X API credentials with appropriate access level

---

## Approach 2: Browserbase (get_user_posts_browserbase.py)

Scrapes posts directly from X.com using a cloud browser via Browserbase.

### Usage

```bash
# Scrape 10 posts from a user (default)
python get_user_posts_browserbase.py elonmusk

# Scrape 50 posts
python get_user_posts_browserbase.py elonmusk --max-posts 50

# Save results to a JSON file
python get_user_posts_browserbase.py elonmusk -o posts.json
```

### Options

| Option | Description |
|--------|-------------|
| `username` | X username to fetch posts from (without @) |
| `--max-posts`, `-n` | Maximum number of posts to scrape (default: 10) |
| `--output`, `-o` | Output file path (JSON format) |

### Output Format

```json
{
  "user": {
    "username": "elonmusk",
    "name": "Elon Musk",
    "bio": "...",
    "followers": "100M Followers",
    "following": "500 Following"
  },
  "post_count": 10,
  "posts": [
    {
      "id": "1234567890",
      "url": "https://x.com/elonmusk/status/1234567890",
      "text": "Post content...",
      "created_at": "2024-01-15T10:30:00.000Z",
      "metrics": {
        "likes": "1.2K",
        "retweets": "500",
        "replies": "200"
      },
      "is_retweet": false
    }
  ],
  "scraped_at": "2024-01-15T12:00:00.000Z"
}
```

### Limitations

- Slower than API approach (browser automation)
- May be affected by X.com UI changes
- Rate limited by Browserbase session limits
- No authentication support (can only see public posts)

---

## LinkedIn: Browserbase (get_linkedin_posts_browserbase.py)

Scrapes posts/activity from LinkedIn profiles using a cloud browser via Browserbase.
Uses **persistent browser context** to maintain login session across runs.

### How Persistent Sessions Work

1. **First run**: You'll need to login and complete any CAPTCHA/verification manually
2. **Subsequent runs**: Session is restored automatically, no login required

The context ID is saved to `.linkedin_context_id` file locally.

### Usage

```bash
# Scrape by username
python get_linkedin_posts_browserbase.py johndoe

# Scrape by full URL
python get_linkedin_posts_browserbase.py "https://www.linkedin.com/in/johndoe"

# Scrape company page
python get_linkedin_posts_browserbase.py "https://www.linkedin.com/company/google"

# Scrape 20 posts
python get_linkedin_posts_browserbase.py johndoe --max-posts 20

# Save to custom file
python get_linkedin_posts_browserbase.py johndoe -o johndoe_posts.json

# Reset session (re-authenticate)
python get_linkedin_posts_browserbase.py --reset-session
```

### Options

| Option | Description |
|--------|-------------|
| `profile` | LinkedIn username or full profile URL |
| `--max-posts`, `-n` | Maximum number of posts to scrape (default: 10) |
| `--output`, `-o` | Output file path (default: linkedin_posts.json) |
| `--reset-session` | Delete saved session and re-authenticate |

### Output Format

```json
{
  "user": {
    "profile_url": "https://www.linkedin.com/in/johndoe",
    "username": "johndoe",
    "name": "John Doe",
    "headline": "Software Engineer at Company",
    "location": "San Francisco, CA",
    "connections": "500+ connections",
    "followers": "1K followers"
  },
  "post_count": 10,
  "posts": [
    {
      "id": "urn:li:activity:123456789",
      "text": "Post content...",
      "author": "John Doe",
      "posted_at": "2d",
      "metrics": {
        "likes": "42 likes",
        "comments": "5 comments",
        "reposts": "2 reposts"
      }
    }
  ],
  "scraped_at": "2024-01-15T12:00:00.000Z"
}
```

### Limitations

- First run requires manual verification (CAPTCHA, email code, etc.)
- May be affected by LinkedIn UI changes
- Rate limited by Browserbase session limits
- Session may expire after extended periods of inactivity
