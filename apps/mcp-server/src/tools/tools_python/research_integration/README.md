# Social Media Posts Fetcher

Fetch posts from social media profiles (X/Twitter, LinkedIn) using multiple approaches.

## Supported Platforms

| Platform | Script | Method | Status |
|----------|--------|--------|--------|
| X (Twitter) | `get_user_posts_api.py` | Official API v2 | ✅ Active |
| LinkedIn | `download_linkedin_html_ocr.py` | Browserbase HTML download | ✅ Active |
| Google Search | `search_google_serp.py` | Google SERP Service API + HTML archiving | ✅ Active |
| arXiv | `search_arxiv_and_download.py` | arXiv API query + PDF download | ✅ Active |
| X (Twitter) | `deprecated/get_user_posts_browserbase.py` | Browserbase scraping | ⚠️ Deprecated |
| LinkedIn | `deprecated/get_linkedin_posts_browserbase.py` | Browserbase scraping | ⚠️ Deprecated |

## Prerequisites

- Python 3.6+
- For X API approach: X API Bearer Token (https://developer.x.com/)
- For Browserbase approach: Browserbase account (https://browserbase.com/)

## Setup

1. Activate the conda environment:

```bash
conda activate test_mcp_tool
```

2. Copy the example environment file:

```bash
cp .env.example .env
```

3. Edit `.env` and add your credentials:

```bash
# For X API approach
X_BEARER_TOKEN=your_bearer_token_here

# For Browserbase approach (X and LinkedIn)
BROWSERBASE_API_KEY=your_api_key_here
BROWSERBASE_PROJECT_ID=your_project_id_here

# For LinkedIn (auto-login)
LINKEDIN_EMAIL=your_email@example.com
LINKEDIN_PASSWORD=your_password_here

# For Google SERP Service
GOOGLE_SERP_BASE_URL=your_google_serp_base_url_here
GOOGLE_SERP_API_KEY=your_api_key_here
# Optional alternative auth
GOOGLE_SERP_BEARER_TOKEN=your_bearer_token_here
```

4. For Browserbase approach, install additional dependencies:

```bash
pip install browserbase playwright
playwright install chromium
```

## MCP Tool Registration

Register active ToDo tools and `person_search` using the unified wrapper:

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
    "description": "Fetch X posts via official API v2",
    "scriptPath": "apps/mcp-server/src/tools/tools_python/unified_research_mcp.py",
    "timeoutMs": 180000
  },
  {
    "name": "linkedin_download_html_ocr",
    "description": "Download LinkedIn profile/activity HTML via Browserbase",
    "scriptPath": "apps/mcp-server/src/tools/tools_python/unified_research_mcp.py",
    "timeoutMs": 300000
  },
  {
    "name": "google_serp_person_search",
    "description": "Search person via Google SERP service and archive HTML results",
    "scriptPath": "apps/mcp-server/src/tools/tools_python/unified_research_mcp.py",
    "timeoutMs": 180000
  },
  {
    "name": "arxiv_search_and_download",
    "description": "Search arXiv and download papers",
    "scriptPath": "apps/mcp-server/src/tools/tools_python/unified_research_mcp.py",
    "timeoutMs": 300000
  }
]'
```

Deprecated tools `get_user_posts_browserbase.py` and `get_linkedin_posts_browserbase.py` should not be registered.

---

## X (Twitter) API Approach (get_user_posts_api.py)

Fetch posts using the official X API v2.

```bash
# Basic usage
python get_user_posts_api.py elonmusk --max-results 10 -o results/tweets.json

# With media download (800 kbps max for videos)
python get_user_posts_api.py elonmusk --max-results 10 --download-media -o results/tweets.json

# Combined: custom bitrate, media directory, and output
python get_user_posts_api.py elonmusk --download-media --max-video-bitrate 400000 --max-results 10 --media-dir my_media -o results/tweets_media.json
```

### Options

| Option | Description |
|--------|-------------|
| `username` | X username to fetch posts from (without @) |
| `--max-results`, `-n` | Maximum number of tweets to fetch (default: all available) |
| `--output`, `-o` | Output file path (JSON format) |
| `--raw` | Output raw API response instead of formatted data |
| `--download-media` | Download media (images/videos) associated with tweets |
| `--max-video-bitrate` | Maximum bitrate for video downloads (default: 800
kbps) |
| `--media-dir` | Directory to save downloaded media (default: `media`) |

### Output Format

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

## LinkedIn HTML Download (download_linkedin_html_ocr.py)

Downloads LinkedIn profile and activity pages as raw HTML files via Browserbase.
Designed for later processing with OCR or parsing tools.

### Usage

```bash
# Download profile and activity pages
python download_linkedin_html_ocr.py "https://www.linkedin.com/in/johndoe"

# Specify custom output directory
python download_linkedin_html_ocr.py johndoe --output-dir my_downloads

# Reset session (re-authenticate)
python download_linkedin_html_ocr.py --reset-session
```

### Options

| Option | Description |
|--------|-------------|
| `profile` | LinkedIn username or full profile URL |
| `--output-dir`, `-o` | Output directory for HTML files (default: linkedin_html) |
| `--reset-session` | Delete saved session and re-authenticate |

### Output

Creates timestamped HTML files:
- `profile_YYYYMMDD_HHMMSS.html` - Full profile page
- `activity_YYYYMMDD_HHMMSS.html` - Activity/posts page

### Limitations

- First run requires manual verification (CAPTCHA, email code, etc.)
- Session may expire after extended periods of inactivity
- HTML structure may change with LinkedIn UI updates

---

## Google SERP Person Search (search_google_serp.py)

Search for a person name using your Google SERP service API and save each returned result page as local HTML.

```bash
# Basic usage (default max results = 10)
python search_google_serp.py "Elon Musk"

python search_google_serp.py "Joe Biden" -n 3 -o results/smoke_google_serp_joe_biden

# Specify result count and output directory
python search_google_serp.py "Sundar Pichai" -n 15 -o results/pichai_search

# Print agent-ready prompt bundle (system prompt + few-shot examples)
python search_google_serp.py "Sam Altman" --print-agent-prompt --agent-max-queries 6
```

The script strictly follows `openapi.json`:
- `GET /search` for up to 10 results (`q`, `num`, `start`)
- `POST /search/paged` for more than 10 results (`total_results` up to 100)

### Dorking Queries

You can pass Google dork-style operators directly in the query string, such as
`site:`, `intext:`, `intitle:`, `inurl:`, `allintext:`, `allinurl:`, `filetype:`,
quoted phrases, `OR`, `before:/after:`, `numrange:`, and exclusions (`-term`).

```bash
# Dorking example (Wikipedia-focused)
python search_google_serp.py 'site:wikipedia.org ("Sam Altman" OR "Samuel Altman") intitle:Sam' -n 5 -o results/dork_sam_altman_wikipedia
```

### Agent Usage (System Prompt + Few-Shot)

When this script is called by an AI agent, use `--print-agent-prompt` to get:
- a system prompt that explicitly lists supported dork operators and output constraints
- few-shot examples showing high-signal query generation
- a user prompt template bound to the target name

This helps stabilize query planning and keeps dork generation consistent.

### Rate Limit Handling

The script now retries temporary failures and rate limits (`429`, `500`, `502`, `503`, `504`)
with exponential backoff + jitter, and honors `Retry-After` when provided.

```bash
python search_google_serp.py "Sam Altman" \
  -n 10 \
  --max-retries 5 \
  --retry-backoff 2 \
  --retry-jitter 0.8 \
  --api-delay 1.2
```

Environment variable defaults are supported:
- `GOOGLE_SERP_MAX_RETRIES`
- `GOOGLE_SERP_RETRY_BACKOFF`
- `GOOGLE_SERP_RETRY_JITTER`
- `GOOGLE_SERP_API_DELAY`

### Output

Creates an output folder with:
- `api_response.json` - raw search API response
- `search_results.json` - normalized result metadata + local file paths
- `index.html` - local summary page linking to downloaded HTML files
- `html_results/*.html` - fetched pages for each search result URL

---

## arXiv Search + PDF Download (search_arxiv_and_download.py)

Search arXiv by author name or topic, then download PDFs for the first N results.

```bash
# Search by author
python search_arxiv_and_download.py --author "Geoffrey Hinton" -n 5

# Search by topic
python search_arxiv_and_download.py --topic "large language model" -n 5

# Combine author + topic
python search_arxiv_and_download.py --author "Yoshua Bengio" --topic "diffusion model" -n 5
```

If you hit arXiv rate limits (`HTTP 429`), use retry/backoff options and a clear User-Agent:

```bash
python search_arxiv_and_download.py \
  --author "Geoffrey Hinton" -n 1 \
  --user-agent "your-name-arxiv-tool/1.0 (mailto:your_email@domain.com)" \
  --api-delay 3 \
  --max-retries 6 \
  --retry-backoff 10
```

### Options

| Option | Description |
|--------|-------------|
| `--author` | Author name (mapped to arXiv `au:` query field) |
| `--topic` | Topic terms (mapped to arXiv `all:` query field) |
| `--max-results`, `-n` | Number of results to fetch/download |
| `--start` | 0-based start index in the result set |
| `--sort-by` | `relevance`, `lastUpdatedDate`, or `submittedDate` |
| `--sort-order` | `ascending` or `descending` |
| `--output-dir`, `-o` | Output directory (default: `results/arxiv_downloads`) |
| `--no-download` | Search metadata only, skip PDF downloads |
| `--max-retries` | Retry count for `429`/temporary HTTP errors |
| `--retry-backoff` | Initial retry delay (seconds), doubles on each retry |

### Output

Creates an output folder with:
- `metadata.json` - query info, parsed entries, and per-file download status
- `pdfs/*.pdf` - downloaded papers

---

## ⚠️ Deprecated Tools

The following scripts are deprecated but kept for reference:

### deprecated/get_user_posts_browserbase.py

**Status:** ⚠️ Deprecated  
**Replacement:** Use `get_user_posts_api.py` instead

Scrapes posts directly from X.com using a cloud browser via Browserbase.

### Usage

```bash
# Scrape 10 posts from a user (default)
python deprecated/get_user_posts_browserbase.py elonmusk

# Scrape 50 posts
python deprecated/get_user_posts_browserbase.py elonmusk --max-posts 50

# Save results to a JSON file
python deprecated/get_user_posts_browserbase.py elonmusk -o results/posts.json
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

### deprecated/get_linkedin_posts_browserbase.py

**Status:** ⚠️ Deprecated  
**Replacement:** Use `download_linkedin_html_ocr.py` instead

Scrapes posts/activity from LinkedIn profiles using a cloud browser via Browserbase.
Uses **persistent browser context** to maintain login session across runs.

### How Persistent Sessions Work

1. **First run**: You'll need to login and complete any CAPTCHA/verification manually
2. **Subsequent runs**: Session is restored automatically, no login required

The context ID is saved to `.linkedin_context_id` file locally.

### Usage

```bash
# Scrape by full URL
python deprecated/get_linkedin_posts_browserbase.py "https://www.linkedin.com/in/johndoe"

# Scrape company page
python deprecated/get_linkedin_posts_browserbase.py "https://www.linkedin.com/company/google"

# Scrape 20 posts
python deprecated/get_linkedin_posts_browserbase.py johndoe --max-posts 20

# Save to custom file
python deprecated/get_linkedin_posts_browserbase.py johndoe -o results/johndoe_posts.json

# Reset session (re-authenticate)
python deprecated/get_linkedin_posts_browserbase.py --reset-session
```

### Options

| Option | Description |
|--------|-------------|
| `profile` | LinkedIn username or full profile URL |
| `--max-posts`, `-n` | Maximum number of posts to scrape (default: 10) |
| `--output`, `-o` | Output file path (default: results/linkedin_posts.json) |
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
