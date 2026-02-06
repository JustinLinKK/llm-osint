#!/usr/bin/env python3
"""
Fetch posts/activity from a LinkedIn user profile via Browserbase.
Uses Browserbase cloud browser + Playwright for automation.

Usage:
    python get_linkedin_posts_browserbase.py <profile_url_or_username> [--max-posts N] [--output FILE]

Environment (via .env file or shell):
    BROWSERBASE_API_KEY: Your Browserbase API key (required)
    BROWSERBASE_PROJECT_ID: Your Browserbase project ID (required)
    LINKEDIN_EMAIL: Your LinkedIn email (required for login)
    LINKEDIN_PASSWORD: Your LinkedIn password (required for login)

Requirements:
    pip install browserbase playwright
    playwright install chromium
"""

import argparse
import json
import os
import sys
import time
from contextlib import redirect_stdout
from pathlib import Path
from datetime import datetime


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
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip()
                    if (value.startswith('"') and value.endswith('"')) or \
                       (value.startswith("'") and value.endswith("'")):
                        value = value[1:-1]
                    if key not in os.environ:
                        os.environ[key] = value


load_env_file()


def get_browserbase_credentials():
    """Get Browserbase credentials from environment."""
    api_key = os.environ.get("BROWSERBASE_API_KEY")
    project_id = os.environ.get("BROWSERBASE_PROJECT_ID")

    if not api_key or not project_id:
        print("Error: BROWSERBASE_API_KEY and BROWSERBASE_PROJECT_ID must be set.", file=sys.stderr)
        print("Add them to your .env file or set as environment variables.", file=sys.stderr)
        sys.exit(1)

    return api_key, project_id


# File to store persistent context ID
CONTEXT_FILE = Path(__file__).parent / ".linkedin_context_id"


def get_or_create_context(bb, project_id):
    """
    Get existing persistent context or create a new one.
    The context preserves cookies/session across runs.
    """
    context_id = None

    # Try to load existing context ID
    if CONTEXT_FILE.exists():
        context_id = CONTEXT_FILE.read_text().strip()
        print(f"Found saved context ID: {context_id}")

        # Verify context still exists
        try:
            context = bb.contexts.retrieve(context_id)
            print(f"Using existing context (preserves login session)")
            return context_id
        except Exception as e:
            print(f"Saved context no longer valid: {e}")
            context_id = None

    # Create new context
    print("Creating new persistent context...")
    context = bb.contexts.create(project_id=project_id)
    context_id = context.id
    print(f"New context created: {context_id}")

    # Save context ID for future runs
    CONTEXT_FILE.write_text(context_id)
    print(f"Context ID saved to {CONTEXT_FILE}")

    return context_id


def is_logged_in(page):
    """Check if already logged into LinkedIn."""
    current_url = page.url

    # If on feed or profile pages, we're logged in
    if "/feed" in current_url or "/mynetwork" in current_url:
        return True

    # Check for logged-in indicators on the page
    logged_in_indicators = [
        '[data-control-name="feed"]',
        '.global-nav__me',
        '.feed-identity-module',
        'nav[aria-label="Primary"]'
    ]

    for indicator in logged_in_indicators:
        if page.locator(indicator).count() > 0:
            return True

    return False


def get_linkedin_credentials():
    """Get LinkedIn credentials from environment."""
    email = os.environ.get("LINKEDIN_EMAIL")
    password = os.environ.get("LINKEDIN_PASSWORD")

    if not email or not password:
        print("Error: LINKEDIN_EMAIL and LINKEDIN_PASSWORD must be set.", file=sys.stderr)
        print("Add them to your .env file or set as environment variables.", file=sys.stderr)
        sys.exit(1)

    return email, password


def linkedin_login(page, email, password):
    """
    Perform LinkedIn login.
    Returns True if login successful, False otherwise.
    """
    print("Logging into LinkedIn...")

    try:
        # Go to LinkedIn login page
        print("Loading login page...")
        page.goto("https://www.linkedin.com/login", wait_until="domcontentloaded", timeout=60000)

        # Wait for the login form to be visible
        page.wait_for_selector('input[name="session_key"], input#username', timeout=30000)
        time.sleep(2)

        # Fill in email
        print("Entering credentials...")
        email_input = page.locator('input[name="session_key"], input#username').first
        if email_input.count() > 0:
            email_input.fill(email)
        else:
            print("Error: Could not find email input field.", file=sys.stderr)
            return False

        # Fill in password
        password_input = page.locator('input[name="session_password"], input#password').first
        if password_input.count() > 0:
            password_input.fill(password)
        else:
            print("Error: Could not find password input field.", file=sys.stderr)
            return False

        time.sleep(1)

        # Click sign in button and wait for navigation
        sign_in_btn = page.locator('button[type="submit"]').first
        if sign_in_btn.count() == 0:
            sign_in_btn = page.locator('button:has-text("Sign in")').first

        if sign_in_btn.count() > 0:
            print("Clicking sign in button...")
            # Use click with force in case of overlay issues
            sign_in_btn.click(force=True)
        else:
            print("Error: Could not find sign in button.", file=sys.stderr)
            return False

        # Wait for URL to change from login page
        print("Waiting for login to complete...")
        try:
            # Wait up to 30 seconds for URL to change
            page.wait_for_url(lambda url: "/login" not in url and "/uas" not in url, timeout=30000)
            print("Navigation detected!")
        except Exception:
            # URL didn't change, wait a bit more and check manually
            print("No redirect detected, waiting longer...")
            time.sleep(10)

        # Check if login was successful (redirected to feed or profile)
        current_url = page.url
        print(f"Current URL after login: {current_url}")

        if "/feed" in current_url or "/in/" in current_url or "/mynetwork" in current_url:
            print("Login successful!")
            return True

        # Check for security verification
        if "/checkpoint" in current_url or "challenge" in current_url:
            print("Warning: LinkedIn security verification required.", file=sys.stderr)
            print("You may need to verify your account manually.", file=sys.stderr)
            print("Check your Browserbase session dashboard to complete verification.")
            print("Waiting up to 180 seconds for manual verification...")

            # Poll URL every 5 seconds instead of waiting all at once
            max_wait = 180
            waited = 0
            while waited < max_wait:
                time.sleep(5)
                waited += 5
                current_url = page.url
                print(f"  [{waited}s] Checking... URL: {current_url[:60]}...")

                if "/feed" in current_url or "/in/" in current_url or "/mynetwork" in current_url:
                    print("Verification completed!")
                    return True

                # If no longer on checkpoint, might be success
                if "/checkpoint" not in current_url and "/challenge" not in current_url:
                    print(f"Redirected to: {current_url}")
                    return True

            print(f"Timeout. Still at: {current_url}")
            return False

        # Still on login page - check for error messages
        if "/login" in current_url or "/uas" in current_url:
            # Check for specific error elements
            error_selectors = [
                '#error-for-username',
                '#error-for-password',
                '.alert-error',
                '.form__label--error',
                '[data-error="true"]'
            ]
            for selector in error_selectors:
                error_el = page.locator(selector).first
                if error_el.count() > 0 and error_el.is_visible():
                    error_text = error_el.inner_text().strip()
                    if error_text:
                        print(f"Login error: {error_text}", file=sys.stderr)
                        return False

            # No specific error found but still on login page
            print("Login failed: Still on login page. Check credentials.", file=sys.stderr)
            return False

        # Unknown page - might still be loading or redirect
        print("Warning: Login status uncertain, proceeding anyway...")
        return True

    except Exception as e:
        print(f"Login error: {str(e)}", file=sys.stderr)
        return False


def normalize_linkedin_url(profile_input):
    """Convert username or URL to full LinkedIn profile URL."""
    if profile_input.startswith("http"):
        # Already a URL
        url = profile_input.rstrip("/")
        if "/in/" not in url and "/company/" not in url:
            print("Error: Invalid LinkedIn URL. Expected /in/ or /company/ in URL.", file=sys.stderr)
            sys.exit(1)
        return url
    else:
        # Assume it's a username
        username = profile_input.lstrip("@").strip()
        return f"https://www.linkedin.com/in/{username}"


def scrape_linkedin_profile(profile_url, max_posts=10):
    """
    Scrape posts from a LinkedIn profile using Browserbase.
    """
    try:
        from browserbase import Browserbase
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("Error: Required packages not installed.", file=sys.stderr)
        print("Run: pip install browserbase playwright", file=sys.stderr)
        print("Then: playwright install chromium", file=sys.stderr)
        sys.exit(1)

    api_key, project_id = get_browserbase_credentials()
    linkedin_email, linkedin_password = get_linkedin_credentials()

    # Initialize Browserbase
    bb = Browserbase(api_key=api_key)

    # Get or create persistent context
    context_id = get_or_create_context(bb, project_id)

    print(f"Creating Browserbase session with persistent context...")
    session = bb.sessions.create(project_id=project_id, browser_settings={"context": {"id": context_id, "persist": True}})
    print(f"Session created: {session.id}")

    posts = []

    try:
        with sync_playwright() as pw:
            # Connect to Browserbase session
            browser = pw.chromium.connect_over_cdp(session.connect_url)
            context = browser.contexts[0]
            page = context.pages[0]

            # First, navigate to LinkedIn to check if already logged in
            print("Checking login status...")
            page.goto("https://www.linkedin.com/feed/", wait_until="domcontentloaded", timeout=60000)
            time.sleep(3)

            # Check if already logged in (from persistent context)
            if is_logged_in(page):
                print("Already logged in (session restored from persistent context)")
            else:
                print("Not logged in, performing login...")
                # Login to LinkedIn
                if not linkedin_login(page, linkedin_email, linkedin_password):
                    print("Error: Failed to login to LinkedIn.", file=sys.stderr)
                    return None

            # Navigate to profile
            print(f"Navigating to {profile_url}...")
            page.goto(profile_url, wait_until="domcontentloaded", timeout=60000)

            # Wait for page to load
            print("Waiting for page to load...")
            time.sleep(5)

            # Check if we still hit a login wall (session expired or blocked)
            if is_login_required(page):
                print("Error: Still seeing login wall after authentication.", file=sys.stderr)
                print("Your account may be blocked or require verification.", file=sys.stderr)
                return None

            # Check if profile exists
            if page.locator("text=Page not found").count() > 0:
                print(f"Error: Profile not found at {profile_url}", file=sys.stderr)
                return None

            # Extract user info
            user_info = extract_linkedin_user_info(page, profile_url)
            print(f"Found profile: {user_info.get('name', 'Unknown')}")

            # Navigate to activity/posts section
            activity_url = get_activity_url(profile_url)
            print(f"Navigating to activity page...")
            page.goto(activity_url, wait_until="domcontentloaded", timeout=60000)
            time.sleep(5)

            # Scroll and collect posts
            print(f"Collecting posts (max: {max_posts})...")
            collected = 0
            scroll_attempts = 0
            max_scroll_attempts = 10
            seen_urns = set()

            while collected < max_posts and scroll_attempts < max_scroll_attempts:
                # Find post elements
                post_elements = page.locator('[data-urn*="activity"], .feed-shared-update-v2, .occludable-update').all()

                for post_el in post_elements:
                    if collected >= max_posts:
                        break

                    try:
                        post = extract_linkedin_post(post_el)
                        post_id = post.get("id") or post.get("text", "")[:50]
                        if post and post_id not in seen_urns:
                            seen_urns.add(post_id)
                            posts.append(post)
                            collected += 1
                            print(f"  Collected {collected}/{max_posts} posts", end="\r")
                    except Exception:
                        continue

                # Scroll down to load more
                page.evaluate("window.scrollBy(0, 1000)")
                time.sleep(2)
                scroll_attempts += 1

            print(f"\nCollected {len(posts)} posts total.")

            return {
                "user": user_info,
                "post_count": len(posts),
                "posts": posts,
                "scraped_at": datetime.utcnow().isoformat() + "Z"
            }

    finally:
        print("Closing Browserbase session...")
        bb.sessions.update(session.id, status="REQUEST_RELEASE", project_id=project_id)


def is_login_required(page):
    """Check if LinkedIn is requiring login."""
    login_indicators = [
        'text="Sign in"',
        'text="Join now"',
        '[data-tracking-control-name="auth_wall"]',
        '.authwall-join-form'
    ]
    for indicator in login_indicators:
        if page.locator(indicator).count() > 0:
            return True
    return False


def get_activity_url(profile_url):
    """Get the activity/posts URL for a LinkedIn profile."""
    if "/company/" in profile_url:
        return f"{profile_url}/posts/"
    else:
        return f"{profile_url}/recent-activity/all/"


def extract_linkedin_user_info(page, profile_url):
    """Extract user information from LinkedIn profile page."""
    info = {"profile_url": profile_url}

    try:
        # Extract username from URL
        if "/in/" in profile_url:
            info["username"] = profile_url.split("/in/")[-1].rstrip("/").split("?")[0]
        elif "/company/" in profile_url:
            info["company_name"] = profile_url.split("/company/")[-1].rstrip("/").split("?")[0]
            info["is_company"] = True

        # Try to get name
        name_selectors = [
            'h1.text-heading-xlarge',
            '.pv-top-card--list li:first-child',
            'h1.top-card-layout__title',
            '.top-card__title'
        ]
        for selector in name_selectors:
            name_el = page.locator(selector).first
            if name_el.count() > 0:
                info["name"] = name_el.inner_text().strip()
                break

        # Try to get headline/title
        headline_selectors = [
            '.text-body-medium.break-words',
            '.pv-top-card--headline',
            '.top-card__subline-row:first-child'
        ]
        for selector in headline_selectors:
            headline_el = page.locator(selector).first
            if headline_el.count() > 0:
                info["headline"] = headline_el.inner_text().strip()
                break

        # Try to get location
        location_selectors = [
            '.text-body-small.inline.t-black--light.break-words',
            '.pv-top-card--location',
            '.top-card__subline-row .top-card__flavor'
        ]
        for selector in location_selectors:
            loc_el = page.locator(selector).first
            if loc_el.count() > 0:
                info["location"] = loc_el.inner_text().strip()
                break

        # Try to get connections/followers
        connections_el = page.locator('text=/\\d+\\+? connections/i').first
        if connections_el.count() > 0:
            info["connections"] = connections_el.inner_text().strip()

        followers_el = page.locator('text=/\\d+[KMB]? followers/i').first
        if followers_el.count() > 0:
            info["followers"] = followers_el.inner_text().strip()

    except Exception:
        pass

    return info


def extract_linkedin_post(post_element):
    """Extract data from a single LinkedIn post element."""
    post = {}

    try:
        # Try to get post URN/ID
        urn = post_element.get_attribute("data-urn")
        if urn:
            post["id"] = urn

        # Get post text
        text_selectors = [
            '.feed-shared-update-v2__description',
            '.break-words span[dir="ltr"]',
            '.feed-shared-text',
            '.update-components-text'
        ]
        for selector in text_selectors:
            text_el = post_element.locator(selector).first
            if text_el.count() > 0:
                post["text"] = text_el.inner_text().strip()
                break

        if not post.get("text"):
            # Fallback: get any visible text content
            post["text"] = post_element.inner_text()[:500].strip()

        # Get author info
        author_selectors = [
            '.update-components-actor__name',
            '.feed-shared-actor__name',
            '.update-components-actor__title'
        ]
        for selector in author_selectors:
            author_el = post_element.locator(selector).first
            if author_el.count() > 0:
                post["author"] = author_el.inner_text().strip()
                break

        # Get timestamp
        time_selectors = [
            '.update-components-actor__sub-description',
            '.feed-shared-actor__sub-description',
            'time'
        ]
        for selector in time_selectors:
            time_el = post_element.locator(selector).first
            if time_el.count() > 0:
                time_text = time_el.inner_text().strip()
                if time_text:
                    post["posted_at"] = time_text
                    break

        # Get engagement metrics
        post["metrics"] = {}

        likes_el = post_element.locator('text=/\\d+[KM]? likes?/i, text=/\\d+[KM]? reactions?/i').first
        if likes_el.count() > 0:
            post["metrics"]["likes"] = likes_el.inner_text().strip()

        comments_el = post_element.locator('text=/\\d+[KM]? comments?/i').first
        if comments_el.count() > 0:
            post["metrics"]["comments"] = comments_el.inner_text().strip()

        reposts_el = post_element.locator('text=/\\d+[KM]? reposts?/i').first
        if reposts_el.count() > 0:
            post["metrics"]["reposts"] = reposts_el.inner_text().strip()

    except Exception:
        pass

    # Only return if we have some content
    return post if post.get("text") or post.get("id") else None


def cli_main():
    parser = argparse.ArgumentParser(
        description="Scrape posts from LinkedIn profile via Browserbase"
    )
    parser.add_argument(
        "profile",
        nargs="?",
        help="LinkedIn profile URL or username (e.g., 'johndoe' or 'https://linkedin.com/in/johndoe')"
    )
    parser.add_argument(
        "--max-posts", "-n", type=int, default=10,
        help="Maximum number of posts to collect (default: 10)"
    )
    parser.add_argument(
        "--output", "-o", type=str, default="linkedin_posts.json",
        help="Output file path (JSON format)"
    )
    parser.add_argument(
        "--reset-session", action="store_true",
        help="Delete saved session and re-authenticate"
    )

    args = parser.parse_args()

    # Handle --reset-session flag
    if args.reset_session:
        if CONTEXT_FILE.exists():
            CONTEXT_FILE.unlink()
            print("Session reset. You will need to re-authenticate on next run.")
        else:
            print("No saved session to reset.")
        if not args.profile:
            return

    if not args.profile:
        parser.error("profile is required unless using --reset-session")

    profile_url = normalize_linkedin_url(args.profile)
    print(f"Scraping LinkedIn profile via Browserbase...\n")
    print(f"Profile URL: {profile_url}\n")

    result = scrape_linkedin_profile(profile_url, args.max_posts)

    if result is None:
        sys.exit(1)

    json_output = json.dumps(result, indent=2, ensure_ascii=False)

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
    profile_input = (input_data.get("profile") or "").strip()
    if not profile_input:
        emit_mcp_result(False, error="Missing required input: profile")
        return

    max_posts = input_data.get("max_posts", 10)
    reset_session = bool(input_data.get("reset_session", False))

    if reset_session and CONTEXT_FILE.exists():
        CONTEXT_FILE.unlink()

    try:
        with redirect_stdout(sys.stderr):
            profile_url = normalize_linkedin_url(profile_input)
            result = scrape_linkedin_profile(profile_url, max_posts)
        if result is None:
            emit_mcp_result(False, error="Failed to scrape LinkedIn profile")
            return
        emit_mcp_result(True, result=result)
    except SystemExit as exc:
        emit_mcp_result(False, error=f"Exited with code {exc.code}")
    except Exception as exc:
        emit_mcp_result(False, error=str(exc))


if __name__ == "__main__":
    payload = read_stdin_payload()
    if payload and "input" in payload:
        mcp_main(payload)
    else:
        cli_main()
