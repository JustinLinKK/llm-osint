#!/usr/bin/env python3
"""
Download LinkedIn profile and activity pages as HTML via Browserbase.
Uses Browserbase cloud browser + Playwright for automation.

Usage:
    python download_linkedin_html.py <profile_url_or_username> [--output-dir DIR]

Environment (via .env file or shell):
    BROWSERBASE_API_KEY: Your Browserbase API key (required)
    BROWSERBASE_PROJECT_ID: Your Browserbase project ID (required)
    LINKEDIN_EMAIL: Your LinkedIn email (required for login)
    LINKEDIN_PASSWORD: Your LinkedIn password (required for login)
    LINKEDIN_CONTEXT_ID: Existing Browserbase context ID to reuse (optional)

Requirements:
    pip install browserbase playwright
    playwright install chromium
"""

import argparse
import os
import sys
import time
from pathlib import Path
from datetime import datetime


def load_env_file():
    """Load environment variables from .env file."""
    env_path = Path(__file__).parent / ".env"
    if not env_path.exists():
        return

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


def get_linkedin_credentials():
    """Get LinkedIn credentials from environment."""
    email = os.environ.get("LINKEDIN_EMAIL")
    password = os.environ.get("LINKEDIN_PASSWORD")

    if not email or not password:
        print("Error: LINKEDIN_EMAIL and LINKEDIN_PASSWORD must be set.", file=sys.stderr)
        print("Add them to your .env file or set as environment variables.", file=sys.stderr)
        sys.exit(1)

    return email, password


# File to store persistent context ID
CONTEXT_FILE = Path(__file__).parent / ".linkedin_context_id"


def get_or_create_context(bb, project_id):
    """
    Get existing persistent context or create a new one.
    The context preserves cookies/session across runs.
    """
    def validate_context_id(context_id, source_label):
        if not context_id:
            return None
        try:
            bb.contexts.retrieve(context_id)
            print(f"Using context from {source_label}: {context_id}")
            return context_id
        except Exception as e:
            print(f"Context from {source_label} is not valid ({context_id}): {e}")
            return None

    # 1) Prefer explicit env var so containerized runs can pin context deterministically.
    env_context_id = os.environ.get("LINKEDIN_CONTEXT_ID", "").strip()
    valid_context_id = validate_context_id(env_context_id, "env LINKEDIN_CONTEXT_ID")
    if valid_context_id:
        # Keep local cache in sync for local runs/tools that rely on this file.
        CONTEXT_FILE.write_text(valid_context_id)
        return valid_context_id

    # 2) Fallback to saved local context id.
    file_context_id = ""
    if CONTEXT_FILE.exists():
        file_context_id = CONTEXT_FILE.read_text().strip()
        valid_context_id = validate_context_id(file_context_id, f"file {CONTEXT_FILE}")
        if valid_context_id:
            return valid_context_id

    # Create new context
    print("Creating new persistent context...")
    context = bb.contexts.create(project_id=project_id)
    new_context_id = context.id
    print(f"New context created: {new_context_id}")

    # Save context ID for future runs
    CONTEXT_FILE.write_text(new_context_id)
    print(f"Context ID saved to {CONTEXT_FILE}")

    return new_context_id


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

        # Click sign in button
        sign_in_btn = page.locator('button[type="submit"]').first
        if sign_in_btn.count() == 0:
            sign_in_btn = page.locator('button:has-text("Sign in")').first

        if sign_in_btn.count() > 0:
            print("Clicking sign in button...")
            sign_in_btn.click(force=True)
        else:
            print("Error: Could not find sign in button.", file=sys.stderr)
            return False

        # Wait for URL to change from login page
        print("Waiting for login to complete...")
        try:
            page.wait_for_url(lambda url: "/login" not in url and "/uas" not in url, timeout=30000)
            print("Navigation detected!")
        except Exception:
            print("No redirect detected, waiting longer...")
            time.sleep(10)

        # Check if login was successful
        current_url = page.url
        print(f"Current URL after login: {current_url}")

        if "/feed" in current_url or "/in/" in current_url or "/mynetwork" in current_url:
            print("Login successful!")
            return True

        # Check for security verification
        if "/checkpoint" in current_url or "challenge" in current_url:
            print("Warning: LinkedIn security verification required.", file=sys.stderr)
            print("Waiting up to 180 seconds for manual verification...")

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

                if "/checkpoint" not in current_url and "/challenge" not in current_url:
                    print(f"Redirected to: {current_url}")
                    return True

            print(f"Timeout. Still at: {current_url}")
            return False

        # Still on login page
        if "/login" in current_url or "/uas" in current_url:
            print("Login failed: Still on login page. Check credentials.", file=sys.stderr)
            return False

        print("Warning: Login status uncertain, proceeding anyway...")
        return True

    except Exception as e:
        print(f"Login error: {str(e)}", file=sys.stderr)
        return False


def normalize_linkedin_url(profile_input):
    """Convert username or URL to full LinkedIn profile URL."""
    if profile_input.startswith("http"):
        url = profile_input.rstrip("/")
        if "/in/" not in url and "/company/" not in url:
            print("Error: Invalid LinkedIn URL. Expected /in/ or /company/ in URL.", file=sys.stderr)
            sys.exit(1)
        return url
    else:
        username = profile_input.lstrip("@").strip()
        return f"https://www.linkedin.com/in/{username}"


def get_activity_url(profile_url):
    """Get the activity/posts URL for a LinkedIn profile."""
    if "/company/" in profile_url:
        return f"{profile_url}/posts/"
    else:
        return f"{profile_url}/recent-activity/all/"


def download_linkedin_html(profile_url, output_dir):
    """
    Download LinkedIn profile and activity pages as HTML.
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

    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Initialize Browserbase
    bb = Browserbase(api_key=api_key)

    # Get or create persistent context
    context_id = get_or_create_context(bb, project_id)

    print(f"Creating Browserbase session with persistent context...")
    session = bb.sessions.create(project_id=project_id, browser_settings={"context": {"id": context_id, "persist": True}})
    print(f"Session created: {session.id}")

    try:
        with sync_playwright() as pw:
            # Connect to Browserbase session
            browser = pw.chromium.connect_over_cdp(session.connect_url)
            context = browser.contexts[0]
            page = context.pages[0]

            # Check if already logged in
            print("Checking login status...")
            page.goto("https://www.linkedin.com/feed/", wait_until="domcontentloaded", timeout=60000)
            time.sleep(3)

            if is_logged_in(page):
                print("Already logged in (session restored from persistent context)")
            else:
                print("Not logged in, performing login...")
                if not linkedin_login(page, linkedin_email, linkedin_password):
                    print("Error: Failed to login to LinkedIn.", file=sys.stderr)
                    return False

            # Navigate to profile page
            print(f"\n{'='*60}")
            print(f"Navigating to profile: {profile_url}")
            print(f"{'='*60}")
            page.goto(profile_url, wait_until="domcontentloaded", timeout=60000)
            time.sleep(5)

            # Scroll to load profile sections
            print("Scrolling to load profile sections...")
            for i in range(3):
                page.evaluate("window.scrollBy(0, 800)")
                time.sleep(2)

            # Download profile page HTML
            profile_html = page.content()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            profile_filename = output_path / f"profile_{timestamp}.html"
            
            with open(profile_filename, "w", encoding="utf-8") as f:
                f.write(profile_html)
            
            print(f"✓ Profile page saved: {profile_filename}")
            print(f"  Size: {len(profile_html):,} bytes")

            # Navigate to activity page
            activity_url = get_activity_url(profile_url)
            print(f"\n{'='*60}")
            print(f"Navigating to activity: {activity_url}")
            print(f"{'='*60}")
            page.goto(activity_url, wait_until="domcontentloaded", timeout=60000)
            time.sleep(5)

            # Scroll to load posts
            print("Scrolling to load posts...")
            scroll_count = 10
            for i in range(scroll_count):
                page.evaluate("window.scrollBy(0, 1000)")
                time.sleep(2)
                print(f"  Scroll {i+1}/{scroll_count}", end="\r")
            print()

            # Download activity page HTML
            activity_html = page.content()
            activity_filename = output_path / f"activity_{timestamp}.html"
            
            with open(activity_filename, "w", encoding="utf-8") as f:
                f.write(activity_html)
            
            print(f"✓ Activity page saved: {activity_filename}")
            print(f"  Size: {len(activity_html):,} bytes")

            print(f"\n{'='*60}")
            print("Download complete!")
            print(f"{'='*60}")
            print(f"Output directory: {output_path.absolute()}")
            print(f"  - {profile_filename.name}")
            print(f"  - {activity_filename.name}")

            return True

    finally:
        print("\nClosing Browserbase session...")
        bb.sessions.update(session.id, status="REQUEST_RELEASE", project_id=project_id)


def main():
    parser = argparse.ArgumentParser(
        description="Download LinkedIn profile and activity pages as HTML"
    )
    parser.add_argument(
        "profile",
        nargs="?",
        help="LinkedIn profile URL or username (e.g., 'johndoe' or 'https://linkedin.com/in/johndoe')"
    )
    parser.add_argument(
        "--output-dir", "-o", type=str, default="linkedin_html",
        help="Output directory for HTML files (default: linkedin_html)"
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
    
    success = download_linkedin_html(profile_url, args.output_dir)
    
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
