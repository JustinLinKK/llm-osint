# Person search: search (DuckDuckGo) and download (fetch + save HTML) are in separate modules.
# Use search.search_pages() and download.download_pages() separately, or workflow.run_workflow() for both.
from .download import PageResult, download_pages
from .search import search_pages
from .workflow import run_workflow

__all__ = ["PageResult", "download_pages", "run_workflow", "search_pages"]
