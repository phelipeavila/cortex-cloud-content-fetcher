#!/usr/bin/env python3
"""
Local Compliance Data Fetcher

A standalone script to fetch compliance data from Cortex Cloud APIs.
This is a local version of the XSOAR scripts for testing and development.

Usage:
    python local_compliance_fetcher.py           # Normal mode (progress bars only)
    python local_compliance_fetcher.py --debug   # Debug mode (verbose output)
    python local_compliance_fetcher.py --dry-run # Don't push to collector

Requirements:
    pip install requests python-dotenv rich
"""

import argparse
import json
import os
import sys
import time
import csv
import random
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import requests
    from dotenv import load_dotenv
except ImportError:
    print("Missing dependencies. Install with:")
    print("  pip install requests python-dotenv rich")
    sys.exit(1)

# Rich imports with fallback
try:
    from rich.console import Console
    from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn, TaskProgressColumn, MofNCompleteColumn
    from rich.table import Table
    from rich.panel import Panel
    from rich.text import Text
    from rich import box
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    print("⚠ Rich not installed. Install with: pip install rich")
    print("  Falling back to basic output.\n")


# =============================================================================
# OUTPUT MANAGER
# =============================================================================

class Output:
    """Manages output based on verbosity level."""
    
    def __init__(self, debug: bool = False):
        self.debug = debug
        self.console = Console() if RICH_AVAILABLE else None
        self._retry_count = 0
    
    def _print(self, msg: str):
        """Print using Rich if available, else plain print."""
        if self.console:
            self.console.print(msg)
        else:
            # Strip rich markup for plain output
            import re
            plain = re.sub(r'\[/?[^\]]+\]', '', msg)
            print(plain)
    
    def header(self, title: str):
        """Print a stage header."""
        if self.console and RICH_AVAILABLE:
            self.console.print()
            self.console.print(Panel(title, style="bold cyan", box=box.DOUBLE))
        else:
            print("\n" + "="*60)
            print(f"  {title}")
            print("="*60)
    
    def info(self, msg: str):
        """Always shown - important info."""
        self._print(msg)
    
    def success(self, msg: str):
        """Always shown - success message."""
        self._print(f"[green]✓ {msg}[/green]")
    
    def warning(self, msg: str):
        """Only shown with --debug."""
        if self.debug:
            self._print(f"[yellow]⚠ {msg}[/yellow]")
    
    def error(self, msg: str):
        """Always shown - error message."""
        self._print(f"[red]❌ {msg}[/red]")
    
    def debug_msg(self, msg: str):
        """Only shown with --debug."""
        if self.debug:
            self._print(f"[dim]{msg}[/dim]")
    
    def retry(self, endpoint: str, status_info: str, error_type: str, delay: float, attempt: int, max_retries: int):
        """Log retry attempt - only shown with --debug."""
        self._retry_count += 1
        if self.debug:
            self._print(f"[yellow]  ⚠ {endpoint} failed{status_info}: {error_type}. Retrying in {delay:.1f}s ({attempt}/{max_retries})[/yellow]")
    
    def table(self, title: str, columns: List[str], rows: List[List[str]]):
        """Display a table."""
        if self.console and RICH_AVAILABLE:
            table = Table(title=title, box=box.ROUNDED, show_header=True, header_style="bold")
            for col in columns:
                table.add_column(col)
            for row in rows:
                table.add_row(*[str(c) for c in row])
            self.console.print(table)
        else:
            print(f"\n{title}")
            print("-" * 60)
            print(" | ".join(columns))
            print("-" * 60)
            for row in rows:
                print(" | ".join(str(c) for c in row))
            print()
    
    def final_summary(self, stats: dict, elapsed: float):
        """Display final summary."""
        if self.console and RICH_AVAILABLE:
            table = Table(title="Execution Summary", box=box.ROUNDED, show_header=False)
            table.add_column("Metric", style="cyan")
            table.add_column("Value", style="green")
            
            table.add_row("Total Time", f"{elapsed:.1f} seconds")
            table.add_row("Anchors", str(stats.get('anchors', 0)))
            table.add_row("Controls", str(stats.get('controls', 0)))
            table.add_row("Assets", f"{stats.get('assets', 0):,}")
            table.add_row("Summaries", str(stats.get('summaries', 0)))
            
            if self._retry_count > 0:
                table.add_row("API Retries", str(self._retry_count))
            
            assets_status = stats.get('assets_push', 'skipped')
            summaries_status = stats.get('summaries_push', 'skipped')
            table.add_row("Assets Push", assets_status)
            table.add_row("Summaries Push", summaries_status)
            
            self.console.print()
            self.console.print(table)
        else:
            print("\n" + "="*60)
            print("  COMPLETE")
            print("="*60)
            print(f"  Total time: {elapsed:.1f} seconds")
            print(f"  Anchors: {stats.get('anchors', 0)}")
            print(f"  Controls: {stats.get('controls', 0)}")
            print(f"  Assets: {stats.get('assets', 0):,}")
            print(f"  Summaries: {stats.get('summaries', 0)}")
            if self._retry_count > 0:
                print(f"  API Retries: {self._retry_count}")


# Global output instance
out: Output = None


# =============================================================================
# PROGRESS BARS
# =============================================================================

def create_progress() -> Progress:
    """Create a Rich Progress instance."""
    if not RICH_AVAILABLE:
        return None
    return Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(bar_width=40),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        TextColumn("•"),
        TimeElapsedColumn(),
        console=out.console if out else Console(),
        transient=False
    )


# =============================================================================
# CONFIGURATION
# =============================================================================

class Config:
    """Configuration loaded from environment variables."""
    
    def __init__(self, dry_run_override: bool = None):
        # Load from config.env file
        env_file = Path(__file__).parent / "config.env"
        if env_file.exists():
            load_dotenv(env_file)
            out.debug_msg(f"Loaded config from {env_file}")
        else:
            out.warning(f"No config.env found at {env_file}")
        
        # Required API credentials
        self.API_URL = os.getenv("API_URL", "").rstrip("/")
        self.API_KEY = os.getenv("API_KEY", "")
        self.API_ID = os.getenv("API_ID", "")
        
        # Tenant URL (for building collector URL)
        self.TENANT_EXTERNAL_URL = os.getenv("TENANT_EXTERNAL_URL", "")
        
        # HTTP Collector API keys
        self.COMPLIANCE_API_KEY = os.getenv("COMPLIANCE_API_KEY", "")
        self.CONTROLS_API_KEY = os.getenv("CONTROLS_API_KEY", "")
        
        # Build collector URL
        if self.TENANT_EXTERNAL_URL:
            self.COLLECTOR_URL = f"https://api-{self.TENANT_EXTERNAL_URL}/logs/v1/event"
        else:
            self.COLLECTOR_URL = ""
        
        # Assessments configuration
        self.ASSESSMENTS = json.loads(os.getenv("ASSESSMENTS", "[]"))
        
        # Processing options
        self.PAGE_SIZE = int(os.getenv("PAGE_SIZE", "100"))
        self.BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
        self.DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
        if dry_run_override is not None:
            self.DRY_RUN = dry_run_override
        self.MAX_ANCHORS = int(os.getenv("MAX_ANCHORS", "0"))
        self.PARALLEL_WORKERS = int(os.getenv("PARALLEL_WORKERS", "5"))
        
        # Output options
        self.OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "./output"))
        self.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        
        self.validate()
    
    def validate(self):
        """Validate required configuration."""
        errors = []
        if not self.API_URL:
            errors.append("API_URL is required")
        if not self.API_KEY:
            errors.append("API_KEY is required")
        if not self.API_ID:
            errors.append("API_ID is required")
        if not self.ASSESSMENTS:
            errors.append("ASSESSMENTS is required (JSON array)")
        
        if errors:
            out.error("Configuration errors:")
            for e in errors:
                out.info(f"   - {e}")
            out.info("\nPlease check your config.env file.")
            sys.exit(1)
        
        out.debug_msg(f"API URL: {self.API_URL}")
        out.debug_msg(f"Assessments: {len(self.ASSESSMENTS)} standard(s)")
        out.debug_msg(f"Output: {self.OUTPUT_DIR}")
        out.debug_msg(f"Dry run: {self.DRY_RUN}")


# =============================================================================
# API CLIENT
# =============================================================================

class CortexAPIClient:
    """Client for Cortex Cloud Compliance APIs with automatic retry."""
    
    MAX_RETRIES = 5
    BASE_DELAY = 2.0
    RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
    
    def __init__(self, config: Config):
        self.config = config
        self.base_url = config.API_URL
        self.headers = {
            "x-xdr-auth-id": str(config.API_ID),
            "Authorization": config.API_KEY,
            "Content-Type": "application/json"
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def post(self, endpoint: str, body: dict, timeout: int = 120) -> dict:
        """Make a POST request with automatic retry on transient errors."""
        url = f"{self.base_url}{endpoint}"
        last_exception = None
        
        for attempt in range(self.MAX_RETRIES + 1):
            try:
                response = self.session.post(url, json=body, timeout=timeout)
                
                if response.status_code in self.RETRYABLE_STATUS_CODES:
                    response.raise_for_status()
                
                response.raise_for_status()
                return response.json()
                
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout,
                    requests.exceptions.ChunkedEncodingError) as e:
                last_exception = e
                
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code not in self.RETRYABLE_STATUS_CODES:
                    out.error(f"HTTP Error: {e}")
                    out.debug_msg(f"Response: {e.response.text[:500]}")
                    raise
                last_exception = e
                
            except requests.exceptions.RequestException as e:
                last_exception = e
            
            if attempt < self.MAX_RETRIES:
                delay = (self.BASE_DELAY * (2 ** attempt)) + random.uniform(0.1, 1.0)
                error_type = type(last_exception).__name__
                status_info = ""
                if hasattr(last_exception, 'response') and last_exception.response is not None:
                    status_info = f" (HTTP {last_exception.response.status_code})"
                
                out.retry(endpoint, status_info, error_type, delay, attempt + 1, self.MAX_RETRIES)
                time.sleep(delay)
        
        out.error(f"Failed after {self.MAX_RETRIES} retries: {endpoint}")
        if last_exception:
            raise last_exception
        raise Exception(f"Failed after {self.MAX_RETRIES} retries: {endpoint}")


# =============================================================================
# STAGE 1: GET ANCHORS
# =============================================================================

def get_all_assessment_results(client: CortexAPIClient) -> List[dict]:
    """Fetch all assessment results from the API."""
    body = {"request_data": {"filters": []}}
    response = client.post("/public_api/v1/compliance/get_assessment_results", body)
    return response.get("reply", {}).get("data", [])


def find_anchor_for_pair(all_results: List[dict], profile_name: str, standard_name: str) -> Optional[dict]:
    """Find the latest assessment anchor for a profile/standard pair."""
    out.debug_msg(f"  Finding anchor: {profile_name} / {standard_name[:50]}...")
    
    matches = [
        r for r in all_results
        if (str(r.get("TYPE", "")).lower() == "profile" and
            str(r.get("ASSESSMENT_PROFILE", "")).strip() == profile_name.strip() and
            str(r.get("STANDARD_NAME", "")).strip() == standard_name.strip())
    ]
    
    if not matches:
        out.debug_msg(f"    No results found for this profile/standard")
        return None
    
    valid_results = [r for r in matches if r.get("ASSESSMENT_PROFILE_REVISION")]
    if not valid_results:
        out.debug_msg(f"    No valid results (missing revision)")
        return None
    
    latest = max(valid_results, key=lambda x: int(x.get("LAST_EVALUATION_TIME", 0)))
    
    anchor = {
        "profile_name": profile_name,
        "standard_name": standard_name,
        "assessment_profile_revision": latest.get("ASSESSMENT_PROFILE_REVISION"),
        "last_evaluation_time": latest.get("LAST_EVALUATION_TIME"),
        "asset_group_id": latest.get("ASSET_GROUP_ID")
    }
    
    eval_time = datetime.fromtimestamp(anchor["last_evaluation_time"] / 1000)
    out.debug_msg(f"    Found anchor (evaluated: {eval_time})")
    
    return anchor


def get_all_anchors(client: CortexAPIClient, assessments: List[dict]) -> List[dict]:
    """Get anchors for all profile/standard pairs."""
    out.header("Stage 1: Getting Assessment Anchors")
    
    out.debug_msg("Fetching all assessment results...")
    all_results = get_all_assessment_results(client)
    out.debug_msg(f"Retrieved {len(all_results)} total assessment results")
    
    anchors = []
    pairs_to_check = []
    
    for assessment in assessments:
        standard = assessment.get("standard")
        profiles = assessment.get("profiles", [])
        if isinstance(profiles, str):
            profiles = [profiles]
        for profile in profiles:
            pairs_to_check.append((profile, standard))
    
    if RICH_AVAILABLE:
        with create_progress() as progress:
            task = progress.add_task("Finding anchors", total=len(pairs_to_check))
            for profile, standard in pairs_to_check:
                anchor = find_anchor_for_pair(all_results, profile, standard)
                if anchor:
                    anchors.append(anchor)
                progress.update(task, advance=1)
    else:
        for profile, standard in pairs_to_check:
            anchor = find_anchor_for_pair(all_results, profile, standard)
            if anchor:
                anchors.append(anchor)
    
    out.success(f"Found {len(anchors)} anchor(s)")
    return anchors


# =============================================================================
# STAGE 2: GET CONTROLS CATALOG
# =============================================================================

def fetch_single_control(client: CortexAPIClient, control_id: str) -> Optional[dict]:
    """Fetch details for a single control."""
    severity_map = {
        "SEV_050_CRITICAL": "Critical",
        "SEV_040_HIGH": "High",
        "SEV_030_MEDIUM": "Medium",
        "SEV_020_LOW": "Low",
        "SEV_010_INFO": "Informational"
    }
    
    try:
        response = client.post(
            "/public_api/v1/compliance/get_control",
            {"request_data": {"id": control_id}}
        )
        
        ctrl = (response.get("reply") or {}).get("control", [None])[0]
        if not ctrl or ctrl.get("STATUS") != "ACTIVE":
            return None
        
        rules = ctrl.get("COMPLIANCE_RULES") or []
        rule_names = sorted({str(r["NAME"]) for r in rules if "NAME" in r})
        scannable_asset_types = sorted({str(at) for r in rules for at in r.get("SCANNABLE_ASSETS", [])})
        
        return {
            "revision": str(ctrl.get("REVISION")),
            "control_id": ctrl.get("CONTROL_ID", control_id),
            "control_name": ctrl.get("CONTROL_NAME", ""),
            "severity": severity_map.get(ctrl.get("SEVERITY"), "Unknown"),
            "category": ctrl.get("CATEGORY", ""),
            "subcategory": ctrl.get("SUBCATEGORY", ""),
            "rule_count": len(rule_names),
            "rule_names": rule_names,
            "mitigation": ctrl.get("MITIGATION", ""),
            "scannable_asset_types": scannable_asset_types
        }
    except Exception:
        return None


def get_controls_for_standard(client: CortexAPIClient, standard_name: str, parallel_workers: int, progress=None, task_id=None) -> List[dict]:
    """Fetch all controls for a standard using parallel requests."""
    out.debug_msg(f"Fetching controls for: {standard_name[:60]}...")
    
    body = {
        "request_data": {
            "filters": [{"field": "name", "operator": "eq", "value": standard_name}]
        }
    }
    response = client.post("/public_api/v1/compliance/get_standards", body)
    
    standards = response.get("reply", {}).get("standards", [])
    if not standards:
        out.debug_msg(f"Standard not found")
        return []
    
    control_ids = list(set(standards[0].get("controls_ids", [])))
    total = len(control_ids)
    out.debug_msg(f"Found {total} control IDs")
    
    if not control_ids:
        return []
    
    controls = []
    
    with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
        futures = {executor.submit(fetch_single_control, client, cid): cid for cid in control_ids}
        
        for future in as_completed(futures):
            result = future.result()
            if result:
                controls.append(result)
            if progress and task_id is not None:
                progress.update(task_id, advance=1)
    
    out.debug_msg(f"Fetched {len(controls)} active controls")
    return controls


def get_all_controls(client: CortexAPIClient, assessments: List[dict], parallel_workers: int) -> Dict[str, dict]:
    """Get controls catalog for all standards."""
    out.header("Stage 2: Getting Controls Catalog")
    
    standards = set()
    for assessment in assessments:
        standards.add(assessment.get("standard"))
    
    out.debug_msg(f"Processing {len(standards)} unique standard(s)")
    
    # First, get all control IDs to calculate total
    standard_control_ids = {}
    total_controls = 0
    for standard_name in standards:
        body = {
            "request_data": {
                "filters": [{"field": "name", "operator": "eq", "value": standard_name}]
            }
        }
        response = client.post("/public_api/v1/compliance/get_standards", body)
        std_list = response.get("reply", {}).get("standards", [])
        if std_list:
            control_ids = list(set(std_list[0].get("controls_ids", [])))
            standard_control_ids[standard_name] = control_ids
            total_controls += len(control_ids)
    
    catalog = {}
    
    if RICH_AVAILABLE and total_controls > 0:
        with create_progress() as progress:
            task = progress.add_task("Fetching controls", total=total_controls)
            for standard_name in standards:
                controls = get_controls_for_standard(client, standard_name, parallel_workers, progress, task)
                catalog[standard_name] = {
                    "standard_name": standard_name,
                    "total_controls": len(controls),
                    "controls": controls
                }
    else:
        for standard_name in standards:
            controls = get_controls_for_standard(client, standard_name, parallel_workers)
            catalog[standard_name] = {
                "standard_name": standard_name,
                "total_controls": len(controls),
                "controls": controls
            }
    
    total = sum(len(c["controls"]) for c in catalog.values())
    out.success(f"Total controls: {total}")
    
    return catalog


# =============================================================================
# STAGE 3: FETCH ASSETS
# =============================================================================

def fetch_asset_page(client: CortexAPIClient, anchor: dict, search_from: int, page_size: int) -> Tuple[int, List[dict]]:
    """Fetch a single page of assets."""
    payload = {
        "request_data": {
            "assessment_profile_revision": anchor["assessment_profile_revision"],
            "last_evaluation_time": anchor["last_evaluation_time"],
            "filters": [
                {"field": "status", "operator": "neq", "value": "Not Assessed"}
            ],
            "search_from": search_from,
            "search_to": search_from + page_size
        }
    }
    
    try:
        response = client.post("/public_api/v1/compliance/get_assets", payload)
        assets = response.get("reply", {}).get("assets", [])
        return (search_from, assets)
    except Exception as e:
        out.debug_msg(f"Error fetching page at {search_from}: {e}")
        return (search_from, [])


def get_total_asset_count(client: CortexAPIClient, anchor: dict) -> int:
    """Get total count of assets for an anchor."""
    payload = {
        "request_data": {
            "assessment_profile_revision": anchor["assessment_profile_revision"],
            "last_evaluation_time": anchor["last_evaluation_time"],
            "filters": [
                {"field": "status", "operator": "neq", "value": "Not Assessed"}
            ],
            "search_from": 0,
            "search_to": 1
        }
    }
    
    response = client.post("/public_api/v1/compliance/get_assets", payload)
    return response.get("reply", {}).get("total_count", 0)


def get_paginated_assets_parallel(
    client: CortexAPIClient, 
    anchor: dict, 
    page_size: int,
    parallel_workers: int,
    progress=None,
    task_id=None
) -> Tuple[List[dict], int]:
    """Fetch all assets for an anchor using parallel requests."""
    
    total_count = get_total_asset_count(client, anchor)
    if total_count == 0:
        return [], 0
    
    page_offsets = list(range(0, total_count, page_size))
    total_pages = len(page_offsets)
    
    out.debug_msg(f"Total: {total_count:,} assets, {total_pages:,} pages")
    
    all_assets = []
    
    with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
        futures = {
            executor.submit(fetch_asset_page, client, anchor, offset, page_size): offset 
            for offset in page_offsets
        }
        
        for future in as_completed(futures):
            _, assets = future.result()
            all_assets.extend(assets)
            if progress and task_id is not None:
                progress.update(task_id, advance=1)
    
    return all_assets, total_count


def enrich_assets(raw_assets: List[dict], anchor: dict, controls_lookup: Dict[str, dict]) -> Tuple[List[dict], int, int]:
    """Enrich raw assets with control data."""
    profile_name = anchor["profile_name"]
    standard_name = anchor["standard_name"]
    eval_timestamp = anchor["last_evaluation_time"]
    eval_datetime = datetime.fromtimestamp(eval_timestamp / 1000).isoformat() if eval_timestamp else None
    
    enriched = []
    matched = 0
    unmatched = 0
    
    for asset in raw_assets:
        revision = str(asset.get("CONTROL_REVISION_ID"))
        control_data = controls_lookup.get(revision, {})
        
        if control_data:
            matched += 1
        else:
            unmatched += 1
        
        enriched.append({
            "ASSESSMENT_PROFILE_NAME": profile_name,
            "ASSESSMENT_PROFILE_REVISION": anchor["assessment_profile_revision"],
            "STANDARD_NAME": standard_name,
            "ASSET_GROUP_ID": anchor.get("asset_group_id"),
            "EVALUATION_DATETIME": eval_datetime,
            "ASSET_ID": asset.get("ASSET_ID"),
            "ASSET_NAME": asset.get("ASSET_NAME"),
            "ASSET_TYPE": asset.get("ASSET_TYPE"),
            "STATUS": asset.get("STATUS"),
            "PROVIDER": asset.get("PROVIDER"),
            "REGION": asset.get("REGION"),
            "REALM": asset.get("REALM"),
            "TAGS": json.dumps(asset.get("TAGS", {})),
            "ORGANIZATION": asset.get("ORGANIZATION"),
            "ASSET_STRONG_ID": asset.get("ASSET_STRONG_ID"),
            "SOURCE": asset.get("SOURCE"),
            "RULE_TYPE": asset.get("RULE_TYPE"),
            "RULE_NAME": asset.get("RULE_NAME"),
            "RULE_REVISION_ID": asset.get("RULE_REVISION_ID"),
            "SEVERITY": control_data.get("severity", asset.get("SEVERITY")),
            "CONTROL_ID": control_data.get("control_id"),
            "CONTROL_NAME": asset.get("CONTROL"),
            "CONTROL_REVISION": revision,
            "CONTROL_RULE_NAMES": json.dumps(control_data.get("rule_names", [])),
            "CONTROL_RULE_COUNT": control_data.get("rule_count", 0),
            "CATEGORY": control_data.get("category", ""),
            "SUBCATEGORY": control_data.get("subcategory", ""),
            "MITIGATION": control_data.get("mitigation", ""),
            "SCANNABLE_ASSET_TYPES": json.dumps(control_data.get("scannable_asset_types", []))
        })
    
    return enriched, matched, unmatched


def fetch_all_assets(
    client: CortexAPIClient,
    anchors: List[dict],
    controls_catalog: Dict[str, dict],
    config: Config
) -> Dict[str, List[dict]]:
    """Fetch assets for all anchors using parallel requests."""
    out.header("Stage 3: Fetching Assets")
    
    if config.MAX_ANCHORS > 0 and len(anchors) > config.MAX_ANCHORS:
        anchors = anchors[:config.MAX_ANCHORS]
        out.debug_msg(f"Limiting to {config.MAX_ANCHORS} anchor(s)")
    
    # Pre-flight: Get counts for all anchors
    out.debug_msg("Pre-flight: Counting assets...")
    
    anchor_counts = []
    total_assets_all = 0
    total_pages_all = 0
    
    for anchor in anchors:
        count = get_total_asset_count(client, anchor)
        pages = (count + config.PAGE_SIZE - 1) // config.PAGE_SIZE if count > 0 else 0
        anchor_counts.append({
            "profile": anchor['profile_name'],
            "standard": anchor['standard_name'][:40],
            "assets": count,
            "pages": pages
        })
        total_assets_all += count
        total_pages_all += pages
    
    # Show pre-flight table
    rows = []
    for i, ac in enumerate(anchor_counts):
        rows.append([str(i+1), ac['profile'][:25], f"{ac['assets']:,}", f"{ac['pages']:,}"])
    
    out.table(
        "Pre-flight Summary",
        ["#", "Profile", "Assets", "Pages"],
        rows
    )
    
    est_rate = config.PARALLEL_WORKERS * 2
    est_minutes = (total_pages_all / est_rate / 60) if est_rate > 0 else 0
    out.info(f"Total: [cyan]{total_assets_all:,}[/cyan] assets • [cyan]{total_pages_all:,}[/cyan] API calls • ~[cyan]{est_minutes:.0f}[/cyan] min")
    
    # Fetch assets
    all_assets = {}
    total_fetched = 0
    
    if RICH_AVAILABLE:
        with create_progress() as progress:
            for i, anchor in enumerate(anchors):
                pair_key = f"{anchor['profile_name']}|{anchor['standard_name']}"
                
                standard_data = controls_catalog.get(anchor['standard_name'], {})
                controls = standard_data.get("controls", [])
                controls_lookup = {str(c.get("revision")): c for c in controls}
                
                total_count = anchor_counts[i]["assets"]
                total_pages = anchor_counts[i]["pages"]
                
                task_desc = f"[{i+1}/{len(anchors)}] {anchor['profile_name'][:30]}"
                task = progress.add_task(task_desc, total=total_pages)
                
                raw_assets, _ = get_paginated_assets_parallel(
                    client, anchor, config.PAGE_SIZE, config.PARALLEL_WORKERS, progress, task
                )
                
                enriched, matched, unmatched = enrich_assets(raw_assets, anchor, controls_lookup)
                out.debug_msg(f"Enrichment: {matched} matched, {unmatched} unmatched")
                
                all_assets[pair_key] = enriched
                total_fetched += len(enriched)
    else:
        for i, anchor in enumerate(anchors):
            pair_key = f"{anchor['profile_name']}|{anchor['standard_name']}"
            out.info(f"[{i+1}/{len(anchors)}] {anchor['profile_name']}")
            
            standard_data = controls_catalog.get(anchor['standard_name'], {})
            controls = standard_data.get("controls", [])
            controls_lookup = {str(c.get("revision")): c for c in controls}
            
            raw_assets, _ = get_paginated_assets_parallel(
                client, anchor, config.PAGE_SIZE, config.PARALLEL_WORKERS
            )
            
            enriched, matched, unmatched = enrich_assets(raw_assets, anchor, controls_lookup)
            all_assets[pair_key] = enriched
            total_fetched += len(enriched)
    
    out.success(f"Total assets fetched: {total_fetched:,}")
    return all_assets


# =============================================================================
# STAGE 4-7: OUTPUTS AND PUSH
# =============================================================================

def save_records_to_ndjson(records: List[dict], output_path: Path) -> int:
    """Save records as NDJSON format."""
    with open(output_path, 'w', encoding='utf-8') as f:
        for record in records:
            f.write(json.dumps(record, default=str) + "\n")
    return len(records)


def print_curl_command(file_path: Path, collector_url: str, api_key_env_var: str):
    """Print curl command for manual push."""
    out.info(f"\n  [dim]To push manually:[/dim]")
    out.info(f"  [dim]curl -X POST \"{collector_url}\" \\[/dim]")
    out.info(f"  [dim]  -H \"Authorization: ${api_key_env_var}\" \\[/dim]")
    out.info(f"  [dim]  -H \"Content-Type: text/plain\" \\[/dim]")
    out.info(f"  [dim]  --data-binary @{file_path}[/dim]\n")


def save_assets_to_csv(assets: List[dict], output_path: Path):
    """Save assets to CSV."""
    if not assets:
        return
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=assets[0].keys())
        writer.writeheader()
        writer.writerows(assets)
    out.debug_msg(f"Saved: {output_path.name}")


def save_all_outputs(all_assets: Dict[str, List[dict]], controls_catalog: Dict[str, dict], anchors: List[dict], config: Config, timestamp: str):
    """Save all outputs to files."""
    out.header("Stage 4: Saving Outputs")
    
    for pair_key, assets in all_assets.items():
        if assets:
            safe_name = pair_key.replace("|", "_").replace(" ", "_")[:50]
            csv_path = config.OUTPUT_DIR / f"assets_{safe_name}_{timestamp}.csv"
            save_assets_to_csv(assets, csv_path)
    
    catalog_path = config.OUTPUT_DIR / f"controls_catalog_{timestamp}.json"
    with open(catalog_path, 'w', encoding='utf-8') as f:
        json.dump(controls_catalog, f, indent=2)
    out.debug_msg(f"Saved: {catalog_path.name}")
    
    anchors_path = config.OUTPUT_DIR / f"anchors_{timestamp}.json"
    with open(anchors_path, 'w', encoding='utf-8') as f:
        json.dump(anchors, f, indent=2)
    out.debug_msg(f"Saved: {anchors_path.name}")
    
    summary = {
        "timestamp": timestamp,
        "total_anchors": len(anchors),
        "total_standards": len(controls_catalog),
        "total_controls": sum(c["total_controls"] for c in controls_catalog.values()),
        "total_assets": sum(len(a) for a in all_assets.values()),
    }
    summary_path = config.OUTPUT_DIR / f"summary_{timestamp}.json"
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2)
    out.debug_msg(f"Saved: {summary_path.name}")
    
    out.success(f"Outputs saved to {config.OUTPUT_DIR}")
    return summary


def push_to_collector(records: List[dict], collector_url: str, api_key: str, batch_size: int, dry_run: bool, label: str = "Record", progress=None) -> dict:
    """Push records to HTTP Collector."""
    if not records:
        return {"pushed": 0, "batches": 0}
    
    if not collector_url or not api_key:
        return {"pushed": 0, "batches": 0, "skipped": True}
    
    if dry_run:
        out.debug_msg(f"[DRY RUN] Would push {len(records)} {label}(s)")
        return {"pushed": len(records), "batches": (len(records) + batch_size - 1) // batch_size, "dry_run": True}
    
    headers = {"Authorization": api_key, "Content-Type": "text/plain"}
    total_pushed = 0
    total_batches = (len(records) + batch_size - 1) // batch_size
    
    if RICH_AVAILABLE and progress is None:
        with create_progress() as prog:
            task = prog.add_task(f"Pushing {label}s", total=total_batches)
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                ndjson_body = "\n".join(json.dumps(r, default=str) for r in batch)
                try:
                    response = requests.post(collector_url, headers=headers, data=ndjson_body, timeout=120)
                    response.raise_for_status()
                    total_pushed += len(batch)
                except requests.exceptions.RequestException as e:
                    out.error(f"Failed to push batch: {e}")
                prog.update(task, advance=1)
    else:
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            ndjson_body = "\n".join(json.dumps(r, default=str) for r in batch)
            try:
                response = requests.post(collector_url, headers=headers, data=ndjson_body, timeout=120)
                response.raise_for_status()
                total_pushed += len(batch)
            except requests.exceptions.RequestException as e:
                out.error(f"Failed to push batch: {e}")
    
    return {"pushed": total_pushed, "batches": total_batches}


def push_assets_to_collector(all_assets: Dict[str, List[dict]], config: Config, timestamp: str) -> dict:
    """Push assets to HTTP Collector."""
    out.header("Stage 5: Push Assets to Collector")
    
    all_records = []
    for assets in all_assets.values():
        all_records.extend(assets)
    
    if not all_records:
        out.warning("No assets to push")
        return {"assets_pushed": 0, "skipped": True}
    
    ndjson_path = config.OUTPUT_DIR / f"assets_collector_{timestamp}.json"
    save_records_to_ndjson(all_records, ndjson_path)
    out.success(f"Saved: {ndjson_path.name} ({len(all_records):,} records)")
    
    if config.COLLECTOR_URL:
        print_curl_command(ndjson_path, config.COLLECTOR_URL, "COMPLIANCE_API_KEY")
    
    if not config.COLLECTOR_URL:
        out.debug_msg("Skipping push: No collector URL")
        return {"assets_pushed": 0, "skipped": True, "saved_file": str(ndjson_path)}
    
    if not config.COMPLIANCE_API_KEY:
        out.debug_msg("Skipping push: No COMPLIANCE_API_KEY")
        return {"assets_pushed": 0, "skipped": True, "saved_file": str(ndjson_path)}
    
    result = push_to_collector(
        all_records, config.COLLECTOR_URL, config.COMPLIANCE_API_KEY,
        config.BATCH_SIZE, config.DRY_RUN, "Asset"
    )
    
    status = "dry run" if result.get("dry_run") else f"{result.get('pushed', 0):,} pushed"
    out.success(f"Assets: {status}")
    
    return {"assets_pushed": result.get("pushed", 0), "saved_file": str(ndjson_path), **result}


def create_control_summary(all_assets: Dict[str, List[dict]], controls_catalog: Dict[str, dict], anchors: List[dict]) -> List[dict]:
    """Create per-control summary records."""
    out.header("Stage 6: Creating Control Summaries")
    
    all_summaries = []
    
    for anchor in anchors:
        profile_name = anchor["profile_name"]
        standard_name = anchor["standard_name"]
        pair_key = f"{profile_name}|{standard_name}"
        
        out.debug_msg(f"Processing: {profile_name} / {standard_name[:40]}...")
        
        assets = all_assets.get(pair_key, [])
        standard_data = controls_catalog.get(standard_name, {})
        controls = standard_data.get("controls", [])
        
        if not controls:
            continue
        
        controls_lookup = {str(c.get("revision")): c for c in controls}
        
        assets_by_control = {}
        for asset in assets:
            revision = asset.get("CONTROL_REVISION", str(asset.get("CONTROL_REVISION_ID", "")))
            if revision not in assets_by_control:
                assets_by_control[revision] = []
            assets_by_control[revision].append(asset)
        
        eval_datetime = anchor.get("last_evaluation_time")
        eval_datetime_str = datetime.fromtimestamp(eval_datetime / 1000).isoformat() if eval_datetime else None
        
        for control in controls:
            revision = str(control.get("revision"))
            control_assets = assets_by_control.get(revision, [])
            
            status_counts = {"PASSED": 0, "FAILED": 0, "NOT_ASSESSED": 0}
            for asset in control_assets:
                status = asset.get("STATUS", "").upper()
                if status in status_counts:
                    status_counts[status] += 1
            
            passed = status_counts["PASSED"]
            failed = status_counts["FAILED"]
            not_assessed = status_counts["NOT_ASSESSED"]
            total = passed + failed + not_assessed
            
            if failed > 0:
                overall_status = "FAILED"
            elif passed > 0:
                overall_status = "PASSED"
            elif not_assessed > 0:
                overall_status = "NOT_ASSESSED"
            else:
                overall_status = "NOT_EVALUATED"
            
            score = passed / (passed + failed) if (passed + failed) > 0 else 0.0
            
            all_summaries.append({
                "STANDARD_NAME": standard_name,
                "ASSESSMENT_PROFILE_NAME": profile_name,
                "ASSESSMENT_PROFILE_REVISION": anchor.get("assessment_profile_revision"),
                "ASSET_GROUP_ID": anchor.get("asset_group_id"),
                "EVALUATION_DATETIME": eval_datetime_str,
                "STATUS": overall_status,
                "CONTROL_NAME": control.get("control_name", ""),
                "CONTROL_ID": control.get("control_id", ""),
                "CONTROL_REVISION": revision,
                "SCORE": round(score, 4),
                "SEVERITY": control.get("severity", ""),
                "CATEGORY": control.get("category", ""),
                "SUBCATEGORY": control.get("subcategory", ""),
                "RULES_COUNT": control.get("rule_count", len(control.get("rule_names", []))),
                "PASSED_COUNT": passed,
                "FAILED_COUNT": failed,
                "NOT_ASSESSED_COUNT": not_assessed,
                "TOTAL_ASSETS": total
            })
    
    out.success(f"Total summaries: {len(all_summaries)}")
    return all_summaries


def push_summaries_to_collector(summaries: List[dict], config: Config, timestamp: str) -> dict:
    """Push summaries to HTTP Collector."""
    out.header("Stage 7: Push Summaries to Collector")
    
    if not summaries:
        out.warning("No summaries to push")
        return {"summaries_pushed": 0, "skipped": True}
    
    ndjson_path = config.OUTPUT_DIR / f"summaries_collector_{timestamp}.json"
    save_records_to_ndjson(summaries, ndjson_path)
    out.success(f"Saved: {ndjson_path.name} ({len(summaries):,} records)")
    
    if config.COLLECTOR_URL:
        print_curl_command(ndjson_path, config.COLLECTOR_URL, "CONTROLS_API_KEY")
    
    if not config.COLLECTOR_URL:
        out.debug_msg("Skipping push: No collector URL")
        return {"summaries_pushed": 0, "skipped": True, "saved_file": str(ndjson_path)}
    
    if not config.CONTROLS_API_KEY:
        out.debug_msg("Skipping push: No CONTROLS_API_KEY")
        return {"summaries_pushed": 0, "skipped": True, "saved_file": str(ndjson_path)}
    
    result = push_to_collector(
        summaries, config.COLLECTOR_URL, config.CONTROLS_API_KEY,
        config.BATCH_SIZE, config.DRY_RUN, "Summary"
    )
    
    status = "dry run" if result.get("dry_run") else f"{result.get('pushed', 0):,} pushed"
    out.success(f"Summaries: {status}")
    
    return {"summaries_pushed": result.get("pushed", 0), "saved_file": str(ndjson_path), **result}


# =============================================================================
# MAIN
# =============================================================================

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Cortex Cloud Compliance Data Fetcher",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python local_compliance_fetcher.py              # Normal mode
  python local_compliance_fetcher.py --debug      # Verbose output
  python local_compliance_fetcher.py --dry-run    # Don't push to collector
        """
    )
    parser.add_argument(
        "--debug", "-d",
        action="store_true",
        help="Enable verbose debug output (show retries, API details)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't push to collector (overrides config)"
    )
    return parser.parse_args()


def main():
    global out
    
    # Parse arguments
    args = parse_args()
    
    # Initialize output manager
    out = Output(debug=args.debug)
    
    # Title
    if RICH_AVAILABLE:
        out.console.print()
        out.console.print(Panel.fit(
            "[bold]Cortex Cloud Compliance Data Fetcher[/bold]" + 
            ("\n[dim]DEBUG MODE[/dim]" if args.debug else ""),
            border_style="cyan"
        ))
    else:
        print("\n" + "="*60)
        print("  Cortex Cloud Compliance Data Fetcher")
        if args.debug:
            print("  DEBUG MODE")
        print("="*60)
    
    start_time = time.time()
    
    # Load configuration
    dry_run_override = True if args.dry_run else None
    config = Config(dry_run_override=dry_run_override)
    
    # Create API client
    client = CortexAPIClient(config)
    
    # Stage 1: Get anchors
    anchors = get_all_anchors(client, config.ASSESSMENTS)
    if not anchors:
        out.error("No anchors found. Check your assessments configuration.")
        return 1
    
    # Stage 2: Get controls catalog
    controls_catalog = get_all_controls(client, config.ASSESSMENTS, config.PARALLEL_WORKERS)
    
    # Stage 3: Fetch assets
    all_assets = fetch_all_assets(client, anchors, controls_catalog, config)
    
    # Generate timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Stage 4: Save outputs
    summary = save_all_outputs(all_assets, controls_catalog, anchors, config, timestamp)
    
    # Stage 5: Push assets
    assets_push = push_assets_to_collector(all_assets, config, timestamp)
    
    # Stage 6: Create summaries
    control_summaries = create_control_summary(all_assets, controls_catalog, anchors)
    
    # Save summaries CSV
    if control_summaries:
        summaries_path = config.OUTPUT_DIR / f"control_summaries_{timestamp}.csv"
        with open(summaries_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=control_summaries[0].keys())
            writer.writeheader()
            writer.writerows(control_summaries)
        out.debug_msg(f"Saved: {summaries_path.name}")
    
    # Stage 7: Push summaries
    summaries_push = push_summaries_to_collector(control_summaries, config, timestamp)
    
    # Final summary
    elapsed = time.time() - start_time
    
    # Determine push status strings
    def push_status(result):
        if result.get("skipped"):
            return "skipped"
        elif result.get("dry_run"):
            return f"{result.get('pushed', 0):,} (dry run)"
        else:
            return f"{result.get('pushed', 0):,} pushed"
    
    stats = {
        "anchors": len(anchors),
        "controls": summary.get("total_controls", 0),
        "assets": summary.get("total_assets", 0),
        "summaries": len(control_summaries),
        "assets_push": push_status(assets_push),
        "summaries_push": push_status(summaries_push)
    }
    
    out.final_summary(stats, elapsed)
    out.info(f"Output directory: [cyan]{config.OUTPUT_DIR}[/cyan]")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
