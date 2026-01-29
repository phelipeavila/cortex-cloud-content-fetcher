#!/usr/bin/env python3
"""
Local Compliance Data Fetcher

A standalone script to fetch compliance data from Cortex Cloud APIs.
This is a local version of the XSOAR scripts for testing and development.

Usage:
    1. Create a config.env file with your credentials
    2. Run: python local_compliance_fetcher.py

Requirements:
    pip install requests python-dotenv
"""

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
    print("  pip install requests python-dotenv")
    sys.exit(1)


# =============================================================================
# CONFIGURATION
# =============================================================================

class Config:
    """Configuration loaded from environment variables."""
    
    def __init__(self):
        # Load from config.env file
        env_file = Path(__file__).parent / "config.env"
        if env_file.exists():
            load_dotenv(env_file)
            print(f"✓ Loaded config from {env_file}")
        else:
            print(f"⚠ No config.env found at {env_file}")
            print("  Create one based on config.env.example")
        
        # Required API credentials
        self.API_URL = os.getenv("API_URL", "").rstrip("/")
        self.API_KEY = os.getenv("API_KEY", "")
        self.API_ID = os.getenv("API_ID", "")
        
        # Tenant URL (for building collector URL)
        self.TENANT_EXTERNAL_URL = os.getenv("TENANT_EXTERNAL_URL", "")
        
        # HTTP Collector API keys (for pushing data to collector)
        # COMPLIANCE_API_KEY - Used by assets push (ComplianceFetchAssets)
        # CONTROLS_API_KEY - Used by summary push (ComplianceCreateSummary)
        self.COMPLIANCE_API_KEY = os.getenv("COMPLIANCE_API_KEY", "")
        self.CONTROLS_API_KEY = os.getenv("CONTROLS_API_KEY", "")
        
        # Build collector URL from tenant URL
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
            print("\n❌ Configuration errors:")
            for e in errors:
                print(f"   - {e}")
            print("\nPlease check your config.env file.")
            sys.exit(1)
        
        print(f"✓ API URL: {self.API_URL}")
        print(f"✓ Assessments: {len(self.ASSESSMENTS)} standard(s)")
        print(f"✓ Output directory: {self.OUTPUT_DIR}")
        
        # Show collector status
        if self.COLLECTOR_URL:
            print(f"✓ Collector URL: {self.COLLECTOR_URL}")
            if self.COMPLIANCE_API_KEY:
                print(f"✓ COMPLIANCE_API_KEY: configured")
            else:
                print(f"⚠ COMPLIANCE_API_KEY: not set (assets won't be pushed)")
            if self.CONTROLS_API_KEY:
                print(f"✓ CONTROLS_API_KEY: configured")
            else:
                print(f"⚠ CONTROLS_API_KEY: not set (summaries won't be pushed)")
        else:
            print(f"⚠ No collector URL (TENANT_EXTERNAL_URL not set)")


# =============================================================================
# API CLIENT
# =============================================================================

class CortexAPIClient:
    """Client for Cortex Cloud Compliance APIs with automatic retry."""
    
    # Retry configuration
    MAX_RETRIES = 5
    BASE_DELAY = 2.0  # seconds
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
        """
        Make a POST request to the API with automatic retry on transient errors.
        
        Retries on:
        - Network errors (ConnectionError, Timeout, etc.)
        - Server errors (500, 502, 503, 504)
        - Rate limiting (429)
        
        Does NOT retry on:
        - Client errors (400, 401, 403, 404) except 429
        """
        url = f"{self.base_url}{endpoint}"
        last_exception = None
        
        for attempt in range(self.MAX_RETRIES + 1):
            try:
                response = self.session.post(url, json=body, timeout=timeout)
                
                # Check if we should retry based on status code
                if response.status_code in self.RETRYABLE_STATUS_CODES:
                    # Force raise to trigger retry
                    response.raise_for_status()
                
                # For other errors (4xx except 429), raise immediately without retry
                response.raise_for_status()
                
                # Success!
                return response.json()
                
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout,
                    requests.exceptions.ChunkedEncodingError) as e:
                # Network errors - always retry
                last_exception = e
                
            except requests.exceptions.HTTPError as e:
                # Only retry 5xx and 429, not other 4xx errors
                if e.response is not None and e.response.status_code not in self.RETRYABLE_STATUS_CODES:
                    # Client error (400, 401, 403, 404) - don't retry
                    print(f"❌ HTTP Error: {e}")
                    print(f"   Response: {e.response.text[:500]}")
                    raise
                last_exception = e
                
            except requests.exceptions.RequestException as e:
                # Other request errors - retry
                last_exception = e
            
            # If we get here, we need to retry
            if attempt < self.MAX_RETRIES:
                # Exponential backoff with jitter
                delay = (self.BASE_DELAY * (2 ** attempt)) + random.uniform(0.1, 1.0)
                error_type = type(last_exception).__name__
                status_info = ""
                if hasattr(last_exception, 'response') and last_exception.response is not None:
                    status_info = f" (HTTP {last_exception.response.status_code})"
                
                print(f"  ⚠ {endpoint} failed{status_info}: {error_type}. Retrying in {delay:.1f}s (attempt {attempt+1}/{self.MAX_RETRIES})")
                time.sleep(delay)
        
        # All retries exhausted
        print(f"❌ Failed after {self.MAX_RETRIES} retries: {endpoint}")
        if last_exception:
            raise last_exception
        raise Exception(f"Failed after {self.MAX_RETRIES} retries: {endpoint}")


# =============================================================================
# STAGE 1: GET ANCHORS
# =============================================================================

def get_all_assessment_results(client: CortexAPIClient) -> List[dict]:
    """Fetch all assessment results from the API (no filters allowed)."""
    # The API only allows 'labels' filter, so we fetch all and filter client-side
    body = {"request_data": {"filters": []}}
    
    response = client.post("/public_api/v1/compliance/get_assessment_results", body)
    # API returns data in "reply.data" (not "reply.assessment_results")
    results = response.get("reply", {}).get("data", [])
    
    return results


def find_anchor_for_pair(all_results: List[dict], profile_name: str, standard_name: str) -> Optional[dict]:
    """Find the latest assessment anchor for a profile/standard pair from cached results."""
    print(f"  Finding anchor for: {profile_name} / {standard_name[:50]}...")
    
    # Filter client-side for matching profile and standard
    # Match by TYPE=profile, ASSESSMENT_PROFILE, and STANDARD_NAME
    matches = [
        r for r in all_results
        if (str(r.get("TYPE", "")).lower() == "profile" and
            str(r.get("ASSESSMENT_PROFILE", "")).strip() == profile_name.strip() and
            str(r.get("STANDARD_NAME", "")).strip() == standard_name.strip())
    ]
    
    if not matches:
        print(f"    ⚠ No assessment results found for this profile/standard")
        return None
    
    # Filter for valid results with revision
    valid_results = [r for r in matches if r.get("ASSESSMENT_PROFILE_REVISION")]
    if not valid_results:
        print(f"    ⚠ No valid assessment results (missing revision)")
        return None
    
    # Get the latest by evaluation time
    latest = max(valid_results, key=lambda x: int(x.get("LAST_EVALUATION_TIME", 0)))
    
    anchor = {
        "profile_name": profile_name,
        "standard_name": standard_name,
        "assessment_profile_revision": latest.get("ASSESSMENT_PROFILE_REVISION"),
        "last_evaluation_time": latest.get("LAST_EVALUATION_TIME"),
        "asset_group_id": latest.get("ASSET_GROUP_ID")
    }
    
    eval_time = datetime.fromtimestamp(anchor["last_evaluation_time"] / 1000)
    print(f"    ✓ Found anchor (evaluated: {eval_time})")
    
    return anchor


def get_all_anchors(client: CortexAPIClient, assessments: List[dict]) -> List[dict]:
    """Get anchors for all profile/standard pairs."""
    print("\n" + "="*60)
    print("STAGE 1: Getting Assessment Anchors")
    print("="*60)
    
    # Fetch all assessment results once (API doesn't support profile/standard filters)
    print("  Fetching all assessment results...")
    all_results = get_all_assessment_results(client)
    print(f"  ✓ Retrieved {len(all_results)} total assessment results")
    
    anchors = []
    
    for assessment in assessments:
        standard = assessment.get("standard")
        profiles = assessment.get("profiles", [])
        
        if isinstance(profiles, str):
            profiles = [profiles]
        
        for profile in profiles:
            anchor = find_anchor_for_pair(all_results, profile, standard)
            if anchor:
                anchors.append(anchor)
    
    print(f"\n✓ Found {len(anchors)} anchor(s)")
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
            "scannable_asset_types": scannable_asset_types
        }
    except Exception as e:
        return None


def get_controls_for_standard(client: CortexAPIClient, standard_name: str, parallel_workers: int = 5) -> List[dict]:
    """Fetch all controls for a standard using parallel requests."""
    print(f"\n  Fetching controls for: {standard_name[:60]}...")
    
    # Get control IDs from standard
    body = {
        "request_data": {
            "filters": [{"field": "name", "operator": "eq", "value": standard_name}]
        }
    }
    response = client.post("/public_api/v1/compliance/get_standards", body)
    
    standards = response.get("reply", {}).get("standards", [])
    if not standards:
        print(f"    ⚠ Standard not found")
        return []
    
    control_ids = list(set(standards[0].get("controls_ids", [])))
    total = len(control_ids)
    print(f"    Found {total} control IDs")
    
    if not control_ids:
        return []
    
    # Fetch controls in parallel
    controls = []
    completed = 0
    
    print(f"    Fetching control details (using {parallel_workers} workers)...")
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
        futures = {executor.submit(fetch_single_control, client, cid): cid for cid in control_ids}
        
        for future in as_completed(futures):
            completed += 1
            result = future.result()
            if result:
                controls.append(result)
            
            # Progress update every 20 controls
            if completed % 20 == 0 or completed == total:
                elapsed = time.time() - start_time
                rate = completed / elapsed if elapsed > 0 else 0
                print(f"      Progress: {completed}/{total} ({rate:.1f}/sec)")
    
    elapsed = time.time() - start_time
    print(f"    ✓ Fetched {len(controls)} active controls in {elapsed:.1f}s")
    
    return controls


def get_all_controls(client: CortexAPIClient, assessments: List[dict], parallel_workers: int = 5) -> Dict[str, dict]:
    """Get controls catalog for all standards."""
    print("\n" + "="*60)
    print("STAGE 2: Getting Controls Catalog")
    print("="*60)
    
    # Get unique standards
    standards = set()
    for assessment in assessments:
        standards.add(assessment.get("standard"))
    
    print(f"Processing {len(standards)} unique standard(s)")
    
    catalog = {}
    for standard_name in standards:
        controls = get_controls_for_standard(client, standard_name, parallel_workers)
        catalog[standard_name] = {
            "standard_name": standard_name,
            "total_controls": len(controls),
            "controls": controls
        }
    
    total_controls = sum(len(c["controls"]) for c in catalog.values())
    print(f"\n✓ Total controls in catalog: {total_controls}")
    
    return catalog


# =============================================================================
# STAGE 3: FETCH ASSETS (PARALLEL)
# =============================================================================

def fetch_asset_page(client: CortexAPIClient, anchor: dict, search_from: int, page_size: int) -> Tuple[int, List[dict]]:
    """Fetch a single page of assets. Returns (search_from, assets) tuple."""
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
        reply = response.get("reply", {})
        assets = reply.get("assets", [])
        return (search_from, assets)
    except Exception as e:
        print(f"      ⚠ Error fetching page at {search_from}: {e}")
        return (search_from, [])


def get_total_asset_count(client: CortexAPIClient, anchor: dict) -> int:
    """Get total count of assets for an anchor (single request)."""
    payload = {
        "request_data": {
            "assessment_profile_revision": anchor["assessment_profile_revision"],
            "last_evaluation_time": anchor["last_evaluation_time"],
            "filters": [
                {"field": "status", "operator": "neq", "value": "Not Assessed"}
            ],
            "search_from": 0,
            "search_to": 1  # Just need count, not actual assets
        }
    }
    
    response = client.post("/public_api/v1/compliance/get_assets", payload)
    return response.get("reply", {}).get("total_count", 0)


def get_paginated_assets_parallel(
    client: CortexAPIClient, 
    anchor: dict, 
    page_size: int,
    parallel_workers: int = 10
) -> Tuple[List[dict], int]:
    """Fetch all assets for an anchor using parallel requests."""
    
    # First, get total count
    total_count = get_total_asset_count(client, anchor)
    if total_count == 0:
        return [], 0
    
    print(f"    Total assets to fetch: {total_count:,}")
    
    # Calculate all page offsets
    page_offsets = list(range(0, total_count, page_size))
    total_pages = len(page_offsets)
    
    print(f"    Pages to fetch: {total_pages:,} (using {parallel_workers} parallel workers)")
    
    # Fetch all pages in parallel
    all_assets = []
    completed = 0
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
        # Submit all page fetches
        futures = {
            executor.submit(fetch_asset_page, client, anchor, offset, page_size): offset 
            for offset in page_offsets
        }
        
        # Collect results as they complete
        for future in as_completed(futures):
            completed += 1
            search_from, assets = future.result()
            all_assets.extend(assets)
            
            # Progress update every 50 pages or at end
            if completed % 50 == 0 or completed == total_pages:
                elapsed = time.time() - start_time
                rate = completed / elapsed if elapsed > 0 else 0
                pct = (completed / total_pages) * 100
                print(f"      Progress: {completed:,}/{total_pages:,} pages ({pct:.1f}%) - {len(all_assets):,} assets - {rate:.1f} pages/sec")
    
    elapsed = time.time() - start_time
    print(f"    ✓ Fetched {len(all_assets):,} assets in {elapsed:.1f}s ({len(all_assets)/elapsed:.0f} assets/sec)")
    
    return all_assets, total_count


def enrich_assets(raw_assets: List[dict], anchor: dict, controls_lookup: Dict[str, dict]) -> List[dict]:
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
    
    print(f"    Enrichment: {matched} matched, {unmatched} unmatched")
    return enriched


def fetch_assets_for_anchor(
    client: CortexAPIClient, 
    anchor: dict, 
    controls_catalog: Dict[str, dict],
    page_size: int,
    parallel_workers: int = 10
) -> List[dict]:
    """Fetch and enrich all assets for an anchor using parallel requests."""
    profile_name = anchor["profile_name"]
    standard_name = anchor["standard_name"]
    
    print(f"\n  Processing: {profile_name} / {standard_name[:40]}...")
    
    # Get controls for this standard
    standard_data = controls_catalog.get(standard_name, {})
    controls = standard_data.get("controls", [])
    
    if not controls:
        print(f"    ⚠ No controls found for standard")
        return []
    
    # Build controls lookup by revision
    controls_lookup = {str(c.get("revision")): c for c in controls}
    print(f"    Controls lookup: {len(controls_lookup)} entries")
    
    # Fetch all assets in parallel
    raw_assets, total_count = get_paginated_assets_parallel(
        client, anchor, page_size, parallel_workers
    )
    
    # Enrich assets
    enriched = enrich_assets(raw_assets, anchor, controls_lookup)
    
    return enriched


def fetch_all_assets(
    client: CortexAPIClient,
    anchors: List[dict],
    controls_catalog: Dict[str, dict],
    config: Config
) -> Dict[str, List[dict]]:
    """Fetch assets for all anchors using parallel requests."""
    print("\n" + "="*60)
    print("STAGE 3: Fetching Assets (Parallel)")
    print("="*60)
    
    # Limit anchors if configured
    if config.MAX_ANCHORS > 0 and len(anchors) > config.MAX_ANCHORS:
        anchors = anchors[:config.MAX_ANCHORS]
        print(f"Limiting to {config.MAX_ANCHORS} anchor(s)")
    
    # =========================================================================
    # PRE-FLIGHT: Get asset counts for all anchors
    # =========================================================================
    print("\n--- Pre-flight: Counting assets for each anchor ---\n")
    
    anchor_counts = []
    total_assets_all = 0
    total_pages_all = 0
    
    for i, anchor in enumerate(anchors):
        profile = anchor['profile_name']
        standard = anchor['standard_name']
        
        # Get count for this anchor
        count = get_total_asset_count(client, anchor)
        pages = (count + config.PAGE_SIZE - 1) // config.PAGE_SIZE if count > 0 else 0
        
        anchor_counts.append({
            "index": i + 1,
            "profile": profile,
            "standard": standard[:50],
            "assets": count,
            "pages": pages
        })
        
        total_assets_all += count
        total_pages_all += pages
        
        print(f"  [{i+1}] {profile[:30]:30} | {count:>10,} assets | {pages:>6,} pages")
    
    # Summary
    print("\n" + "-"*60)
    print(f"  TOTAL: {total_assets_all:,} assets across {total_pages_all:,} API requests")
    print(f"  Workers: {config.PARALLEL_WORKERS}")
    
    # Estimate time (assuming ~10 requests/sec with parallel workers)
    est_rate = config.PARALLEL_WORKERS * 2  # ~2 req/sec per worker
    est_seconds = total_pages_all / est_rate if est_rate > 0 else 0
    est_minutes = est_seconds / 60
    print(f"  Estimated time: ~{est_minutes:.0f} minutes ({est_seconds:.0f} seconds)")
    print("-"*60 + "\n")
    
    # =========================================================================
    # FETCH: Now fetch assets for each anchor
    # =========================================================================
    all_assets = {}
    total_fetched = 0
    
    for i, anchor in enumerate(anchors):
        print(f"\nAnchor {i+1}/{len(anchors)}:")
        pair_key = f"{anchor['profile_name']}|{anchor['standard_name']}"
        
        assets = fetch_assets_for_anchor(
            client, anchor, controls_catalog, 
            config.PAGE_SIZE, config.PARALLEL_WORKERS
        )
        all_assets[pair_key] = assets
        total_fetched += len(assets)
    
    print(f"\n✓ Total assets fetched: {total_fetched:,}")
    return all_assets


# =============================================================================
# PUSH TO COLLECTOR
# =============================================================================

def save_records_to_ndjson(records: List[dict], output_path: Path) -> int:
    """Save records as NDJSON format (collector-ready)."""
    with open(output_path, 'w', encoding='utf-8') as f:
        for record in records:
            f.write(json.dumps(record, default=str) + "\n")
    return len(records)


def print_curl_command(file_path: Path, collector_url: str, api_key_env_var: str):
    """Print a curl command to push the saved file manually."""
    print(f"\n  To push manually, run:")
    print(f"  curl -X POST \"{collector_url}\" \\")
    print(f"    -H \"Authorization: ${api_key_env_var}\" \\")
    print(f"    -H \"Content-Type: text/plain\" \\")
    print(f"    --data-binary @{file_path}\n")


def push_to_collector(
    records: List[dict],
    collector_url: str,
    api_key: str,
    batch_size: int,
    dry_run: bool,
    label: str = "Record"
) -> dict:
    """Push records to HTTP Collector in NDJSON format."""
    if not records:
        return {"pushed": 0, "batches": 0}
    
    if not collector_url or not api_key:
        print(f"    ⚠ Skipping push ({label}): No collector URL or API key")
        return {"pushed": 0, "batches": 0, "skipped": True}
    
    if dry_run:
        print(f"    [DRY RUN] Would push {len(records)} {label}(s) to collector")
        return {"pushed": len(records), "batches": (len(records) + batch_size - 1) // batch_size, "dry_run": True}
    
    headers = {
        "Authorization": api_key,
        "Content-Type": "text/plain"
    }
    
    total_pushed = 0
    total_batches = 0
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        # NDJSON format: one JSON object per line
        ndjson_body = "\n".join(json.dumps(r, default=str) for r in batch)
        
        try:
            response = requests.post(
                collector_url,
                headers=headers,
                data=ndjson_body,
                timeout=120
            )
            response.raise_for_status()
            total_pushed += len(batch)
            total_batches += 1
            print(f"    Pushed batch {total_batches}: {len(batch)} {label}(s)")
        except requests.exceptions.RequestException as e:
            print(f"    ❌ Failed to push batch: {e}")
    
    print(f"    ✓ Total pushed: {total_pushed} {label}(s) in {total_batches} batch(es)")
    return {"pushed": total_pushed, "batches": total_batches}


# =============================================================================
# OUTPUT
# =============================================================================

def save_assets_to_csv(assets: List[dict], output_path: Path):
    """Save assets to a CSV file."""
    if not assets:
        return
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=assets[0].keys())
        writer.writeheader()
        writer.writerows(assets)
    
    print(f"  ✓ Saved: {output_path.name}")


def save_all_outputs(
    all_assets: Dict[str, List[dict]],
    controls_catalog: Dict[str, dict],
    anchors: List[dict],
    config: Config
):
    """Save all outputs to files."""
    print("\n" + "="*60)
    print("STAGE 4: Saving Outputs")
    print("="*60)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save assets CSVs
    for pair_key, assets in all_assets.items():
        if assets:
            safe_name = pair_key.replace("|", "_").replace(" ", "_")[:50]
            csv_path = config.OUTPUT_DIR / f"assets_{safe_name}_{timestamp}.csv"
            save_assets_to_csv(assets, csv_path)
    
    # Save controls catalog JSON
    catalog_path = config.OUTPUT_DIR / f"controls_catalog_{timestamp}.json"
    with open(catalog_path, 'w', encoding='utf-8') as f:
        json.dump(controls_catalog, f, indent=2)
    print(f"  ✓ Saved: {catalog_path.name}")
    
    # Save anchors JSON
    anchors_path = config.OUTPUT_DIR / f"anchors_{timestamp}.json"
    with open(anchors_path, 'w', encoding='utf-8') as f:
        json.dump(anchors, f, indent=2)
    print(f"  ✓ Saved: {anchors_path.name}")
    
    # Save summary JSON
    summary = {
        "timestamp": timestamp,
        "total_anchors": len(anchors),
        "total_standards": len(controls_catalog),
        "total_controls": sum(c["total_controls"] for c in controls_catalog.values()),
        "total_assets": sum(len(a) for a in all_assets.values()),
        "pairs": [
            {
                "pair": k,
                "assets": len(v)
            }
            for k, v in all_assets.items()
        ]
    }
    summary_path = config.OUTPUT_DIR / f"summary_{timestamp}.json"
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2)
    print(f"  ✓ Saved: {summary_path.name}")
    
    return summary


def push_assets_to_collector(all_assets: Dict[str, List[dict]], config: Config, timestamp: str) -> dict:
    """Push all assets to HTTP Collector using COMPLIANCE_API_KEY."""
    print("\n" + "="*60)
    print("STAGE 5: Push Assets to HTTP Collector")
    print("="*60)
    
    # Flatten all assets into single list
    all_records = []
    for assets in all_assets.values():
        all_records.extend(assets)
    
    if not all_records:
        print("⚠ No assets to push")
        return {"assets_pushed": 0, "skipped": True}
    
    print(f"Total asset records: {len(all_records):,}")
    
    # Save to NDJSON file (collector-ready format)
    ndjson_path = config.OUTPUT_DIR / f"assets_collector_{timestamp}.json"
    save_records_to_ndjson(all_records, ndjson_path)
    print(f"  ✓ Saved: {ndjson_path.name} ({len(all_records):,} records)")
    
    # Print curl command for manual push
    if config.COLLECTOR_URL:
        print_curl_command(ndjson_path, config.COLLECTOR_URL, "COMPLIANCE_API_KEY")
    
    # Check if we should push
    if not config.COLLECTOR_URL:
        print("⚠ Skipping push: No collector URL configured")
        return {"assets_pushed": 0, "skipped": True, "saved_file": str(ndjson_path)}
    
    if not config.COMPLIANCE_API_KEY:
        print("⚠ Skipping push: No COMPLIANCE_API_KEY configured")
        return {"assets_pushed": 0, "skipped": True, "saved_file": str(ndjson_path)}
    
    print(f"Dry run mode: {config.DRY_RUN}")
    
    result = push_to_collector(
        records=all_records,
        collector_url=config.COLLECTOR_URL,
        api_key=config.COMPLIANCE_API_KEY,
        batch_size=config.BATCH_SIZE,
        dry_run=config.DRY_RUN,
        label="Asset"
    )
    
    return {"assets_pushed": result.get("pushed", 0), "saved_file": str(ndjson_path), **result}


# =============================================================================
# STAGE 6: CREATE CONTROL SUMMARY
# =============================================================================

def create_control_summary(
    all_assets: Dict[str, List[dict]],
    controls_catalog: Dict[str, dict],
    anchors: List[dict]
) -> List[dict]:
    """
    Create per-control summary records from asset data.
    
    For each control, calculates:
    - OVERALL_STATUS: FAILED > PASSED > NOT_ASSESSED > NOT_EVALUATED
    - SCORE: PASSED / (PASSED + FAILED) for evaluated assets
    - Counts of PASSED, FAILED, NOT_ASSESSED assets
    """
    print("\n" + "="*60)
    print("STAGE 6: Creating Control Summaries")
    print("="*60)
    
    all_summaries = []
    
    for anchor in anchors:
        profile_name = anchor["profile_name"]
        standard_name = anchor["standard_name"]
        pair_key = f"{profile_name}|{standard_name}"
        
        print(f"\n  Processing: {profile_name} / {standard_name[:40]}...")
        
        # Get assets for this pair
        assets = all_assets.get(pair_key, [])
        
        # Get controls for this standard
        standard_data = controls_catalog.get(standard_name, {})
        controls = standard_data.get("controls", [])
        
        if not controls:
            print(f"    ⚠ No controls found for standard")
            continue
        
        # Build controls lookup by revision
        controls_lookup = {str(c.get("revision")): c for c in controls}
        
        # Group assets by control revision
        assets_by_control = {}
        for asset in assets:
            revision = asset.get("CONTROL_REVISION", str(asset.get("CONTROL_REVISION_ID", "")))
            if revision not in assets_by_control:
                assets_by_control[revision] = []
            assets_by_control[revision].append(asset)
        
        # Create summary for each control
        eval_datetime = anchor.get("last_evaluation_time")
        eval_datetime_str = datetime.fromtimestamp(eval_datetime / 1000).isoformat() if eval_datetime else None
        
        for control in controls:
            revision = str(control.get("revision"))
            control_assets = assets_by_control.get(revision, [])
            
            # Count statuses
            status_counts = {"PASSED": 0, "FAILED": 0, "NOT_ASSESSED": 0}
            for asset in control_assets:
                status = asset.get("STATUS", "").upper()
                if status in status_counts:
                    status_counts[status] += 1
            
            passed = status_counts["PASSED"]
            failed = status_counts["FAILED"]
            not_assessed = status_counts["NOT_ASSESSED"]
            total = passed + failed + not_assessed
            
            # Determine overall status (priority: FAILED > PASSED > NOT_ASSESSED > NOT_EVALUATED)
            if failed > 0:
                overall_status = "FAILED"
            elif passed > 0:
                overall_status = "PASSED"
            elif not_assessed > 0:
                overall_status = "NOT_ASSESSED"
            else:
                overall_status = "NOT_EVALUATED"
            
            # Calculate score (PASSED / (PASSED + FAILED))
            score = 0.0
            if passed + failed > 0:
                score = passed / (passed + failed)
            
            # Create summary record
            summary_record = {
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
            }
            
            all_summaries.append(summary_record)
        
        # Print stats for this pair
        pair_summaries = [s for s in all_summaries if s["ASSESSMENT_PROFILE_NAME"] == profile_name and s["STANDARD_NAME"] == standard_name]
        status_dist = {}
        for s in pair_summaries:
            status_dist[s["STATUS"]] = status_dist.get(s["STATUS"], 0) + 1
        
        print(f"    Controls: {len(pair_summaries)}")
        print(f"    Status distribution: {status_dist}")
        
        # Calculate average score for evaluated controls
        evaluated = [s for s in pair_summaries if s["STATUS"] in ("PASSED", "FAILED")]
        if evaluated:
            avg_score = sum(s["SCORE"] for s in evaluated) / len(evaluated)
            print(f"    Average score: {avg_score:.2%}")
    
    print(f"\n✓ Total control summaries: {len(all_summaries)}")
    return all_summaries


def save_summaries_to_csv(summaries: List[dict], output_path: Path):
    """Save control summaries to a CSV file."""
    if not summaries:
        return
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=summaries[0].keys())
        writer.writeheader()
        writer.writerows(summaries)
    
    print(f"  ✓ Saved: {output_path.name}")


def push_summaries_to_collector(summaries: List[dict], config: Config, timestamp: str) -> dict:
    """Push control summaries to HTTP Collector using CONTROLS_API_KEY."""
    print("\n" + "="*60)
    print("STAGE 7: Push Summaries to HTTP Collector")
    print("="*60)
    
    if not summaries:
        print("⚠ No summaries to push")
        return {"summaries_pushed": 0, "skipped": True}
    
    print(f"Total summary records: {len(summaries):,}")
    
    # Save to NDJSON file (collector-ready format)
    ndjson_path = config.OUTPUT_DIR / f"summaries_collector_{timestamp}.json"
    save_records_to_ndjson(summaries, ndjson_path)
    print(f"  ✓ Saved: {ndjson_path.name} ({len(summaries):,} records)")
    
    # Print curl command for manual push
    if config.COLLECTOR_URL:
        print_curl_command(ndjson_path, config.COLLECTOR_URL, "CONTROLS_API_KEY")
    
    # Check if we should push
    if not config.COLLECTOR_URL:
        print("⚠ Skipping push: No collector URL configured")
        return {"summaries_pushed": 0, "skipped": True, "saved_file": str(ndjson_path)}
    
    if not config.CONTROLS_API_KEY:
        print("⚠ Skipping push: No CONTROLS_API_KEY configured")
        return {"summaries_pushed": 0, "skipped": True, "saved_file": str(ndjson_path)}
    
    print(f"Dry run mode: {config.DRY_RUN}")
    
    result = push_to_collector(
        records=summaries,
        collector_url=config.COLLECTOR_URL,
        api_key=config.CONTROLS_API_KEY,
        batch_size=config.BATCH_SIZE,
        dry_run=config.DRY_RUN,
        label="Summary"
    )
    
    return {"summaries_pushed": result.get("pushed", 0), "saved_file": str(ndjson_path), **result}


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("\n" + "="*60)
    print("  Cortex Cloud Compliance Data Fetcher (Local)")
    print("="*60)
    
    start_time = time.time()
    
    # Load configuration
    config = Config()
    
    # Create API client
    client = CortexAPIClient(config)
    
    # Stage 1: Get anchors
    anchors = get_all_anchors(client, config.ASSESSMENTS)
    if not anchors:
        print("\n❌ No anchors found. Check your assessments configuration.")
        return 1
    
    # Stage 2: Get controls catalog
    controls_catalog = get_all_controls(client, config.ASSESSMENTS, config.PARALLEL_WORKERS)
    
    # Stage 3: Fetch assets
    all_assets = fetch_all_assets(client, anchors, controls_catalog, config)
    
    # Generate timestamp for all output files
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Stage 4: Save outputs to files
    summary = save_all_outputs(all_assets, controls_catalog, anchors, config)
    
    # Stage 5: Push ASSETS to HTTP Collector (COMPLIANCE_API_KEY)
    assets_push_result = push_assets_to_collector(all_assets, config, timestamp)
    summary["assets_push_result"] = assets_push_result
    
    # Stage 6: Create Control Summaries
    control_summaries = create_control_summary(all_assets, controls_catalog, anchors)
    summary["total_summaries"] = len(control_summaries)
    
    # Save summaries to CSV
    if control_summaries:
        summaries_path = config.OUTPUT_DIR / f"control_summaries_{timestamp}.csv"
        save_summaries_to_csv(control_summaries, summaries_path)
    
    # Stage 7: Push SUMMARIES to HTTP Collector (CONTROLS_API_KEY)
    summaries_push_result = push_summaries_to_collector(control_summaries, config, timestamp)
    summary["summaries_push_result"] = summaries_push_result
    
    # Print final summary
    elapsed = time.time() - start_time
    print("\n" + "="*60)
    print("  COMPLETE")
    print("="*60)
    print(f"  Total time: {elapsed:.1f} seconds")
    print(f"  Anchors processed: {summary['total_anchors']}")
    print(f"  Controls fetched: {summary['total_controls']}")
    print(f"  Assets fetched: {summary['total_assets']}")
    print(f"  Control summaries: {summary['total_summaries']}")
    print(f"  Output directory: {config.OUTPUT_DIR}")
    
    # Assets push results
    apr = assets_push_result
    if apr.get("skipped"):
        print(f"  Assets push: skipped (not configured)")
    elif apr.get("dry_run"):
        print(f"  Assets push: {apr.get('assets_pushed', 0):,} records (DRY RUN)")
    else:
        print(f"  Assets push: {apr.get('assets_pushed', 0):,} records pushed")
    
    # Summaries push results
    spr = summaries_push_result
    if spr.get("skipped"):
        print(f"  Summaries push: skipped (not configured)")
    elif spr.get("dry_run"):
        print(f"  Summaries push: {spr.get('summaries_pushed', 0):,} records (DRY RUN)")
    else:
        print(f"  Summaries push: {spr.get('summaries_pushed', 0):,} records pushed")
    
    print("="*60 + "\n")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
