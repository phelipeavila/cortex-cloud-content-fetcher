"""ComplianceFetchAssets - Cortex Cloud Script

Stage 2: Fetches compliance assets, enriches them with control data,
and optionally pushes to the HTTP Collector.

OPTIMIZED VERSION: Fetches ALL assets at once per anchor instead of
per-control, reducing API calls from 150+ to ~5-10 (pagination only).

Supports multiple profile/standard pairs from context.
Supports progress tracking for resumable execution on timeouts.

Prerequisites:
    - ComplianceLoadConfig must be run first to load configuration.
    - ComplianceGetAnchor must be run to get the assessment anchor(s).
    - ComplianceGetControls must be run to get the controls catalog.

Arguments:
    push_to_collector (bool): Optional. Whether to push data to HTTP Collector.
                              Defaults to True (respects DRY_RUN config).
    max_anchors (int): Optional. Maximum number of anchors to process per run.
                       Use this to avoid timeouts by processing in batches.
                       Defaults to 0 (process all).
    reset (bool): Optional. If True, clears previous progress and starts fresh.
                  Defaults to False (resume from previous progress).

Outputs:
    Context path: ComplianceAssetsResults (list of per-pair results)
        Each entry contains:
        - profile_name: Assessment profile name
        - standard_name: Standard name
        - total_assets: Total number of assets fetched
        - pushed_count: Number of records pushed to collector

    Context path: ComplianceAssetsProgress (progress tracking)
        - total_anchors: Total number of anchors to process
        - completed_anchors: Number of anchors completed
        - completed_pairs: List of completed pair keys (profile|standard)
        - pending_pairs: List of pending pair keys
        - is_complete: True if all anchors have been processed

    File output: CSV file(s) with asset records per pair
"""

# from CommonServerPython import *
import json
import requests
from datetime import datetime


# =============================================================================
# CONTEXT HELPERS
# =============================================================================

def get_config_from_context():
    """Retrieve configuration from context."""
    config = demisto.context().get("ComplianceConfigInternal")
    if not config:
        raise DemistoException(
            "Configuration not found in context. "
            "Please run ComplianceLoadConfig first."
        )
    return config


def get_anchors_from_context():
    """Retrieve list of anchors from context."""
    anchors = demisto.context().get("ComplianceAnchors")
    if not anchors:
        raise DemistoException(
            "Anchors not found in context. "
            "Please run ComplianceGetAnchor first."
        )
    # Ensure it's a list
    if isinstance(anchors, dict):
        anchors = [anchors]
    return anchors


def get_controls_catalog_from_context():
    """
    Retrieve controls catalog (dict keyed by standard) from context.
    
    After Phase 1 optimization, the catalog is stored directly as a dict:
    {
        "Standard1": {"controls": [...], "standard_name": "...", "total_controls": N},
        "Standard2": {"controls": [...], ...}
    }
    
    Legacy format (before optimization) was a list that needed merging.
    This function handles both formats for backwards compatibility.
    """
    catalog = demisto.context().get("ComplianceControlsCatalog")
    if not catalog:
        raise DemistoException(
            "Controls catalog not found in context. "
            "Please run ComplianceGetControls first."
        )

    # NEW FORMAT (Phase 1): Direct dict keyed by standard name
    if isinstance(catalog, dict):
        # Check if already in correct format (keyed by standard name)
        for key, val in catalog.items():
            if isinstance(val, dict) and "controls" in val:
                return catalog
        
        # Check if it's a single standard entry with flat structure
        if "standard_name" in catalog and "controls" in catalog:
            standard_name = catalog.get("standard_name")
            return {standard_name: catalog}

        # Unexpected dict structure - return as-is
        return catalog

    # LEGACY FORMAT: List of dicts (from CommandResults with outputs_prefix)
    # This handles old context data before the optimization was applied
    elif isinstance(catalog, list):
        catalog_dict = {}
        for item in catalog:
            if isinstance(item, dict):
                for key, value in item.items():
                    if isinstance(value, dict) and "controls" in value:
                        catalog_dict[key] = value
                    elif key == "standard_name" and "controls" in item:
                        catalog_dict[value] = item
            elif isinstance(item, str):
                # Skip truncation messages
                continue
        return catalog_dict

    else:
        raise DemistoException(
            f"Unexpected controls catalog type: {type(catalog).__name__}"
        )


def get_controls_for_standard(catalog, standard_name):
    """Get controls list for a specific standard from the catalog."""
    # Debug: Show available keys vs requested key
    available_keys = list(catalog.keys()) if isinstance(catalog, dict) else "N/A (not a dict)"
    demisto.info(f"Looking for '{standard_name}' in catalog keys: {available_keys}")

    standard_data = catalog.get(standard_name)
    if not standard_data:
        # Provide detailed error for debugging
        demisto.debug(f"No controls found for standard: {standard_name}. Available: {available_keys}")
        return []

    # Handle case where standard_data might be a list of controls directly
    if isinstance(standard_data, list):
        return standard_data

    return standard_data.get("controls", [])


# =============================================================================
# PROGRESS TRACKING (Simplified - per-anchor only)
# =============================================================================

def get_progress_from_context():
    """Retrieve progress tracking data from context."""
    progress = demisto.context().get("ComplianceAssetsProgress")
    if not progress:
        return None
    # Handle XSOAR list wrapping
    if isinstance(progress, list) and len(progress) > 0:
        progress = progress[0]
    return progress


def initialize_progress(anchors):
    """Initialize progress tracking for a new run."""
    all_pairs = []
    for anchor in anchors:
        pair_key = f"{anchor.get('profile_name')}|{anchor.get('standard_name')}"
        all_pairs.append(pair_key)

    return {
        "total_anchors": len(anchors),
        "completed_anchors": 0,
        "completed_pairs": [],
        "pending_pairs": all_pairs,
        "is_complete": False
    }


def save_progress(progress):
    """Save progress to context."""
    demisto.setContext("ComplianceAssetsProgress", progress)


def update_progress_after_anchor(progress, pair_key):
    """Update progress after successfully processing an anchor."""
    completed = progress.get("completed_pairs", [])
    pending = progress.get("pending_pairs", [])

    # Move pair from pending to completed
    if pair_key in pending:
        pending.remove(pair_key)
    if pair_key not in completed:
        completed.append(pair_key)

    progress["completed_pairs"] = completed
    progress["pending_pairs"] = pending
    progress["completed_anchors"] = len(completed)
    progress["is_complete"] = len(pending) == 0

    return progress


def get_pending_anchors(anchors, progress):
    """Filter anchors to only include pending ones."""
    if not progress:
        return anchors

    completed_pairs = set(progress.get("completed_pairs", []))
    pending_anchors = []

    for anchor in anchors:
        pair_key = f"{anchor.get('profile_name')}|{anchor.get('standard_name')}"
        if pair_key not in completed_pairs:
            pending_anchors.append(anchor)

    return pending_anchors


# =============================================================================
# API HELPERS
# =============================================================================

def parse_api_response(result, operation):
    """Parse the response from a core-api-post command."""
    if not result:
        raise DemistoException(f"{operation}: Empty response from API")

    for entry in result:
        if not isinstance(entry, dict):
            continue

        if entry.get("Type") == 4:
            continue

        contents = entry.get("Contents", {})
        if not isinstance(contents, dict):
            continue

        response = contents.get("response", {})
        if response and "reply" in response:
            return response

    error_msg = get_error(result) if is_error(result) else "No valid response found"
    raise DemistoException(f"{operation}: {error_msg}")


def post_to_api(uri, body):
    """Execute a POST request via core-api-post."""
    # Note: We don't checkpoint here to avoid too many context writes
    # The checkpoint before calling this function should be sufficient
    result = demisto.executeCommand("core-api-post", {
        "uri": uri,
        "body": json.dumps(body),
        "timeout": 300  # 5 minute timeout for API calls
    })
    return parse_api_response(result, f"POST {uri}")


def push_records_to_collector(records, collector_url, api_key, batch_size, dry_run, label="Asset"):
    """Push records to HTTP Collector in batches (uses direct requests)."""
    if dry_run:
        demisto.info(f"DRY RUN: Skipping push of {len(records)} records to {label} Collector")
        return 0

    if not api_key:
        demisto.error(f"API key for {label} Collector is missing. Aborting push.")
        return 0

    demisto.info(f"Pushing {len(records)} records to {label} Collector")
    total_pushed = 0
    collector_headers = {"Authorization": api_key, "Content-Type": "text/plain"}

    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        ndjson_body = "\n".join(json.dumps(r, default=str) for r in batch)

        try:
            resp = requests.post(collector_url, headers=collector_headers, data=ndjson_body, timeout=120)
            resp.raise_for_status()
            total_pushed += len(batch)
        except requests.HTTPError as e:
            demisto.error(f"HTTP Collector push failed: {e}\nResponse: {e.response.text}")
            continue

    demisto.info(f"Successfully pushed {total_pushed} records to {label} Collector")
    return total_pushed


# =============================================================================
# ASSET FETCHING (OPTIMIZED - Fetch all assets at once)
# =============================================================================

def get_asset_count_for_anchor(anchor):
    """Get total asset count for an anchor (single API call)."""
    payload = {
        "request_data": {
            "assessment_profile_revision": anchor.get("assessment_profile_revision"),
            "last_evaluation_time": anchor.get("last_evaluation_time"),
            "filters": [
                {"field": "status", "operator": "neq", "value": "Not Assessed"}
            ],
            "search_from": 0,
            "search_to": 1  # Just need count, not actual assets
        }
    }
    
    try:
        response = post_to_api("/public_api/v1/compliance/get_assets", payload)
        return response.get("reply", {}).get("total_count", 0)
    except Exception as e:
        demisto.error(f"Error getting asset count: {e}")
        return 0


def preflight_count_assets(anchors, page_size):
    """
    Pre-flight: Count assets for each anchor before fetching.
    Returns list of counts and total summary.
    """
    demisto.info("=== PRE-FLIGHT: Counting assets for each anchor ===")
    
    anchor_counts = []
    total_assets = 0
    total_pages = 0
    
    for i, anchor in enumerate(anchors):
        profile = anchor.get('profile_name', 'Unknown')
        standard = anchor.get('standard_name', 'Unknown')
        
        count = get_asset_count_for_anchor(anchor)
        pages = (count + page_size - 1) // page_size if count > 0 else 0
        
        anchor_counts.append({
            "index": i + 1,
            "profile": profile,
            "standard": standard,
            "assets": count,
            "pages": pages
        })
        
        total_assets += count
        total_pages += pages
        
        demisto.info(f"  [{i+1}] {profile[:30]} | {count:,} assets | {pages:,} pages")
    
    demisto.info(f"=== TOTAL: {total_assets:,} assets across {total_pages:,} API requests ===")
    
    return {
        "anchor_counts": anchor_counts,
        "total_assets": total_assets,
        "total_pages": total_pages
    }


def get_paginated_assets(base_payload, page_size):
    """
    Fetch all assets with pagination using core-api-post.
    OPTIMIZED: Now fetches ALL assets at once instead of per-control.
    """
    all_assets = []
    search_from = 0
    total_count = None
    page_num = 0

    while True:
        payload = json.loads(json.dumps(base_payload))  # Deep copy
        payload["request_data"]["search_from"] = search_from
        payload["request_data"]["search_to"] = search_from + page_size

        try:
            response = post_to_api("/public_api/v1/compliance/get_assets", payload)
            page_num += 1
            reply = response.get("reply", {})
            page_assets = reply.get("assets", [])

            if total_count is None:
                total_count = reply.get("total_count", 0)
                demisto.info(f"Fetching {total_count} total assets (page size: {page_size})")

            if not page_assets:
                break

            all_assets.extend(page_assets)
            demisto.info(f"Page {page_num}: fetched {len(page_assets)} assets ({len(all_assets)}/{total_count})")

            if len(all_assets) >= total_count:
                break

            search_from += page_size

        except Exception as e:
            demisto.error(f"Error during asset pagination (page {page_num}): {e}")
            break

    return all_assets, total_count or 0


def enrich_assets(raw_assets, anchor, controls_lookup):
    """Enrich raw assets with control data using CONTROL_REVISION_ID as join key."""
    profile_name = anchor.get("profile_name")
    standard_name = anchor.get("standard_name")
    eval_timestamp = anchor.get("last_evaluation_time")
    eval_datetime = timestamp_to_datestring(eval_timestamp) if eval_timestamp else None

    enriched = []
    enrichment_stats = {"matched": 0, "unmatched": 0}
    
    for asset in raw_assets:
        revision = str(asset.get("CONTROL_REVISION_ID"))
        control_data = controls_lookup.get(revision, {})
        
        if control_data:
            enrichment_stats["matched"] += 1
        else:
            enrichment_stats["unmatched"] += 1

        enriched.append({
            "ASSESSMENT_PROFILE_NAME": profile_name,
            "ASSESSMENT_PROFILE_REVISION": anchor.get("assessment_profile_revision"),
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

    demisto.info(f"Enrichment complete: {enrichment_stats['matched']} matched, {enrichment_stats['unmatched']} unmatched")
    return enriched


def fetch_and_enrich_assets(anchor, controls, config):
    """
    OPTIMIZED: Fetch ALL assets at once and enrich in memory.
    
    Instead of 150+ API calls (one per control), makes 1 API call
    with pagination (~5-10 calls total) to fetch all assets.
    """
    page_size = config.get("PAGE_SIZE", 100)
    profile_name = anchor.get("profile_name")
    standard_name = anchor.get("standard_name")
    
    demisto.info(f"Fetching all assets for {profile_name}/{standard_name}")

    # Build controls lookup by revision (CONTROL_REVISION_ID is our join key)
    controls_lookup = {str(c.get("revision")): c for c in controls}
    demisto.info(f"Built controls lookup with {len(controls_lookup)} entries")

    # Build payload - fetch ALL assets at once (no control_name filter)
    payload = {
        "request_data": {
            "assessment_profile_revision": anchor.get("assessment_profile_revision"),
            "last_evaluation_time": anchor.get("last_evaluation_time"),
            "filters": [
                {"field": "status", "operator": "neq", "value": "Not Assessed"}
            ]
        }
    }

    # Fetch ALL assets with pagination
    raw_assets, total_count = get_paginated_assets(payload, page_size)
    demisto.info(f"Fetched {len(raw_assets)} assets (expected: {total_count})")

    # Enrich all assets in memory
    enriched_assets = enrich_assets(raw_assets, anchor, controls_lookup)

    # Return summary info
    fetch_info = {
        "total_controls": len(controls),
        "total_assets_fetched": len(raw_assets),
        "total_assets_enriched": len(enriched_assets)
    }

    return enriched_assets, fetch_info


# =============================================================================
# MAIN FUNCTION
# =============================================================================

def main():
    try:
        args = demisto.args()
        push_enabled = argToBoolean(args.get('push_to_collector', 'true'))
        max_anchors = int(args.get('max_anchors', 0))
        reset_progress = argToBoolean(args.get('reset', 'false'))

        demisto.info(f"ComplianceFetchAssets starting: push={push_enabled}, max_anchors={max_anchors}, reset={reset_progress}")

        # Get required context data
        config = get_config_from_context()
        anchors = get_anchors_from_context()
        
        try:
            controls_catalog = get_controls_catalog_from_context()
            demisto.info(f"Loaded controls catalog with {len(controls_catalog)} standards")
        except Exception as e:
            demisto.info(f"Warning: Could not load controls catalog: {e}. Proceeding without control enrichment.")
            controls_catalog = {}

        dry_run = config.get("DRY_RUN", True)
        batch_size = config.get("BATCH_SIZE", 500)
        collector_url = config.get("COLLECTOR_URL")
        compliance_api_key = config.get("COMPLIANCE_API_KEY")

        # Initialize or load progress
        progress = None if reset_progress else get_progress_from_context()

        # Auto-reset if previous run completed successfully
        if progress and progress.get('is_complete'):
            demisto.info("Previous run completed. Auto-resetting for new run.")
            progress = None

        if progress:
            demisto.info(f"Resuming: {progress.get('completed_anchors')}/{progress.get('total_anchors')} completed")
        else:
            progress = initialize_progress(anchors)
            save_progress(progress)
            demisto.info(f"Starting fresh with {len(anchors)} anchor(s)")

        # Filter to pending anchors only
        pending_anchors = get_pending_anchors(anchors, progress)

        if not pending_anchors:
            demisto.info("All anchors have been processed")
            readable = "## Compliance Assets Fetched\n\n"
            readable += "**Status:** All anchors have been processed.\n"
            readable += f"**Total Completed:** {progress.get('completed_anchors')}/{progress.get('total_anchors')}\n"
            return_results(CommandResults(
                outputs_prefix="ComplianceAssetsProgress",
                outputs=progress,
                readable_output=readable
            ))
            return

        # Limit to max_anchors if specified
        if max_anchors > 0 and len(pending_anchors) > max_anchors:
            anchors_to_process = pending_anchors[:max_anchors]
            demisto.info(f"Limiting to {max_anchors} anchors (of {len(pending_anchors)} pending)")
        else:
            anchors_to_process = pending_anchors

        demisto.info(f"Processing {len(anchors_to_process)} anchor(s) this run")

        # =====================================================================
        # PRE-FLIGHT: Count assets for each anchor
        # =====================================================================
        page_size = config.get("PAGE_SIZE", 100)
        preflight = preflight_count_assets(anchors_to_process, page_size)
        
        # Store preflight info for readable output
        demisto.info(f"Pre-flight complete: {preflight['total_assets']:,} assets, {preflight['total_pages']:,} API calls needed")

        # Process each anchor
        all_results = []
        all_assets_by_pair = {}
        all_fetch_info = {}
        file_results = []
        total_assets = 0
        total_pushed = 0
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        for anchor_idx, anchor in enumerate(anchors_to_process):
            profile_name = anchor.get("profile_name")
            standard_name = anchor.get("standard_name")
            pair_key = f"{profile_name}|{standard_name}"

            demisto.info(f"Processing anchor {anchor_idx + 1}/{len(anchors_to_process)}: {pair_key}")

            # Get controls for this standard
            controls = get_controls_for_standard(controls_catalog, standard_name)

            if not controls:
                catalog_keys = list(controls_catalog.keys()) if isinstance(controls_catalog, dict) else "N/A"
                error_msg = f"No controls found for '{standard_name}'. Available: {catalog_keys}"
                demisto.info(error_msg)
                all_results.append({
                    "profile_name": profile_name,
                    "standard_name": standard_name,
                    "total_assets": 0,
                    "pushed_count": 0,
                    "dry_run": dry_run,
                    "error": error_msg
                })
                continue

            # OPTIMIZED: Fetch ALL assets at once and enrich in memory
            enriched_assets, fetch_info = fetch_and_enrich_assets(anchor, controls, config)
            total_assets += len(enriched_assets)
            all_fetch_info[pair_key] = fetch_info

            # Store assets for this pair
            all_assets_by_pair[pair_key] = enriched_assets

            # Push to collector if enabled
            pushed_count = 0
            if push_enabled and enriched_assets:
                pushed_count = push_records_to_collector(
                    enriched_assets,
                    collector_url,
                    compliance_api_key,
                    batch_size,
                    dry_run,
                    label=f"Asset ({profile_name}/{standard_name})"
                )
                total_pushed += pushed_count

            # Store result for this pair
            all_results.append({
                "profile_name": profile_name,
                "standard_name": standard_name,
                "total_assets": len(enriched_assets),
                "pushed_count": pushed_count,
                "dry_run": dry_run
            })

            # Mark anchor as complete
            progress = update_progress_after_anchor(progress, pair_key)
            save_progress(progress)
            demisto.info(f"Anchor complete: {progress.get('completed_anchors')}/{progress.get('total_anchors')} - {len(enriched_assets)} assets")

            # Create CSV file for this pair
            if enriched_assets:
                csv_lines = [",".join(enriched_assets[0].keys())]
                for asset in enriched_assets:
                    row = []
                    for value in asset.values():
                        str_val = str(value) if value is not None else ""
                        if '"' in str_val or ',' in str_val or '\n' in str_val:
                            str_val = '"' + str_val.replace('"', '""') + '"'
                        row.append(str_val)
                    csv_lines.append(",".join(row))

                csv_content = "\n".join(csv_lines)
                filename = f"compliance_assets_{profile_name}_{standard_name}_{timestamp}.csv".replace(" ", "_")
                file_results.append(fileResult(filename, csv_content, EntryType.ENTRY_INFO_FILE))

        # Store all assets in context for Stage 3
        demisto.setContext("ComplianceAssetsData", all_assets_by_pair)

        # Build readable output
        readable = "## Compliance Assets Fetched (Optimized)\n\n"
        readable += f"**Total Anchors:** {progress.get('completed_anchors')}/{progress.get('total_anchors')}\n"
        readable += f"**Total Assets:** {total_assets}\n"
        readable += f"**Total Pushed:** {total_pushed}\n"
        readable += f"**Dry Run:** {'Yes' if dry_run else 'No'}\n"
        readable += f"**Status:** {'COMPLETE' if progress.get('is_complete') else 'IN PROGRESS'}\n\n"

        if not progress.get('is_complete'):
            pending_count = len(progress.get('pending_pairs', []))
            readable += f"*Run script again to process remaining {pending_count} anchor(s).*\n\n"

        # Show pre-flight summary (API calls needed)
        readable += "### Pre-flight Summary (Assets & API Calls)\n\n"
        readable += "| # | Profile | Standard | Assets | API Calls |\n"
        readable += "| :--- | :--- | :--- | ---: | ---: |\n"
        for ac in preflight.get('anchor_counts', []):
            readable += f"| {ac['index']} | {ac['profile'][:25]} | {ac['standard'][:35]} | {ac['assets']:,} | {ac['pages']:,} |\n"
        readable += f"| | | **TOTAL** | **{preflight['total_assets']:,}** | **{preflight['total_pages']:,}** |\n\n"

        # Show fetch info
        if all_fetch_info:
            readable += "### Fetch Summary\n\n"
            readable += "| Profile/Standard | Controls | Assets Fetched |\n"
            readable += "| :--- | :--- | :--- |\n"
            for pair_key, info in all_fetch_info.items():
                readable += f"| {pair_key} | {info.get('total_controls', 0)} | {info.get('total_assets_fetched', 0)} |\n"
            readable += "\n"

        for result in all_results:
            readable += f"### {result['profile_name']} / {result['standard_name']}\n\n"
            readable += f"| Metric | Value |\n"
            readable += f"| :--- | :--- |\n"
            readable += f"| Assets | {result['total_assets']} |\n"
            readable += f"| Pushed | {result['pushed_count']} |\n"

            if result.get('error'):
                readable += f"| Error | {result['error']} |\n"
            readable += "\n"

        results = [
            CommandResults(
                outputs_prefix="ComplianceAssetsResults",
                outputs_key_field="profile_name",
                outputs=all_results,
                readable_output=readable
            ),
            CommandResults(
                outputs_prefix="ComplianceAssetsProgress",
                outputs=progress
            )
        ]
        results.extend(file_results)

        return_results(results)

    except Exception as e:
        demisto.error(f"Failed to fetch assets: {e}")
        return_error(f"ComplianceFetchAssets failed: {e}")


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ in ('__main__', '__builtin__', 'builtins'):
    main()
