"""ComplianceFetchAssets - Cortex Cloud Script

Stage 2: Fetches compliance assets, enriches them with control data,
and optionally pushes to the HTTP Collector.

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
        - assets: List of enriched asset records (if small enough)

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
import time
from datetime import datetime

# =============================================================================
# CONFIGURATION
# =============================================================================

# Skip controls that have no rules (they won't return assets anyway)
SKIP_CONTROLS_WITHOUT_RULES = True

# Rate limiting (set to 0 for faster execution, increase if hitting API rate limits)
CONTROL_DELAY = 0

# Time estimation
SAMPLE_SIZE = 3  # Number of controls to sample for time estimation
TIMEOUT_BUFFER = 1.5  # Multiplier for recommended timeout


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
    """Retrieve controls catalog (dict keyed by standard) from context."""
    catalog = demisto.context().get("ComplianceControlsCatalog")
    if not catalog:
        raise DemistoException(
            "Controls catalog not found in context. "
            "Please run ComplianceGetControls first."
        )

    # XSOAR stores dict outputs as a list of single-key dicts:
    # [{'Standard1': {controls: [...]}}, {'Standard2': {controls: [...]}}]
    # We need to merge them into one dict keyed by standard name.

    if isinstance(catalog, list):
        catalog_dict = {}
        for item in catalog:
            if isinstance(item, dict):
                # Each item is {standard_name: {controls: [...], ...}}
                for key, value in item.items():
                    if isinstance(value, dict) and "controls" in value:
                        catalog_dict[key] = value
                    elif key == "standard_name" and "controls" in item:
                        # Alternative structure: {standard_name: "X", controls: [...]}
                        catalog_dict[value] = item
            elif isinstance(item, str):
                # Skip truncation messages like "...NOTE, too much data..."
                continue
        return catalog_dict

    elif isinstance(catalog, dict):
        # Check if it's a single standard entry
        if "standard_name" in catalog and "controls" in catalog:
            standard_name = catalog.get("standard_name")
            return {standard_name: catalog}

        # Check if already keyed by standard name
        for key, val in catalog.items():
            if isinstance(val, dict) and "controls" in val:
                return catalog

        # Unexpected structure
        return catalog

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
# PROGRESS TRACKING
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
        "is_complete": False,
        # Control-level progress for current anchor
        "current_pair": None,
        "processed_controls": [],  # Control names already processed for current pair
        "skipped_controls": [],  # Controls that timed out and were skipped
        "total_controls_in_pair": 0,
        "current_control": None  # Control currently being processed (for timeout detection)
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

    # Reset control-level progress for next anchor
    progress["current_pair"] = None
    progress["processed_controls"] = []
    progress["total_controls_in_pair"] = 0

    return progress


def start_anchor_progress(progress, pair_key, total_controls):
    """Start tracking control-level progress for an anchor."""
    # Only reset processed_controls if we're starting a new pair
    if progress.get("current_pair") != pair_key:
        progress["processed_controls"] = []
    progress["current_pair"] = pair_key
    progress["total_controls_in_pair"] = total_controls
    return progress


def update_progress_after_control(progress, control_name):
    """Update progress after processing a single control."""
    processed = progress.get("processed_controls", [])
    if control_name not in processed:
        processed.append(control_name)
    progress["processed_controls"] = processed
    return progress


def get_remaining_controls(control_names, progress, pair_key):
    """Get controls that haven't been processed yet for this pair."""
    # Check if we're resuming the same pair
    if progress.get("current_pair") == pair_key:
        processed = set(progress.get("processed_controls", []))
        skipped = set(progress.get("skipped_controls", []))
        return [c for c in control_names if c not in processed and c not in skipped]
    # New pair, all controls need processing
    return control_names


def skip_timed_out_control(progress, control_name):
    """Mark a control as skipped (timed out too many times)."""
    skipped = progress.get("skipped_controls", [])
    if control_name not in skipped:
        skipped.append(control_name)
    progress["skipped_controls"] = skipped
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
    result = demisto.executeCommand("core-api-post", {
        "uri": uri,
        "body": json.dumps(body)
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
# ASSET FETCHING
# =============================================================================

def get_paginated_assets(base_payload, page_size):
    """Fetch all assets with pagination using core-api-post."""
    all_assets = []
    search_from = 0
    total_count = None

    while True:
        payload = json.loads(json.dumps(base_payload))  # Deep copy
        payload["request_data"]["search_from"] = search_from
        payload["request_data"]["search_to"] = search_from + page_size

        try:
            response = post_to_api("/public_api/v1/compliance/get_assets", payload)
            reply = response.get("reply", {})
            page_assets = reply.get("assets", [])

            if total_count is None:
                total_count = reply.get("total_count", 0)
                if total_count > 0:
                    demisto.debug(f"Found {total_count} total assets to retrieve")

            if not page_assets:
                break

            all_assets.extend(page_assets)

            if len(all_assets) >= total_count:
                break

            search_from += page_size

        except Exception as e:
            demisto.error(f"Error during asset pagination: {e}")
            break

    return all_assets


def enrich_assets(raw_assets, anchor, controls_lookup):
    """Enrich raw assets with control data."""
    profile_name = anchor.get("profile_name")
    standard_name = anchor.get("standard_name")
    eval_timestamp = anchor.get("last_evaluation_time")
    eval_datetime = timestamp_to_datestring(eval_timestamp) if eval_timestamp else None

    enriched = []
    for asset in raw_assets:
        revision = str(asset.get("CONTROL_REVISION_ID"))
        control_data = controls_lookup.get(revision, {})

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
            "CATEGORY": control_data.get("category", ""),
            "SUBCATEGORY": control_data.get("subcategory", ""),
            "SCANNABLE_ASSET_TYPES": json.dumps(control_data.get("scannable_asset_types", []))
        })

    return enriched


def fetch_and_enrich_assets(anchor, controls, config, progress, all_assets_by_pair):
    """
    Fetch assets for all controls and enrich with control data.
    Saves progress after each control to support resume on timeout.
    """
    page_size = config.get("PAGE_SIZE", 100)
    profile_name = anchor.get("profile_name")
    standard_name = anchor.get("standard_name")
    pair_key = f"{profile_name}|{standard_name}"

    # Filter controls based on configuration
    if SKIP_CONTROLS_WITHOUT_RULES:
        scannable_controls = [c for c in controls if c.get("rule_names")]
        skipped_count = len(controls) - len(scannable_controls)
        controls = scannable_controls
    else:
        skipped_count = 0

    # Build controls lookup by revision
    controls_lookup = {str(c.get("revision")): c for c in controls}
    all_control_names = list(set(c.get("control_name") for c in controls))
    total_controls = len(all_control_names)

    # Check if a control timed out on previous run (current_control set but not processed)
    # If so, skip it to avoid infinite timeout loop
    last_control = progress.get("current_control")
    if last_control and progress.get("current_pair") == pair_key:
        processed = progress.get("processed_controls", [])
        if last_control not in processed:
            demisto.info(f"Control '{last_control}' timed out on previous run - skipping it")
            progress = skip_timed_out_control(progress, last_control)
            save_progress(progress)

    # Get remaining controls (skip already processed and skipped ones)
    control_names = get_remaining_controls(all_control_names, progress, pair_key)

    # Start tracking this anchor
    progress = start_anchor_progress(progress, pair_key, total_controls)
    # Save progress immediately so we know which anchor we're working on
    save_progress(progress)

    # Get existing assets for this pair (from previous partial run)
    existing_assets = all_assets_by_pair.get(pair_key, [])

    base_payload_data = {
        "assessment_profile_revision": anchor.get("assessment_profile_revision"),
        "last_evaluation_time": anchor.get("last_evaluation_time"),
    }

    processed_this_run = len(progress.get("processed_controls", []))
    demisto.info(f"Processing {len(control_names)} remaining controls for {pair_key} ({processed_this_run}/{total_controls} already done)")

    timed_out_count = len(progress.get("skipped_controls", []))
    estimate_info = {
        "total_controls": total_controls,
        "scannable_controls": total_controls,
        "skipped_no_rules": skipped_count,
        "skipped_timeout": timed_out_count,
        "remaining_controls": len(control_names),
        "already_processed": processed_this_run
    }

    all_new_assets = []

    # Process each control and save progress after each
    for i, control_name in enumerate(control_names):
        # Save which control we're about to process (before API call)
        progress["current_control"] = control_name
        save_progress(progress)
        demisto.info(f"Starting control {i+1}/{len(control_names)}: {control_name}")

        try:
            payload = {
                "request_data": {
                    **base_payload_data,
                    "filters": [
                        {"field": "control_name", "operator": "eq", "value": control_name},
                        {"field": "status", "operator": "neq", "value": "Not Assessed"}
                    ]
                }
            }
            assets = get_paginated_assets(payload, page_size)

            if assets:
                # Enrich immediately
                enriched = enrich_assets(assets, anchor, controls_lookup)
                all_new_assets.extend(enriched)

            # Update progress for this control
            progress = update_progress_after_control(progress, control_name)

            # Save progress and assets after each control
            all_assets_by_pair[pair_key] = existing_assets + all_new_assets
            save_progress(progress)
            demisto.setContext("ComplianceAssetsData", all_assets_by_pair)

            current_done = len(progress.get("processed_controls", []))
            demisto.info(f"Control {current_done}/{total_controls}: {control_name} - {len(assets)} assets")

        except Exception as e:
            demisto.debug(f"Error fetching assets for control {control_name}: {e}")
            continue

        time.sleep(CONTROL_DELAY)

    # Combine existing and new assets
    final_assets = existing_assets + all_new_assets

    return final_assets, estimate_info, progress


# =============================================================================
# MAIN FUNCTION
# =============================================================================

def main():
    try:
        args = demisto.args()
        push_enabled = argToBoolean(args.get('push_to_collector', 'true'))
        max_anchors = int(args.get('max_anchors', 0))
        reset_progress = argToBoolean(args.get('reset', 'false'))

        # Get data from context
        config = get_config_from_context()
        anchors = get_anchors_from_context()
        controls_catalog = get_controls_catalog_from_context()

        dry_run = config.get("DRY_RUN", True)
        batch_size = config.get("BATCH_SIZE", 500)
        collector_url = config.get("COLLECTOR_URL")
        compliance_api_key = config.get("COMPLIANCE_API_KEY")

        # Initialize or retrieve progress tracking
        progress = None if reset_progress else get_progress_from_context()

        if progress:
            demisto.info(f"Resuming from previous progress: {progress.get('completed_anchors')}/{progress.get('total_anchors')} completed")
        else:
            progress = initialize_progress(anchors)
            # Save initial progress immediately so it exists even if first API call times out
            save_progress(progress)
            demisto.info(f"Starting fresh with {len(anchors)} profile/standard pair(s)")

        # Filter to only pending anchors
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

        # Process each anchor
        all_results = []
        all_assets_by_pair = {}
        all_estimates = {}
        file_results = []
        total_assets = 0
        total_pushed = 0
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Get existing assets data if resuming
        existing_assets = demisto.context().get("ComplianceAssetsData")
        if existing_assets and isinstance(existing_assets, dict) and not reset_progress:
            all_assets_by_pair = existing_assets
            demisto.info(f"Loaded {len(all_assets_by_pair)} existing pair(s) from context")

        for anchor in anchors_to_process:
            profile_name = anchor.get("profile_name")
            standard_name = anchor.get("standard_name")
            pair_key = f"{profile_name}|{standard_name}"

            # Get controls for this standard
            controls = get_controls_for_standard(controls_catalog, standard_name)

            if not controls:
                # Include debug info in error for troubleshooting
                catalog_keys = list(controls_catalog.keys()) if isinstance(controls_catalog, dict) else f"Not a dict: {type(controls_catalog).__name__}"
                error_msg = f"No controls found. Looking for '{standard_name}'. Available keys: {catalog_keys}"
                all_results.append({
                    "profile_name": profile_name,
                    "standard_name": standard_name,
                    "total_assets": 0,
                    "pushed_count": 0,
                    "dry_run": dry_run,
                    "error": error_msg
                })
                continue

            # Fetch and enrich assets (saves progress after each control)
            enriched_assets, estimate_info, progress = fetch_and_enrich_assets(
                anchor, controls, config, progress, all_assets_by_pair
            )
            total_assets += len(enriched_assets)

            if estimate_info:
                all_estimates[pair_key] = estimate_info

            # Check if all controls were processed (or skipped) for this anchor
            processed_count = len(progress.get("processed_controls", []))
            skipped_count = len(progress.get("skipped_controls", []))
            total_in_pair = progress.get("total_controls_in_pair", 0)

            if (processed_count + skipped_count) >= total_in_pair:
                # Anchor complete - push to collector if enabled
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
                pair_result = {
                    "profile_name": profile_name,
                    "standard_name": standard_name,
                    "total_assets": len(enriched_assets),
                    "pushed_count": pushed_count,
                    "dry_run": dry_run
                }
                all_results.append(pair_result)

                # Mark anchor as complete
                progress = update_progress_after_anchor(progress, pair_key)
                save_progress(progress)
                demisto.info(f"Anchor complete: {progress.get('completed_anchors')}/{progress.get('total_anchors')}")
            else:
                # Anchor partially processed (should only happen on timeout)
                demisto.info(f"Anchor {pair_key} partially processed: {processed_count}/{total_in_pair} controls")

            # Create CSV file for this pair
            if enriched_assets:
                csv_lines = []
                csv_lines.append(",".join(enriched_assets[0].keys()))

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
                demisto.info(f"Created asset report: {filename}")

        # Store all assets in context for Stage 3 (final save)
        demisto.setContext("ComplianceAssetsData", all_assets_by_pair)

        # Build readable output
        readable = f"## Compliance Assets Fetched\n\n"

        # Progress status section
        readable += "### Progress Status\n\n"
        readable += f"| Metric | Value |\n"
        readable += f"| :--- | :--- |\n"
        readable += f"| Anchors Completed | {progress.get('completed_anchors')}/{progress.get('total_anchors')} |\n"
        readable += f"| Anchors Pending | {len(progress.get('pending_pairs', []))} |\n"

        # Show control-level progress if anchor is partially processed
        current_pair = progress.get("current_pair")
        if current_pair and current_pair not in progress.get("completed_pairs", []):
            processed_controls = len(progress.get("processed_controls", []))
            skipped_controls = len(progress.get("skipped_controls", []))
            total_controls = progress.get("total_controls_in_pair", 0)
            readable += f"| Current Anchor | {current_pair} |\n"
            readable += f"| Controls Processed | {processed_controls}/{total_controls} |\n"
            if skipped_controls > 0:
                readable += f"| Controls Skipped (timeout) | {skipped_controls} |\n"

        readable += f"| Status | {'COMPLETE' if progress.get('is_complete') else 'IN PROGRESS'} |\n\n"

        # Show skipped controls if any
        skipped_list = progress.get("skipped_controls", [])
        if skipped_list:
            readable += f"**Warning:** {len(skipped_list)} control(s) were skipped due to API timeout:\n"
            for ctrl in skipped_list[:5]:  # Show first 5
                readable += f"- {ctrl}\n"
            if len(skipped_list) > 5:
                readable += f"- ... and {len(skipped_list) - 5} more\n"
            readable += "\n"

        if not progress.get('is_complete'):
            pending_count = len(progress.get('pending_pairs', []))
            current_pair = progress.get("current_pair")
            if current_pair and current_pair not in progress.get("completed_pairs", []):
                readable += f"*Run script again to continue processing {current_pair} and {pending_count - 1} more pair(s).*\n\n"
            else:
                readable += f"*Run script again to continue processing remaining {pending_count} pair(s).*\n\n"

        readable += f"**Total Assets This Run:** {total_assets}\n"
        readable += f"**Total Pushed This Run:** {total_pushed}\n"
        readable += f"**Dry Run:** {'Yes' if dry_run else 'No'}\n"
        readable += f"**Skip Controls Without Rules:** {'Yes' if SKIP_CONTROLS_WITHOUT_RULES else 'No'}\n\n"

        # Show control processing details if available
        if all_estimates:
            readable += "### Control Processing\n\n"
            readable += "| Profile/Standard | Total | Skipped | Processed | Remaining |\n"
            readable += "| :--- | :--- | :--- | :--- | :--- |\n"
            for pair_key, est in all_estimates.items():
                total = est.get('total_controls', 0)
                skipped = est.get('skipped_controls', 0)
                remaining = est.get('remaining_controls', 0)
                already = est.get('already_processed', 0)
                readable += f"| {pair_key} | {total} | {skipped} | {already} | {remaining} |\n"
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
