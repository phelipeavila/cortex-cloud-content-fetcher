"""ComplianceCreateSummary - Cortex XSOAR Script

Stage 3: Creates control-level summary reports from asset data
and optionally pushes to the HTTP Collector.

Supports multiple profile/standard pairs from context.

Prerequisites:
    - ComplianceLoadConfig must be run first.
    - ComplianceGetAnchor must be run.
    - ComplianceGetControls must be run.
    - ComplianceFetchAssets must be run.

Arguments:
    push_to_collector (bool): Optional. Whether to push summary to HTTP Collector.
                              Defaults to True (respects DRY_RUN config).

Outputs:
    Context path: ComplianceSummaryResults (list of per-pair summaries)
        Each entry contains:
        - profile_name: Assessment profile name
        - standard_name: Standard name
        - total_controls: Total number of controls
        - passed_count: Number of passed controls
        - failed_count: Number of failed controls
        - compliance_score: Overall compliance score (0-1)

    File output: CSV file(s) with control summaries per pair
"""

# from CommonServerPython import *
import json
import requests
from datetime import datetime
from collections import defaultdict


# =============================================================================
# HELPER FUNCTIONS
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
    standard_data = catalog.get(standard_name)
    if not standard_data:
        demisto.debug(f"No controls found for standard: {standard_name}")
        return []

    # Handle case where standard_data might be a list of controls directly
    if isinstance(standard_data, list):
        return standard_data

    return standard_data.get("controls", [])


def get_assets_from_context():
    """Retrieve assets dict (keyed by pair) from context."""
    assets_data = demisto.context().get("ComplianceAssetsData")
    if assets_data and isinstance(assets_data, dict):
        return assets_data

    # Return empty dict if no assets
    demisto.debug("No assets found in context - will create empty summaries")
    return {}


def get_assets_for_pair(assets_data, profile_name, standard_name):
    """Get assets list for a specific profile/standard pair."""
    pair_key = f"{profile_name}|{standard_name}"
    return assets_data.get(pair_key, [])


def push_records_to_collector(records, collector_url, api_key, batch_size, dry_run, label="Summary"):
    """Push records to HTTP Collector in batches."""
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


def create_control_summary(controls, assets, anchor):
    """Create control-level summary from assets."""
    profile_name = anchor.get("profile_name")
    standard_name = anchor.get("standard_name")
    eval_timestamp = anchor.get("last_evaluation_time")
    eval_datetime = timestamp_to_datestring(eval_timestamp) if eval_timestamp else None

    demisto.info(f"Creating summary for {len(controls)} controls from {len(assets)} assets")

    # Build controls lookup
    controls_by_id = {c.get("control_id"): c for c in controls}

    # If no assets, create stub summary
    if not assets:
        demisto.debug("No assets - creating stub summary")
        summary_records = []
        for ctrl in controls:
            summary_records.append({
                "STANDARD_NAME": standard_name,
                "ASSESSMENT_PROFILE_NAME": profile_name,
                "ASSESSMENT_PROFILE_REVISION": anchor.get("assessment_profile_revision"),
                "ASSET_GROUP_ID": anchor.get("asset_group_id"),
                "EVALUATION_DATETIME": eval_datetime,
                "CONTROL_ID": ctrl.get("control_id"),
                "CONTROL_NAME": ctrl.get("control_name"),
                "SEVERITY": ctrl.get("severity"),
                "CATEGORY": ctrl.get("category", ""),
                "STATUS": "NOT_EVALUATED",
                "SCORE": 0.0,
                "RULES_COUNT": ctrl.get("rule_count", len(ctrl.get("rule_names", []))),
                "PASSED_ASSETS": 0,
                "FAILED_ASSETS": 0,
                "NOT_ASSESSED_ASSETS": 0,
                "TOTAL_ASSETS": 0
            })
        return summary_records

    # Aggregate assets by control and asset ID (get worst status per asset)
    # Status priority: FAILED > NOT_ASSESSED > PASSED
    status_priority = {"FAILED": 3, "NOT_ASSESSED": 2, "PASSED": 1}

    # Group assets by control
    control_assets = defaultdict(list)
    for asset in assets:
        control_id = asset.get("CONTROL_ID")
        if control_id:
            control_assets[control_id].append(asset)

    # Create summary for each control
    summary_records = []

    for ctrl in controls:
        control_id = ctrl.get("control_id")
        control_name = ctrl.get("control_name")

        # Get assets for this control
        ctrl_assets = control_assets.get(control_id, [])

        # Group by unique asset and get worst status
        asset_worst_status = {}
        for asset in ctrl_assets:
            asset_id = asset.get("ASSET_ID")
            status = asset.get("STATUS", "NOT_ASSESSED")

            if asset_id not in asset_worst_status:
                asset_worst_status[asset_id] = status
            else:
                current_priority = status_priority.get(asset_worst_status[asset_id], 0)
                new_priority = status_priority.get(status, 0)
                if new_priority > current_priority:
                    asset_worst_status[asset_id] = status

        # Count statuses
        passed_count = sum(1 for s in asset_worst_status.values() if s == "PASSED")
        failed_count = sum(1 for s in asset_worst_status.values() if s == "FAILED")
        not_assessed_count = sum(1 for s in asset_worst_status.values() if s == "NOT_ASSESSED")
        total_assets = len(asset_worst_status)

        # Determine overall status
        if failed_count > 0:
            overall_status = "FAILED"
        elif passed_count > 0:
            overall_status = "PASSED"
        elif not_assessed_count > 0:
            overall_status = "NOT_ASSESSED"
        else:
            overall_status = "NOT_EVALUATED"

        # Calculate score (only from PASSED and FAILED)
        scoreable = passed_count + failed_count
        score = passed_count / scoreable if scoreable > 0 else 0.0

        summary_records.append({
            "STANDARD_NAME": standard_name,
            "ASSESSMENT_PROFILE_NAME": profile_name,
            "ASSESSMENT_PROFILE_REVISION": anchor.get("assessment_profile_revision"),
            "ASSET_GROUP_ID": anchor.get("asset_group_id"),
            "EVALUATION_DATETIME": eval_datetime,
            "CONTROL_ID": control_id,
            "CONTROL_NAME": control_name,
            "SEVERITY": ctrl.get("severity"),
            "CATEGORY": ctrl.get("category", ""),
            "STATUS": overall_status,
            "SCORE": round(score, 4),
            "RULES_COUNT": ctrl.get("rule_count", len(ctrl.get("rule_names", []))),
            "PASSED_ASSETS": passed_count,
            "FAILED_ASSETS": failed_count,
            "NOT_ASSESSED_ASSETS": not_assessed_count,
            "TOTAL_ASSETS": total_assets
        })

    return summary_records


# =============================================================================
# MAIN FUNCTION
# =============================================================================

def main():
    try:
        args = demisto.args()
        push_enabled = argToBoolean(args.get('push_to_collector', 'true'))

        # Get data from context
        config = get_config_from_context()
        anchors = get_anchors_from_context()
        controls_catalog = get_controls_catalog_from_context()
        assets_data = get_assets_from_context()

        dry_run = config.get("DRY_RUN", True)
        batch_size = config.get("BATCH_SIZE", 500)
        collector_url = config.get("COLLECTOR_URL")
        controls_api_key = config.get("CONTROLS_API_KEY")

        demisto.info(f"Creating summaries for {len(anchors)} profile/standard pair(s)")

        # Process each anchor
        all_results = []
        file_results = []
        all_scores = []
        total_controls = 0
        total_pushed = 0
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        for anchor in anchors:
            profile_name = anchor.get("profile_name")
            standard_name = anchor.get("standard_name")

            demisto.info(f"Creating summary for {profile_name}/{standard_name}")

            # Get controls and assets for this pair
            controls = get_controls_for_standard(controls_catalog, standard_name)
            assets = get_assets_for_pair(assets_data, profile_name, standard_name)

            if not controls:
                demisto.debug(f"No controls found for {standard_name}, creating empty summary")

            # Create summary
            summary_records = create_control_summary(controls, assets, anchor)
            total_controls += len(summary_records)

            # Push to collector if enabled
            pushed_count = 0
            if push_enabled and summary_records:
                pushed_count = push_records_to_collector(
                    summary_records,
                    collector_url,
                    controls_api_key,
                    batch_size,
                    dry_run,
                    label=f"Summary ({profile_name}/{standard_name})"
                )
                total_pushed += pushed_count

            # Calculate statistics for this pair
            status_counts = defaultdict(int)
            pair_score_total = 0
            pair_scored_count = 0

            for record in summary_records:
                status_counts[record.get("STATUS")] += 1
                if record.get("STATUS") in ["PASSED", "FAILED"]:
                    pair_score_total += record.get("SCORE", 0)
                    pair_scored_count += 1

            compliance_score = pair_score_total / pair_scored_count if pair_scored_count > 0 else 0.0
            if pair_scored_count > 0:
                all_scores.append(compliance_score)

            # Store result for this pair
            pair_result = {
                "profile_name": profile_name,
                "standard_name": standard_name,
                "total_controls": len(summary_records),
                "passed_count": status_counts.get("PASSED", 0),
                "failed_count": status_counts.get("FAILED", 0),
                "not_assessed_count": status_counts.get("NOT_ASSESSED", 0),
                "not_evaluated_count": status_counts.get("NOT_EVALUATED", 0),
                "compliance_score": round(compliance_score, 4),
                "pushed_count": pushed_count,
                "dry_run": dry_run
            }
            all_results.append(pair_result)

            # Create CSV file for this pair
            if summary_records:
                csv_lines = []
                csv_lines.append(",".join(summary_records[0].keys()))

                for record in summary_records:
                    row = []
                    for value in record.values():
                        str_val = str(value) if value is not None else ""
                        if '"' in str_val or ',' in str_val or '\n' in str_val:
                            str_val = '"' + str_val.replace('"', '""') + '"'
                        row.append(str_val)
                    csv_lines.append(",".join(row))

                csv_content = "\n".join(csv_lines)
                filename = f"compliance_summary_{profile_name}_{standard_name}_{timestamp}.csv".replace(" ", "_")
                file_results.append(fileResult(filename, csv_content, EntryType.ENTRY_INFO_FILE))
                demisto.info(f"Created summary report: {filename}")

        # Calculate overall score
        overall_score = sum(all_scores) / len(all_scores) if all_scores else 0.0

        # Build readable output
        readable = f"## Compliance Control Summaries\n\n"
        readable += f"**Total Pairs Processed:** {len(anchors)}\n"
        readable += f"**Total Controls:** {total_controls}\n"
        readable += f"**Overall Compliance Score:** {overall_score:.2%}\n"
        readable += f"**Dry Run:** {'Yes' if dry_run else 'No'}\n\n"

        for result in all_results:
            readable += f"### {result['profile_name']} / {result['standard_name']}\n\n"
            readable += f"**Compliance Score:** {result['compliance_score']:.2%}\n\n"
            readable += "| Status | Count |\n"
            readable += "| :--- | :--- |\n"
            readable += f"| Passed | {result['passed_count']} |\n"
            readable += f"| Failed | {result['failed_count']} |\n"
            readable += f"| Not Assessed | {result['not_assessed_count']} |\n"
            readable += f"| Not Evaluated | {result['not_evaluated_count']} |\n"
            readable += f"| **Total** | **{result['total_controls']}** |\n\n"
            readable += f"**Pushed:** {result['pushed_count']}\n\n"
            readable += "---\n\n"

        demisto.info(f"Successfully created summaries for {len(anchors)} pair(s) with {total_controls} total controls")

        results = [
            CommandResults(
                outputs_prefix="ComplianceSummaryResults",
                outputs_key_field="profile_name",
                outputs=all_results,
                readable_output=readable
            )
        ]
        results.extend(file_results)

        return_results(results)

    except Exception as e:
        demisto.error(f"Failed to create summary: {e}")
        return_error(f"ComplianceCreateSummary failed: {e}")


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ in ('__main__', '__builtin__', 'builtins'):
    main()
