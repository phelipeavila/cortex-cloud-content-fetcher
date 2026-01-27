from CommonServerPython import *
import json
import random
import requests
import pandas as pd
import numpy as np
from datetime import timezone, datetime
import time

# =================================================================================
# CONFIGURATION & SETUP
# =================================================================================

# 1. Fetch the configuration from XSOAR Lists
list_name = "Script_Compliance_Config"
res = demisto.executeCommand("getList", {"listName": list_name})

# 2. Error Handling
if is_error(res) or not res[0].get('Contents'):
    return_error(f"Configuration List '{list_name}' not found or empty.")

# 3. Load the JSON data
try:
    config_data = json.loads(res[0]['Contents'])
except ValueError:
    return_error(f"The content of List '{list_name}' is not valid JSON.")

# --- Cortex API Configuration ---
CC_FQDN       = config_data.get('CC_FQDN')
CC_API_KEY_ID = config_data.get('CC_API_KEY_ID')
CC_API_KEY    = config_data.get('CC_API_KEY')

# --- HTTP Collector Configuration ---
TENANT_EXTERNAL_URL = config_data.get('TENANT_EXTERNAL_URL')
COLLECTOR_URL = f"https://api-{TENANT_EXTERNAL_URL}/logs/v1/event"

Compliance_API_KEY = config_data.get('COMPLIANCE_API_KEY')
Controls_API_KEY   = config_data.get('Controls_API_KEY')

# --- Script Parameters ---
# We use .get(key, default_value) to be safe
PAGE_SIZE  = config_data.get('PAGE_SIZE', 100)
BATCH_SIZE = config_data.get('BATCH_SIZE', 500)

# DRY_RUN Control
# If true, data is NOT sent to HTTP collector
DRY_RUN = config_data.get('DRY_RUN', True)

# Debug output to confirm settings (visible in War Room if Debug Mode is On)
demisto.debug(f"Config Loaded. Dry Run: {DRY_RUN}, Batch Size: {BATCH_SIZE}")

# --- Global Headers ---
CC_HEADERS = {
    "x-xdr-auth-id": str(CC_API_KEY_ID),
    "Authorization": CC_API_KEY,
    "Accept": "application/json",
    "Content-Type": "application/json",
}

# =================================================================================
# HELPER FUNCTIONS
# =================================================================================
def _post_to_cc_api(path, body, timeout=120):
    """
    Generic POST request handler for the Cortex API with auto-retry
    for network errors and 5xx/429 server responses.
    """
    url = f"https://{CC_FQDN}{path}"

    # Configuration
    max_retries = 3
    base_delay = 2  # seconds start

    for attempt in range(max_retries + 1):
        try:
            r = requests.post(url, headers=CC_HEADERS, json=body, timeout=timeout)

            # If we get a 429 (Rate Limit) or 5xx (Server Error), raise exception to trigger retry
            if r.status_code in [429, 500, 502, 503, 504]:
                r.raise_for_status() # This raises HTTPError

            # If we get here, it might be 200 (OK) or 4xx (Client Error like 400/401/404)
            # We do NOT want to retry 4xx errors (except 429), so we raise immediately.
            r.raise_for_status()

            return r.json()

        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ProxyError,
                requests.exceptions.ChunkedEncodingError) as e:
            # These are purely network level errors. We ALWAYS retry these.
            pass # Drop down to the sleep block

        except requests.exceptions.HTTPError as e:
            # Only retry 5xx and 429. If it's 400, 401, 403, 404, we crash immediately.
            if e.response.status_code not in [429, 500, 502, 503, 504]:
                raise Exception(f"Cortex API Client Error {e.response.status_code} for {path}: {e.response.text}")
            # If it IS 5xx/429, we pass to the sleep block
            pass

        # --- RETRY LOGIC ---
        if attempt < max_retries:
            sleep_time = (base_delay * (2 ** attempt)) + random.uniform(0.1, 1.0)
            demisto.info(f"Network/Server issue calling {path}. Retrying in {sleep_time:.2f}s... (Attempt {attempt+1}/{max_retries})")
            time.sleep(sleep_time)
        else:
            # Out of retries
            raise Exception(f"Failed calling {path} after {max_retries} retries.")

def push_records_to_collector(records_to_push, collector_url, api_key, label="Generic"):
    """Pushes a list of records to a specified HTTP Collector in batches."""

    # --- DRY RUN LOGIC START ---
    if DRY_RUN:
        demisto.info(f"DRY RUN ENABLED: Skipping push of {len(records_to_push)} records to {label} Collector.")
        return 0
    # --- DRY RUN LOGIC END ---

    if not api_key:
        demisto.error(f"API key for {label} Collector is missing. Aborting push.")
        return 0

    demisto.info(f"Starting push of {len(records_to_push)} records to {label} Collector")
    total_pushed = 0
    collector_headers = {"Authorization": api_key, "Content-Type": "text/plain"}

    for i in range(0, len(records_to_push), BATCH_SIZE):
        batch = records_to_push[i:i + BATCH_SIZE]
        ndjson_body = "\n".join(json.dumps(r, default=str) for r in batch)
        try:
            resp = requests.post(collector_url, headers=collector_headers, data=ndjson_body, timeout=120)
            resp.raise_for_status()
            total_pushed += len(batch)
        except requests.HTTPError as e:
            demisto.error(f"HTTP Collector ({label}) push failed for a batch: {e}\nResponse: {e.response.text}")
            continue

    demisto.info(f"Successfully pushed {total_pushed} total records to {label}.")
    return total_pushed

def get_all_paginated_assets(base_payload, silent=False):
    """
    Calls the /get_assets API and handles pagination to fetch all asset results.
    """
    all_assets = []
    search_from = 0
    total_count = None
    if not silent:
        demisto.debug("Calling /get_assets API to fetch all assets (with pagination)...")

    while True:
        base_payload["request_data"]["search_from"] = search_from
        base_payload["request_data"]["search_to"] = search_from + PAGE_SIZE

        try:
            response = _post_to_cc_api("/public_api/v1/compliance/get_assets", base_payload)
            reply = response.get("reply", {})
            page_assets = reply.get("assets", [])

            if total_count is None:
                total_count = reply.get("total_count", 0)
                if not silent and total_count > 0:
                    demisto.debug(f"Found a total of {total_count} asset results to retrieve.")

            if not page_assets:
                if not silent and total_count > 0 and len(all_assets) == total_count:
                    demisto.debug("All pages fetched.")
                break

            all_assets.extend(page_assets)
            if not silent:
                demisto.debug(f"Fetched page with {len(page_assets)} assets (Total retrieved: {len(all_assets)} / {total_count})")

            if len(all_assets) >= total_count:
                if not silent and total_count > 0:
                    demisto.debug("All assets retrieved.")
                break

            search_from += PAGE_SIZE

        except Exception as e:
            if not silent:
                demisto.error(f"Error during pagination for /get_assets: {e}")
            break

    return all_assets

# =================================================================================
# SCRIPT STAGES
# =================================================================================

def stage_0_get_assessment_anchor(profile_name, standard_name):
    """Finds the latest assessment run anchor."""
    demisto.info(f"Stage 0: Finding Assessment Run for Profile '{profile_name}' and Standard '{standard_name}'")
    request_body = {"request_data": {"filters": []}}
    j = _post_to_cc_api("/public_api/v1/compliance/get_assessment_results", request_body)
    data = (j.get("reply") or {}).get("data") or []
    if not data:
        raise DemistoException("No assessment results returned from the API.")

    matches = [d for d in data if str(d.get("TYPE", "")).lower() == "profile" and str(d.get("ASSESSMENT_PROFILE", "")).strip() == profile_name.strip() and str(d.get("STANDARD_NAME", "")).strip() == standard_name.strip()]

    if not matches:
        raise DemistoException(f"No assessment run found for Profile '{profile_name}' and Standard '{standard_name}'.")

    latest = sorted(matches, key=lambda x: int(x.get("LAST_EVALUATION_TIME", 0)), reverse=True)[0]
    anchor = {
        "assessment_profile_revision": latest.get("ASSESSMENT_PROFILE_REVISION"),
        "last_evaluation_time": latest.get("LAST_EVALUATION_TIME"),
        "asset_group_id": latest.get("ASSET_GROUP_ID")
    }
    if not all(anchor.values()):
        raise DemistoException("Found matching run, but it's missing required data.")
    demisto.debug(f"Successfully found anchor: {anchor}")
    return anchor

def fetch_single_control_details(cid, progress=None):
    """Worker function for fetching a single control's details.

    Args:
        cid: Control ID to fetch
        progress: Optional dict with 'count' and 'total' for progress tracking
    """
    try:
        sev_map = {
            "SEV_050_CRITICAL": "Critical",
            "SEV_040_HIGH": "High",
            "SEV_030_MEDIUM": "Medium",
            "SEV_020_LOW": "Low",
            "SEV_010_INFO": "Informational"
        }
        d = _post_to_cc_api("/public_api/v1/compliance/get_control", {"request_data": {"id": cid}})
        ctrl = (d.get("reply") or {}).get("control")[0]
        if ctrl.get('STATUS') != 'ACTIVE':
            return None
        rules = ctrl.get("COMPLIANCE_RULES") or []
        return {
            "control_id": ctrl.get("CONTROL_ID", cid),
            "control_name": ctrl.get("CONTROL_NAME", ""),
            "revision": str(ctrl.get("REVISION")),
            "severity": sev_map.get(ctrl.get("SEVERITY"), "Unknown"),
            "rule_names": sorted({str(r["NAME"]) for r in rules if "NAME" in r}),
            "scannable_asset_types": sorted({str(at) for r in rules for at in r.get("SCANNABLE_ASSETS", [])}),
        }
    except Exception as e:
        demisto.debug(f"Error fetching control ID {cid}: {e}")
        return None
    finally:
        # Handle progress logging if progress dict is provided
        if progress is not None:
            progress["count"] += 1
            if progress["count"] % 50 == 0 or progress["count"] == progress["total"]:
                demisto.debug(f"Progress: [{progress['count']}/{progress['total']}] controls processed...")

def stage_1_get_controls_catalog(standard_name):
    """
    Fetches all controls for the standard in serial mode.
    """
    demisto.info(f"Stage 1: Fetching Compliance Controls for Standard '{standard_name}' (Serial Mode)")
    body = {"request_data": {"filters": [{"field": "name", "operator": "eq", "value": standard_name}]}}
    resp = _post_to_cc_api("/public_api/v1/compliance/get_standards", body)
    try:
        control_ids = list(set(resp["reply"]["standards"][0].get("controls_ids", [])))
        demisto.debug(f"Found {len(control_ids)} unique control IDs.")
    except (KeyError, IndexError):
        raise DemistoException(f"Could not retrieve control IDs for standard '{standard_name}'.")

    if not control_ids:
        return pd.DataFrame()

    # Local progress tracking
    progress = {"count": 0, "total": len(control_ids)}
    rows = []

    for cid in control_ids:
        result = fetch_single_control_details(cid, progress)
        if result:
            rows.append(result)
        time.sleep(0.2)  # 200ms delay between each control fetch

    demisto.info(f"Successfully fetched details for {len(rows)} active controls.")
    return pd.DataFrame(rows)

def stage_2_process_assets_and_push(anchor, controls_df, profile_name, standard_name):
    """
    Fetches assets, enriches them, and pushes to the collector.
    (Serial Mode)
    """
    demisto.info("Stage 2: Fetching, Enriching, and Pushing Asset Data (Serial Mode)")

    # --- Part 1: Setup ---
    controls_lookup = controls_df.set_index('revision').to_dict('index')
    control_names_to_fetch = list(controls_df['control_name'].unique())
    total_controls_to_fetch = len(control_names_to_fetch)
    demisto.debug(f"Will now fetch assets for {total_controls_to_fetch} unique control names, one by one.")

    base_payload_data = {
        "assessment_profile_revision": anchor['assessment_profile_revision'],
        "last_evaluation_time": anchor['last_evaluation_time'],
    }

    # --- Part 2: Serial Fetching ---
    all_assets = []
    processed_count = 0

    for control_name in control_names_to_fetch:
        processed_count += 1
        demisto.debug(f"Processing control [{processed_count}/{total_controls_to_fetch}]: '{control_name}'")

        try:
            payload = { "request_data": base_payload_data.copy() }
            payload["request_data"]["filters"] = [
                { "field": "control_name", "operator": "eq", "value": control_name },
                { "field": "status", "operator": "neq", "value": "Not Assessed" }
            ]

            # Use silent=True to suppress per-page pagination logs
            assets_for_this_control = get_all_paginated_assets(payload, silent=True)

            if not assets_for_this_control:
                 pass # Don't print "No assets" to reduce noise

            all_assets.extend(assets_for_this_control)

        except Exception as e:
            demisto.error(f"Failed to fetch assets for control '{control_name}': {e}. Skipping to the next control.")
            continue

        time.sleep(0.3)  # 300ms delay between fetching asset lists for each control

    demisto.info(f"All controls processed. Total assets retrieved: {len(all_assets)}")

    # --- Part 3: Processing and Pushing ---
    if not all_assets:
        demisto.info("No assets found for this assessment run.")
        return pd.DataFrame()

    demisto.debug(f"Processing and enriching {len(all_assets)} total asset records...")
    processed_results = []

    evaluation_datetime = pd.to_datetime(anchor['last_evaluation_time'], unit='ms')

    for asset in all_assets:
        asset_control_revision = str(asset.get("CONTROL_REVISION_ID"))
        control_data = controls_lookup.get(asset_control_revision, {})

        processed_results.append({
            "ASSESSMENT_PROFILE_NAME": profile_name,
            "ASSESSMENT_PROFILE_REVISION": anchor['assessment_profile_revision'],
            "STANDARD_NAME": standard_name,
            "ASSET_GROUP_ID": anchor.get('asset_group_id'),
            "EVALUATION_DATETIME": evaluation_datetime,
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
            "CONTROL_REVISION": asset_control_revision,
            "CONTROL_RULE_NAMES": json.dumps(control_data.get("rule_names", [])),
            "CATEGORY": control_data.get("category", ""),
            "SUBCATEGORY": control_data.get("subcategory", ""),
            "MITIGATION": control_data.get("mitigation", ""),
            "SCANNABLE_ASSET_TYPES": json.dumps(control_data.get("scannable_asset_types", []))
        })

    # Define the final column order for the report
    column_order = [
        "ASSESSMENT_PROFILE_NAME", "ASSESSMENT_PROFILE_REVISION","STANDARD_NAME", "ASSET_GROUP_ID", "EVALUATION_DATETIME",
        "ASSET_ID", "ASSET_NAME", "ASSET_TYPE", "STATUS", "SEVERITY",
        "CONTROL_ID", "CONTROL_NAME", "CONTROL_REVISION", "CONTROL_RULE_NAMES",
        "RULE_NAME", "RULE_REVISION_ID", "RULE_TYPE",
        "CATEGORY", "SUBCATEGORY",
        "SCANNABLE_ASSET_TYPES", "SOURCE", "PROVIDER", "REGION", "REALM", "TAGS",
        "ORGANIZATION", "ASSET_STRONG_ID"
    ]
    df_final = pd.DataFrame(processed_results, columns=column_order)

    for col in column_order:
        if col not in df_final.columns:
            df_final[col] = None

    demisto.info(f"Enrichment complete. Total records to push: {len(df_final)}.")

    push_records_to_collector(
        processed_results,
        COLLECTOR_URL,
        Compliance_API_KEY,
        label="Asset"
    )

    return df_final

def stage_3_create_control_summary_report(controls_df, df_unified, profile_name, standard_name, anchor_data):
    """
    Aggregates asset results to create a per-control summary report.
    """
    demisto.info("Stage 3: Creating Control-Based Summary Report")

    if df_unified.empty:
        demisto.debug("The asset report (df_unified) is empty. No summary can be generated.")
        stub_df = controls_df.copy()
        stub_df['STANDARD_NAME'] = standard_name
        stub_df['ASSESSMENT_PROFILE_NAME'] = profile_name
        stub_df['ASSESSMENT_PROFILE_REVISION']= anchor_data.get('assessment_profile_revision')
        stub_df['ASSET_GROUP_ID'] = anchor_data.get('asset_group_id')
        stub_df['EVALUATION_DATETIME'] = pd.to_datetime(anchor_data.get('last_evaluation_time'), unit='ms')
        stub_df['STATUS'] = 'NOT_EVALUATED'
        stub_df['SCORE'] = 0.0
        stub_df['RULES_COUNT'] = stub_df.get('rule_names', []).apply(len)

        stub_df = stub_df.rename(columns={
            'control_id': 'CONTROL_ID',
            'control_name': 'CONTROL_NAME',
            'severity': 'SEVERITY',
            'category': 'CATEGORY'
        })

        final_columns = [
            'STANDARD_NAME', 'ASSESSMENT_PROFILE_NAME','ASSESSMENT_PROFILE_REVISION','ASSET_GROUP_ID', 'EVALUATION_DATETIME',
            'STATUS', 'CONTROL_NAME', 'CONTROL_ID', 'SCORE','SEVERITY', 'CATEGORY', 'RULES_COUNT'
        ]
        for col in final_columns:
            if col not in stub_df.columns:
                stub_df[col] = None

        return stub_df[final_columns]

    demisto.debug("Using global values from loop variables.")
    global_standard_name = standard_name
    global_profile_name = profile_name
    global_asset_group_id = anchor_data.get('asset_group_id')
    global_eval_datetime = pd.to_datetime(anchor_data.get('last_evaluation_time'), unit='ms')



    demisto.debug("Aggregating results for 'OVERALL_STATUS' (Unique Asset Logic)...")
    status_priority = pd.CategoricalDtype(
        ['PASSED', 'NOT_ASSESSED', 'FAILED'],
        ordered=True
    )
    df_unified_copy = df_unified.copy()
    df_unified_copy['STATUS_PRIORITY'] = df_unified_copy['STATUS'].astype(status_priority)

    df_asset_worst_status = df_unified_copy.groupby(
        ['CONTROL_ID', 'CONTROL_NAME', 'ASSET_ID']
    )['STATUS_PRIORITY'].max().reset_index()

    df_agg = df_asset_worst_status.groupby(
        ['CONTROL_ID', 'CONTROL_NAME']
    )['STATUS_PRIORITY'].value_counts()

    df_results_agg = df_agg.unstack(level='STATUS_PRIORITY', fill_value=0).reset_index()

    demisto.debug("Merging aggregated results with the main controls catalog...")

    controls_df_renamed = controls_df.rename(columns={
        'control_id': 'CONTROL_ID',
        'control_name': 'CONTROL_NAME'
    })

    df_summary = pd.merge(
        controls_df_renamed,
        df_results_agg,
        on=['CONTROL_ID', 'CONTROL_NAME'],
        how='left'
    )

    status_cols = ['PASSED', 'FAILED', 'NOT_ASSESSED']
    for col in status_cols:
        if col not in df_summary.columns:
            df_summary[col] = 0
        df_summary[col] = df_summary[col].fillna(0).astype(int)

    df_summary['TOTAL_ASSETS_ASSESSED'] = df_summary[status_cols].sum(axis=1)

    conditions = [
        (df_summary['FAILED'] > 0),
        (df_summary['PASSED'] > 0),
        (df_summary['NOT_ASSESSED'] > 0),
        (df_summary['TOTAL_ASSETS_ASSESSED'] == 0)
    ]
    choices = [ 'FAILED', 'PASSED', 'NOT_ASSESSED', 'NOT_EVALUATED' ]
    df_summary['OVERALL_STATUS'] = np.select(conditions, choices, default='UNKNOWN')

    demisto.debug("Calculating per-control scores (Ignoring 'Not Assessed' assets)...")
    df_scoring = df_unified[df_unified['STATUS'].isin(['PASSED', 'FAILED'])].copy()

    if not df_scoring.empty:
        score_priority = pd.CategoricalDtype(['PASSED', 'FAILED'], ordered=True)
        df_scoring['SCORE_PRIORITY'] = df_scoring['STATUS'].astype(score_priority)

        df_asset_score_status = df_scoring.groupby(
            ['CONTROL_ID', 'ASSET_ID']
        )['SCORE_PRIORITY'].max().reset_index()

        df_score_agg = df_asset_score_status.groupby('CONTROL_ID')['SCORE_PRIORITY'].value_counts().unstack(level='SCORE_PRIORITY', fill_value=0)
        df_score_agg = df_score_agg.rename(columns={'PASSED': 'score_passed', 'FAILED': 'score_failed'}).reset_index()
        df_summary = pd.merge(df_summary, df_score_agg, on='CONTROL_ID', how='left')
        df_summary[['score_passed', 'score_failed']] = df_summary[['score_passed', 'score_failed']].fillna(0).astype(int)
    else:
        df_summary['score_passed'] = 0
        df_summary['score_failed'] = 0

    df_summary['total_for_score'] = df_summary['score_passed'] + df_summary['score_failed']
    df_summary['per_control_score'] = 0.0

    df_summary.loc[df_summary['total_for_score'] > 0, 'per_control_score'] = (
        df_summary['score_passed'] / df_summary['total_for_score']
    )

    if 'rule_names' in df_summary.columns:
        df_summary['num_rules'] = df_summary['rule_names'].apply(lambda x: len(x) if isinstance(x, (list, set)) else 0)
    else:
        df_summary['num_rules'] = 0

    df_summary['STANDARD_NAME'] = global_standard_name
    df_summary['ASSESSMENT_PROFILE_NAME'] = global_profile_name
    df_summary['ASSET_GROUP_ID'] = global_asset_group_id
    df_summary['EVALUATION_DATETIME'] = global_eval_datetime

    df_summary = df_summary.rename(columns={
        'OVERALL_STATUS': 'STATUS',
        'per_control_score': 'SCORE',
        'num_rules': 'RULES_COUNT',
        'category': 'CATEGORY',
        'severity': 'SEVERITY'
    })

    final_columns = [
        'STANDARD_NAME', 'ASSESSMENT_PROFILE_NAME','ASSESSMENT_PROFILE_REVISION','ASSET_GROUP_ID', 'EVALUATION_DATETIME',
        'STATUS', 'CONTROL_NAME', 'CONTROL_ID', 'SCORE','SEVERITY', 'CATEGORY', 'RULES_COUNT'
    ]

    for col in final_columns:
        if col not in df_summary.columns:
            df_summary[col] = None

    df_final_summary = df_summary[final_columns]

    demisto.info(f"Generated control summary report with {len(df_final_summary)} records.")

    return df_final_summary

# =================================================================================
# MAIN EXECUTION
# =================================================================================

def main():
    try:
        demisto.info("Script starting execution")
        args = demisto.args()

        # Parse arguments with semicolon separator
        profile_list = argToList(args.get('profiles'), separator=';', transform=str.strip)
        standard_list = argToList(args.get('standards'), separator=';', transform=str.strip)

        if not profile_list or not standard_list:
            raise DemistoException("Input for 'profiles' and 'standards' cannot be empty.")

        if len(profile_list) != len(standard_list):
            raise DemistoException("Error: The number of profiles must match the number of standards.")

        assessment_pairs = list(zip(profile_list, standard_list))
        demisto.info(f"Found {len(assessment_pairs)} (Profile, Standard) pairs to process.")

        # --- Lists to hold all DataFrames for final CSV export ---
        all_asset_dfs = []
        all_control_summary_dfs = []

        # --- Aggregators for the final summary ---
        total_passed_all = 0
        total_failed_all = 0
        total_not_assessed_all = 0
        total_not_evaluated_all = 0
        total_controls_all = 0
        all_scores = []
        all_pair_summaries = []

        for profile, standard in assessment_pairs:
            pair_summary_text = ""
            try:
                demisto.info(f"Processing pair: ('{profile}', '{standard}')")

                pair_summary_text += f"### Summary for: '{profile}' / '{standard}'\n\n"

                # Stage 0: Get the assessment anchor
                anchor = stage_0_get_assessment_anchor(profile, standard)

                # Stage 1: Get the controls catalog
                controls_df = stage_1_get_controls_catalog(standard)

                # Stage 2: Fetch, enrich, and push asset data.
                asset_df = stage_2_process_assets_and_push(anchor, controls_df, profile, standard)

                if not asset_df.empty:
                    all_asset_dfs.append(asset_df)

                # Stage 3: Create and push the new Control Summary report
                demisto.debug(f"Starting Stage 3: Create/Push Control Summary for ('{profile}', '{standard}')")

                control_summary_df = stage_3_create_control_summary_report(
                    controls_df,
                    asset_df,
                    profile,
                    standard,
                    anchor
                )

                if not control_summary_df.empty:
                    all_control_summary_dfs.append(control_summary_df)

                    demisto.debug(f"Generated {len(control_summary_df)} summary records.")
                    summary_records = control_summary_df.to_dict('records')

                    push_records_to_collector(
                        summary_records,
                        COLLECTOR_URL,
                        Controls_API_KEY,
                        label="Summary"
                    )

                    # --- Calculate stats for this pair and add to totals ---
                    final_score = 0.0
                    total_controls = len(control_summary_df)

                    # Filter for scorable controls (PASSED or FAILED)
                    evaluated_controls_df = control_summary_df[
                        control_summary_df['STATUS'].isin(['PASSED', 'FAILED'])
                    ]

                    evaluated_controls_count = len(evaluated_controls_df)

                    if evaluated_controls_count > 0:
                        final_score = evaluated_controls_df['SCORE'].mean()
                        all_scores.append(final_score)

                    status_counts = control_summary_df['STATUS'].value_counts().to_dict()
                    passed_count = status_counts.get('PASSED', 0)
                    failed_count = status_counts.get('FAILED', 0)
                    not_assessed_count = status_counts.get('NOT_ASSESSED', 0)
                    not_evaluated_count = status_counts.get('NOT_EVALUATED', 0)

                    # Add to global counters
                    total_passed_all += passed_count
                    total_failed_all += failed_count
                    total_not_assessed_all += not_assessed_count
                    total_not_evaluated_all += not_evaluated_count
                    total_controls_all += total_controls

                    # Build the markdown table for this pair
                    pair_summary_text += f"**Compliance Score: {final_score:.2%}**\n\n"
                    pair_summary_text += "| Control Status | Count |\n"
                    pair_summary_text += "| :--- | :--- |\n"
                    pair_summary_text += f"| Passed | {passed_count} |\n"
                    pair_summary_text += f"| Failed | {failed_count} |\n"
                    pair_summary_text += f"| Not Assessed | {not_assessed_count} |\n"
                    pair_summary_text += f"| Not Evaluated | {not_evaluated_count} |\n"
                    pair_summary_text += f"| **Total Controls** | **{total_controls}** |\n"

                    all_pair_summaries.append(pair_summary_text)

                else:
                    demisto.debug("Control summary report is empty. Nothing to push.")
                    pair_summary_text += "No summary generated (No controls found)."
                    all_pair_summaries.append(pair_summary_text)

            except Exception as e:
                demisto.error(f"Failed to process pair ('{profile}', '{standard}'): {e}")
                pair_summary_text += f"**Error:**\n`{e}`\n"
                all_pair_summaries.append(pair_summary_text)
                continue

        # --- Build the final summary ---
        demisto.info("Script execution finished")

        # Calculate overall average score
        overall_average_score = 0.0
        if all_scores:
            overall_average_score = sum(all_scores) / len(all_scores)

        # Build readable output for War Room
        readable_output = "# Compliance Data Fetcher Results\n\n"
        readable_output += "## Per-Pair Summaries\n\n"
        readable_output += "\n---\n".join(all_pair_summaries)
        readable_output += "\n\n---\n\n"
        readable_output += "## Overall Control Summary (All Pairs)\n\n"
        readable_output += "| Status | Count |\n"
        readable_output += "| :--- | :--- |\n"
        readable_output += f"| Passed | {total_passed_all} |\n"
        readable_output += f"| Failed | {total_failed_all} |\n"
        readable_output += f"| Not Assessed | {total_not_assessed_all} |\n"
        readable_output += f"| Not Evaluated | {total_not_evaluated_all} |\n"
        readable_output += f"| **Total Controls** | **{total_controls_all}** |\n\n"
        readable_output += "## Overall Score Summary\n\n"
        readable_output += f"- Number of Scored Assessments: {len(all_scores)}\n"
        readable_output += f"- **Overall Average Score:** {overall_average_score:.2%}\n"

        # --- Prepare file results for CSV export ---
        file_results = []
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Save Asset Report using fileResult
        if all_asset_dfs:
            final_asset_df = pd.concat(all_asset_dfs, ignore_index=True)
            asset_csv_content = final_asset_df.to_csv(index=False, encoding='utf-8')
            asset_filename = f"compliance_asset_report_{timestamp}.csv"
            file_results.append(fileResult(asset_filename, asset_csv_content, EntryType.ENTRY_INFO_FILE))
            demisto.info(f"Created asset report file: {asset_filename}")
        else:
            demisto.debug("No asset data was generated; asset report CSV not created.")

        # Save Control Summary Report using fileResult
        if all_control_summary_dfs:
            final_control_summary_df = pd.concat(all_control_summary_dfs, ignore_index=True)
            control_csv_content = final_control_summary_df.to_csv(index=False, encoding='utf-8')
            control_filename = f"compliance_control_summary_{timestamp}.csv"
            file_results.append(fileResult(control_filename, control_csv_content, EntryType.ENTRY_INFO_FILE))
            demisto.info(f"Created control summary file: {control_filename}")
        else:
            demisto.debug("No control summary data was generated; control summary CSV not created.")

        # Build output context data
        output_data = {
            "TotalPassed": total_passed_all,
            "TotalFailed": total_failed_all,
            "TotalNotAssessed": total_not_assessed_all,
            "TotalNotEvaluated": total_not_evaluated_all,
            "TotalControls": total_controls_all,
            "OverallAverageScore": round(overall_average_score, 4),
            "ScoredAssessmentsCount": len(all_scores),
            "DryRun": DRY_RUN,
            "ProcessedPairs": len(assessment_pairs)
        }

        # Return results to War Room
        command_result = CommandResults(
            outputs_prefix="ComplianceDataFetcher",
            outputs_key_field="ProcessedPairs",
            outputs=output_data,
            readable_output=readable_output
        )

        # Return command results and file results
        return_results([command_result] + file_results)

    except Exception as e:
        demisto.error(f"A critical error occurred: {e}")
        return_error(f"A critical error occurred: {e}")

if __name__ in ('__main__', '__builtin__', 'builtins'):
    main()
