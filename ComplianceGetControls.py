"""ComplianceGetControls - Cortex Cloud Script

Stage 1: Fetches all compliance controls for given standard(s).

Automatically deduplicates to avoid fetching the same standard twice.

Prerequisites:
    - ComplianceLoadConfig must be run first to load configuration to context.

Arguments:
    assessments (str): Optional. JSON array of assessments grouped by standard, or
                       an XSOAR List name containing the JSON.
                       If not provided, reads from ComplianceAssessments context
                       (set by ComplianceLoadConfig).
                       Each item has:
                       - standard: The compliance standard name
                       - profiles: Array of profile names (used for context, not for fetching)

Outputs:
    Context path: ComplianceControlsCatalog (dict keyed by standard_name)
        Each entry contains:
        - standard_name: The standard name
        - total_controls: Number of controls fetched
        - controls: List of control objects with details

Example JSON (assessments argument):
    [
        {
            "standard": "CIS Amazon Web Services Foundations Benchmark v3.0.0 Level 1",
            "profiles": ["Production", "Development"]
        },
        {
            "standard": "NIST 800-53 Rev. 5",
            "profiles": ["Production"]
        }
    ]

    This will fetch controls for CIS AWS and NIST 800-53 (unique standards only).
"""

# from CommonServerPython import *
import json
import time

# =============================================================================
# CONFIGURATION
# =============================================================================

BATCH_SIZE = 50  # Number of controls to fetch before a small delay
BATCH_DELAY = 0.3  # Delay between batches (seconds) - reduced from 0.2 per control
SAMPLE_SIZE = 5  # Number of controls to sample for time estimation
TIMEOUT_BUFFER = 1.5  # Multiplier for recommended timeout (50% buffer)


# =============================================================================
# API HELPERS
# =============================================================================

def parse_api_response(result, operation):
    """
    Parse the response from a core-api-post command.
    """
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


# =============================================================================
# CONTROL FETCHING
# =============================================================================

def estimate_time_and_log(control_ids, standard_name):
    """
    Sample first few controls to estimate total processing time.
    Returns (avg_time_per_control, sampled_results, estimate_info).
    """
    total = len(control_ids)
    sample_count = min(SAMPLE_SIZE, total)

    if sample_count == 0:
        return 0, [], {}

    sample_times = []
    sample_results = []

    for i in range(sample_count):
        start = time.time()
        result = fetch_single_control_details(control_ids[i])
        elapsed = time.time() - start
        sample_times.append(elapsed)
        if result:
            sample_results.append(result)

    avg_time = sum(sample_times) / len(sample_times)

    # Calculate estimates
    remaining = total - sample_count
    num_batches = remaining // BATCH_SIZE
    estimated_api_time = remaining * avg_time
    estimated_batch_delays = num_batches * BATCH_DELAY
    estimated_total = estimated_api_time + estimated_batch_delays

    # Format time nicely
    if estimated_total < 60:
        time_str = f"{estimated_total:.0f} seconds"
    else:
        minutes = int(estimated_total // 60)
        seconds = int(estimated_total % 60)
        time_str = f"{minutes} min {seconds} sec"

    recommended_timeout = int((estimated_total + (sample_count * avg_time)) * TIMEOUT_BUFFER)

    estimate_info = {
        "total_controls": total,
        "avg_api_time": round(avg_time, 2),
        "estimated_time": time_str,
        "recommended_timeout": recommended_timeout
    }

    return avg_time, sample_results, estimate_info


def fetch_single_control_details(cid):
    """
    Fetch details for a single control by ID.
    
    Returns MINIMAL data to reduce context size:
    - revision: Used as lookup key in ComplianceFetchAssets
    - control_id: For output enrichment
    - control_name: For API filter and summary output
    - severity: For output enrichment
    - category: For output enrichment
    - subcategory: For output enrichment
    - rule_count: Number of rules (for quick filtering)
    - rule_names: Full rule names list (for output enrichment)
    
    Removed to save space:
    - scannable_asset_types: Not used in enrichment
    """
    severity_map = {
        "SEV_050_CRITICAL": "Critical",
        "SEV_040_HIGH": "High",
        "SEV_030_MEDIUM": "Medium",
        "SEV_020_LOW": "Low",
        "SEV_010_INFO": "Informational"
    }

    try:
        response = post_to_api(
            "/public_api/v1/compliance/get_control",
            {"request_data": {"id": cid}}
        )

        ctrl = (response.get("reply") or {}).get("control", [None])[0]
        if not ctrl or ctrl.get('STATUS') != 'ACTIVE':
            return None

        rules = ctrl.get("COMPLIANCE_RULES") or []
        rule_names = sorted({str(r["NAME"]) for r in rules if "NAME" in r})
        scannable_asset_types = sorted({str(at) for r in rules for at in r.get("SCANNABLE_ASSETS", [])})

        # OPTIMIZED: Store only essential fields for enrichment
        return {
            "revision": str(ctrl.get("REVISION")),  # Lookup key
            "control_id": ctrl.get("CONTROL_ID", cid),
            "control_name": ctrl.get("CONTROL_NAME", ""),  # Needed for API filter & summary
            "severity": severity_map.get(ctrl.get("SEVERITY"), "Unknown"),
            "category": ctrl.get("CATEGORY", ""),
            "subcategory": ctrl.get("SUBCATEGORY", ""),
            "rule_count": len(rule_names),  # For quick filtering
            "rule_names": rule_names,  # Full list for output enrichment
            "scannable_asset_types": scannable_asset_types,  # Asset types this control scans
        }

    except Exception as e:
        demisto.debug(f"Error fetching control ID {cid}: {e}")
        return None


def get_controls_catalog(standard_name):
    """
    Fetch all controls for a compliance standard using batch processing.
    Returns (controls_list, estimate_info).
    """
    # Get control IDs from standard
    body = {
        "request_data": {
            "filters": [{"field": "name", "operator": "eq", "value": standard_name}]
        }
    }
    response = post_to_api("/public_api/v1/compliance/get_standards", body)

    try:
        standards = response.get("reply", {}).get("standards", [])
        if not standards:
            raise DemistoException(f"Standard '{standard_name}' not found.")
        control_ids = list(set(standards[0].get("controls_ids", [])))
    except (KeyError, IndexError) as e:
        raise DemistoException(f"Could not retrieve control IDs for standard '{standard_name}': {e}")

    if not control_ids:
        return [], {}

    total = len(control_ids)

    # Sample first few controls and estimate time
    avg_time, sampled_results, estimate_info = estimate_time_and_log(control_ids, standard_name)
    controls = sampled_results.copy()
    sample_count = min(SAMPLE_SIZE, total)

    # Fetch remaining controls (skip already sampled ones)
    remaining_ids = control_ids[sample_count:]

    if remaining_ids:
        for i, cid in enumerate(remaining_ids):
            result = fetch_single_control_details(cid)
            if result:
                controls.append(result)

            # Batch delay
            if (i + 1) % BATCH_SIZE == 0:
                time.sleep(BATCH_DELAY)

    return controls, estimate_info


# =============================================================================
# INPUT PARSING
# =============================================================================

def parse_standards_input(args):
    """
    Parse the assessments input - checks context first, then argument.

    Returns list of unique standard names.
    """
    assessments = None
    assessments_input = args.get('assessments', '')

    # First, check if assessments argument was provided
    if assessments_input:
        # Check if already a dict or list (passed from context/other script)
        if isinstance(assessments_input, list):
            assessments = assessments_input
            demisto.debug("Using assessments from argument (list)")
        elif isinstance(assessments_input, dict):
            # Single assessment object - wrap in list
            assessments = [assessments_input]
            demisto.debug("Using assessments from argument (single dict)")
        elif isinstance(assessments_input, str):
            try:
                # Try to parse as JSON directly
                parsed = json.loads(assessments_input)
                assessments = parsed if isinstance(parsed, list) else [parsed]
                demisto.debug("Using assessments from argument (JSON string)")
            except json.JSONDecodeError:
                # Maybe it's a reference to an XSOAR List
                demisto.debug(f"Trying to load assessments from XSOAR List: {assessments_input}")
                res = demisto.executeCommand("getList", {"listName": assessments_input})
                if is_error(res) or not res[0].get('Contents'):
                    raise DemistoException(
                        f"Invalid assessments input. Expected JSON array or valid XSOAR List name. "
                        f"Got: {assessments_input[:100]}..."
                    )
                try:
                    parsed = json.loads(res[0]['Contents'])
                    assessments = parsed if isinstance(parsed, list) else [parsed]
                    demisto.debug("Using assessments from argument (XSOAR List)")
                except json.JSONDecodeError:
                    raise DemistoException(
                        f"XSOAR List '{assessments_input}' does not contain valid JSON."
                    )
        else:
            raise DemistoException(f"Invalid assessments type: {type(assessments_input)}")
    else:
        # Check context for assessments (set by ComplianceLoadConfig)
        assessments = demisto.context().get("ComplianceAssessments")
        if assessments:
            # Ensure it's a list
            if isinstance(assessments, dict):
                assessments = [assessments]
            demisto.debug("Using assessments from context (ComplianceAssessments)")
        else:
            raise DemistoException(
                "No assessments provided. Either pass 'assessments' argument or "
                "include ASSESSMENTS in config and run ComplianceLoadConfig first."
            )

    # Extract unique standards from JSON
    standards = set()
    for item in assessments:
        standard = item.get('standard')
        if standard:
            standards.add(standard.strip())

    if not standards:
        raise DemistoException("No valid standards found in assessments input.")

    return list(standards)


# =============================================================================
# MAIN FUNCTION
# =============================================================================

def main():
    try:
        args = demisto.args()

        # Parse input (JSON or legacy format)
        standard_list = parse_standards_input(args)

        if not standard_list:
            raise DemistoException("No standards found in input.")

        # Fetch controls for each unique standard
        controls_catalog = {}
        estimates = {}
        errors = []
        total_controls = 0

        for standard_name in standard_list:
            try:
                controls, estimate_info = get_controls_catalog(standard_name)
                controls_catalog[standard_name] = {
                    "standard_name": standard_name,
                    "total_controls": len(controls),
                    "controls": controls
                }
                if estimate_info:
                    estimates[standard_name] = estimate_info
                total_controls += len(controls)
            except Exception as e:
                error_msg = f"{standard_name}: {str(e)}"
                errors.append(error_msg)

        if not controls_catalog:
            raise DemistoException(f"Failed to fetch controls for any standard. Errors: {'; '.join(errors)}")

        # Build readable output
        readable = f"## Compliance Controls Catalog\n\n"
        readable += f"**Standards Processed:** {len(controls_catalog)}\n"
        readable += f"**Total Controls:** {total_controls}\n\n"

        # Show time estimates (useful if script times out)
        if estimates:
            readable += "### Time Estimates\n\n"
            readable += "| Standard | Controls | Avg API Time | Est. Time | Recommended Timeout |\n"
            readable += "| :--- | :--- | :--- | :--- | :--- |\n"
            for std_name, est in estimates.items():
                # Truncate long standard names for table
                display_name = std_name[:40] + "..." if len(std_name) > 40 else std_name
                readable += f"| {display_name} | {est.get('total_controls', 0)} | {est.get('avg_api_time', 0)}s | {est.get('estimated_time', 'N/A')} | {est.get('recommended_timeout', 0)}s |\n"
            readable += "\n"

        for standard_name, data in controls_catalog.items():
            controls = data.get('controls', [])
            readable += f"### {standard_name}\n\n"
            readable += f"**Active Controls:** {len(controls)}\n\n"

            if controls:
                # Summary by severity
                severity_counts = {}
                for ctrl in controls:
                    sev = ctrl.get('severity', 'Unknown')
                    severity_counts[sev] = severity_counts.get(sev, 0) + 1

                readable += "| Severity | Count |\n"
                readable += "| :--- | :--- |\n"
                for sev in ['Critical', 'High', 'Medium', 'Low', 'Informational', 'Unknown']:
                    if sev in severity_counts:
                        readable += f"| {sev} | {severity_counts[sev]} |\n"
                readable += "\n"

        if errors:
            readable += "### Errors\n\n"
            for error in errors:
                readable += f"- {error}\n"

        # PHASE 1 FIX: Use setContext directly to REPLACE (not append) the catalog
        # CommandResults with outputs_prefix appends to a list, causing duplicates
        demisto.setContext("ComplianceControlsCatalog", controls_catalog)
        
        # Return readable output only (no outputs to avoid duplication)
        return_results(CommandResults(
            readable_output=readable
        ))

    except Exception as e:
        demisto.error(f"Failed to get controls catalog: {e}")
        return_error(f"ComplianceGetControls failed: {e}")


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ in ('__main__', '__builtin__', 'builtins'):
    main()
