"""ComplianceOrchestrator - Cortex XSOAR Script

Main orchestrator that runs all compliance stages for multiple profile/standard pairs.
This script calls each stage script in sequence, passing all pairs at once.

Each stage script handles multiple values internally:
- ComplianceGetAnchor: Finds anchors for all profile/standard pairs
- ComplianceGetControls: Fetches controls for all unique standards
- ComplianceFetchAssets: Fetches and enriches assets for all pairs
- ComplianceCreateSummary: Creates summaries for all pairs

Arguments:
    assessments (str): Optional. JSON array of assessments grouped by standard, or
                       an XSOAR List name containing the JSON.
                       If not provided, reads from ComplianceAssessments context
                       (set by ComplianceLoadConfig from ASSESSMENTS in config).
                       Each item has:
                       - standard: The compliance standard name
                       - profiles: Array of profile names to assess against this standard

    push_to_collector (bool): Optional. Whether to push data to HTTP Collector.
                              Defaults to True (respects DRY_RUN config).

Outputs:
    Context path: ComplianceResults
        - total_pairs: Number of profile/standard pairs processed
        - overall_score: Average compliance score across all pairs
        - summary_results: Per-pair summary results from ComplianceCreateSummary

Example JSON (assessments argument):
    [
        {
            "standard": "CIS Amazon Web Services Foundations Benchmark v3.0.0 Level 1",
            "profiles": ["Production", "Development", "Staging"]
        },
        {
            "standard": "NIST 800-53 Rev. 5",
            "profiles": ["Production", "Compliance"]
        }
    ]

    This will process:
    - (Production, CIS AWS...)
    - (Development, CIS AWS...)
    - (Staging, CIS AWS...)
    - (Production, NIST 800-53)
    - (Compliance, NIST 800-53)

To use an XSOAR List, create a list with the JSON above and pass the list name:
    assessments: "My_Compliance_Assessments"
"""

# from CommonServerPython import *
import json
from datetime import datetime


# =============================================================================
# STAGE EXECUTION HELPERS
# =============================================================================

def run_load_config():
    """Run ComplianceLoadConfig script."""
    demisto.info("Running: ComplianceLoadConfig")
    result = demisto.executeCommand("ComplianceLoadConfig", {})

    if is_error(result):
        raise DemistoException(f"ComplianceLoadConfig failed: {get_error(result)}")

    demisto.debug("ComplianceLoadConfig completed successfully")
    return result


def run_get_anchors(assessments_json=None):
    """Run ComplianceGetAnchor script for all profile/standard pairs."""
    demisto.info("Running: ComplianceGetAnchor for all pairs")

    # If assessments_json is None, sub-script will read from context
    args = {"assessments": assessments_json} if assessments_json else {}
    result = demisto.executeCommand("ComplianceGetAnchor", args)

    if is_error(result):
        raise DemistoException(f"ComplianceGetAnchor failed: {get_error(result)}")

    demisto.debug("ComplianceGetAnchor completed successfully")
    return result


def run_get_controls(assessments_json=None):
    """Run ComplianceGetControls script for all standards."""
    demisto.info("Running: ComplianceGetControls for all standards")

    # If assessments_json is None, sub-script will read from context
    args = {"assessments": assessments_json} if assessments_json else {}
    result = demisto.executeCommand("ComplianceGetControls", args)

    if is_error(result):
        raise DemistoException(f"ComplianceGetControls failed: {get_error(result)}")

    demisto.debug("ComplianceGetControls completed successfully")
    return result


def run_fetch_assets(push_enabled):
    """Run ComplianceFetchAssets script for all pairs."""
    demisto.info("Running: ComplianceFetchAssets for all pairs")
    result = demisto.executeCommand("ComplianceFetchAssets", {
        "push_to_collector": str(push_enabled).lower()
    })

    if is_error(result):
        raise DemistoException(f"ComplianceFetchAssets failed: {get_error(result)}")

    demisto.debug("ComplianceFetchAssets completed successfully")
    return result


def run_create_summary(push_enabled):
    """Run ComplianceCreateSummary script for all pairs."""
    demisto.info("Running: ComplianceCreateSummary for all pairs")
    result = demisto.executeCommand("ComplianceCreateSummary", {
        "push_to_collector": str(push_enabled).lower()
    })

    if is_error(result):
        raise DemistoException(f"ComplianceCreateSummary failed: {get_error(result)}")

    demisto.debug("ComplianceCreateSummary completed successfully")
    return result


# =============================================================================
# INPUT PARSING
# =============================================================================

def parse_assessments_input(args):
    """
    Parse the assessments input - checks argument first, then context.

    Returns:
        - assessments_json: JSON string to pass to sub-scripts (or None to use context)
        - total_pairs: Count of profile/standard pairs
    """
    assessments_input = args.get('assessments', '')

    # First, check if assessments argument was provided
    if assessments_input:
        # Check if already a dict or list (passed from context/other script)
        if isinstance(assessments_input, list):
            assessments = assessments_input
            demisto.debug("Using assessments from argument (list)")
            total_pairs = sum(len(item.get('profiles', [])) for item in assessments)
            # Return None - data is already in a usable format, sub-scripts will get from context
            return None, total_pairs
        elif isinstance(assessments_input, dict):
            # Single assessment object - wrap in list
            assessments = [assessments_input]
            demisto.debug("Using assessments from argument (single dict)")
            total_pairs = sum(len(item.get('profiles', [])) for item in assessments)
            return None, total_pairs
        elif isinstance(assessments_input, str):
            try:
                # Try to parse as JSON directly
                parsed = json.loads(assessments_input)
                assessments = parsed if isinstance(parsed, list) else [parsed]
                total_pairs = sum(len(item.get('profiles', [])) for item in assessments)
                demisto.debug("Using assessments from argument (JSON string)")
                return assessments_input, total_pairs
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
                    total_pairs = sum(len(item.get('profiles', [])) for item in assessments)
                    demisto.debug("Using assessments from argument (XSOAR List)")
                    return assessments_input, total_pairs
                except json.JSONDecodeError:
                    raise DemistoException(
                        f"XSOAR List '{assessments_input}' does not contain valid JSON."
                    )
        else:
            raise DemistoException(f"Invalid assessments type: {type(assessments_input)}")

    # Check context for assessments (set by ComplianceLoadConfig)
    assessments = demisto.context().get("ComplianceAssessments")
    if assessments:
        # Ensure it's a list
        if isinstance(assessments, dict):
            assessments = [assessments]
        total_pairs = sum(len(item.get('profiles', [])) for item in assessments)
        demisto.debug("Using assessments from context (ComplianceAssessments)")
        # Return None to signal sub-scripts should use context
        return None, total_pairs

    raise DemistoException(
        "No assessments provided. Either pass 'assessments' argument or "
        "include ASSESSMENTS in config."
    )


# =============================================================================
# MAIN FUNCTION
# =============================================================================

def main():
    try:
        args = demisto.args()
        push_enabled = argToBoolean(args.get('push_to_collector', 'true'))

        # Step 1: Load configuration first (populates ComplianceAssessments context)
        run_load_config()

        # Parse input - checks argument first, then context (populated by run_load_config)
        assessments_json, total_pairs = parse_assessments_input(args)

        demisto.info(f"Starting orchestration for {total_pairs} profile/standard pair(s)")

        # Step 2: Get all anchors (passes assessments JSON)
        run_get_anchors(assessments_json)

        # Step 3: Get all controls (passes assessments JSON)
        run_get_controls(assessments_json)

        # Step 4: Fetch and push assets for all pairs
        run_fetch_assets(push_enabled)

        # Step 5: Create and push summaries for all pairs
        run_create_summary(push_enabled)

        # Get results from context
        summary_results = demisto.context().get("ComplianceSummaryResults", [])
        assets_results = demisto.context().get("ComplianceAssetsResults", [])

        # Ensure it's a list
        if isinstance(summary_results, dict):
            summary_results = [summary_results]
        if isinstance(assets_results, dict):
            assets_results = [assets_results]

        # Build assets lookup for enriching summary
        assets_lookup = {}
        for asset_result in assets_results:
            key = f"{asset_result.get('profile_name')}|{asset_result.get('standard_name')}"
            assets_lookup[key] = asset_result.get('total_assets', 0)

        # Calculate overall statistics
        all_scores = []
        successful_pairs = 0

        for result in summary_results:
            score = result.get('compliance_score')
            if score is not None and score > 0:
                all_scores.append(score)
            if result.get('total_controls', 0) > 0:
                successful_pairs += 1

        overall_score = sum(all_scores) / len(all_scores) if all_scores else 0.0

        # Build output
        output_data = {
            "total_pairs": total_pairs,
            "successful_pairs": successful_pairs,
            "overall_score": round(overall_score, 4),
            "summary_results": summary_results
        }

        # Build readable output
        readable = "# Compliance Data Fetcher Results\n\n"
        readable += f"**Total Pairs Processed:** {total_pairs}\n"
        readable += f"**Successful:** {successful_pairs}\n"
        readable += f"**Overall Compliance Score:** {overall_score:.2%}\n\n"

        readable += "## Per-Pair Results\n\n"

        for result in summary_results:
            profile = result.get('profile_name', 'Unknown')
            standard = result.get('standard_name', 'Unknown')
            key = f"{profile}|{standard}"
            total_assets = assets_lookup.get(key, 0)

            readable += f"### {profile} / {standard}\n\n"
            readable += f"| Metric | Value |\n"
            readable += f"| :--- | :--- |\n"
            readable += f"| Compliance Score | {result.get('compliance_score', 0):.2%} |\n"
            readable += f"| Total Controls | {result.get('total_controls', 0)} |\n"
            readable += f"| Passed | {result.get('passed_count', 0)} |\n"
            readable += f"| Failed | {result.get('failed_count', 0)} |\n"
            readable += f"| Not Assessed | {result.get('not_assessed_count', 0)} |\n"
            readable += f"| Not Evaluated | {result.get('not_evaluated_count', 0)} |\n"
            readable += f"| Total Assets | {total_assets} |\n"
            readable += f"| Pushed to Collector | {result.get('pushed_count', 0)} |\n"
            readable += "\n---\n\n"

        demisto.info(f"Orchestration complete. Processed {total_pairs} pair(s).")

        return_results(CommandResults(
            outputs_prefix="ComplianceResults",
            outputs_key_field="total_pairs",
            outputs=output_data,
            readable_output=readable
        ))

    except Exception as e:
        demisto.error(f"Orchestrator failed: {e}")
        return_error(f"ComplianceOrchestrator failed: {e}")


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ in ('__main__', '__builtin__', 'builtins'):
    main()
