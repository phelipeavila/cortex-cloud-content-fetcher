"""ComplianceGetAnchor - Cortex XSOAR Script

Stage 0: Finds the latest assessment run anchor for given profile/standard pairs.

Prerequisites:
    - ComplianceLoadConfig must be run first to load configuration to context.

Arguments:
    assessments (str): Optional. JSON array of assessments grouped by standard, or
                       an XSOAR List name containing the JSON.
                       If not provided, reads from ComplianceAssessments context
                       (set by ComplianceLoadConfig).
                       Each item has:
                       - standard: The compliance standard name
                       - profiles: Array of profile names to assess against this standard

Outputs:
    Context path: ComplianceAnchors (list of anchor objects)
        Each anchor contains:
        - assessment_profile_revision: The profile revision ID
        - last_evaluation_time: Timestamp of last evaluation (ms)
        - asset_group_id: The asset group ID
        - profile_name: The profile name (for reference)
        - standard_name: The standard name (for reference)

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

    This will find anchors for:
    - (Production, CIS AWS...)
    - (Development, CIS AWS...)
    - (Staging, CIS AWS...)
    - (Production, NIST 800-53)
    - (Compliance, NIST 800-53)
"""

# from CommonServerPython import *
import json


# =============================================================================
# API HELPERS
# =============================================================================

def parse_api_response(result, operation):
    """
    Parse the response from a core-api-post command.

    Handles edge cases where the API may return error entries
    followed by the actual valid response.
    """
    if not result:
        raise DemistoException(f"{operation}: Empty response from API")

    for entry in result:
        if not isinstance(entry, dict):
            continue

        # Skip error entries (Type 4 = error)
        if entry.get("Type") == 4:
            continue

        contents = entry.get("Contents", {})
        if not isinstance(contents, dict):
            continue

        response = contents.get("response", {})
        if response and "reply" in response:
            return response

    # No valid response found
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
# MAIN LOGIC
# =============================================================================

def get_assessment_anchor(profile_name, standard_name):
    """Finds the latest assessment run anchor for the given profile and standard."""
    demisto.info(f"Finding assessment anchor for profile='{profile_name}', standard='{standard_name}'")

    request_body = {"request_data": {"filters": []}}
    response = post_to_api("/public_api/v1/compliance/get_assessment_results", request_body)

    data = (response.get("reply") or {}).get("data") or []
    if not data:
        raise DemistoException("No assessment results returned from the API.")

    # Filter for matching profile and standard
    matches = [
        d for d in data
        if (str(d.get("TYPE", "")).lower() == "profile" and
            str(d.get("ASSESSMENT_PROFILE", "")).strip() == profile_name.strip() and
            str(d.get("STANDARD_NAME", "")).strip() == standard_name.strip())
    ]

    if not matches:
        raise DemistoException(
            f"No assessment run found for Profile '{profile_name}' and Standard '{standard_name}'."
        )

    # Get the latest by evaluation time
    latest = sorted(matches, key=lambda x: int(x.get("LAST_EVALUATION_TIME", 0)), reverse=True)[0]

    anchor = {
        "assessment_profile_revision": latest.get("ASSESSMENT_PROFILE_REVISION"),
        "last_evaluation_time": latest.get("LAST_EVALUATION_TIME"),
        "asset_group_id": latest.get("ASSET_GROUP_ID"),
        "profile_name": profile_name,
        "standard_name": standard_name
    }

    if not all([anchor["assessment_profile_revision"], anchor["last_evaluation_time"]]):
        raise DemistoException("Found matching assessment run, but it's missing required data.")

    demisto.debug(f"Found anchor: {anchor}")
    return anchor


# =============================================================================
# INPUT PARSING
# =============================================================================

def parse_assessments_input(args):
    """
    Parse the assessments input - checks context first, then argument.

    Returns list of (profile_name, standard_name) tuples.
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

    # Expand JSON to pairs
    pairs = []
    for item in assessments:
        standard = item.get('standard')
        profiles = item.get('profiles', [])

        if not standard:
            raise DemistoException("Each assessment item must have a 'standard' field.")
        if not profiles:
            raise DemistoException(f"Standard '{standard}' has no profiles defined.")

        # Ensure profiles is a list
        if isinstance(profiles, str):
            profiles = [profiles]

        for profile in profiles:
            pairs.append((profile.strip(), standard.strip()))

    if not pairs:
        raise DemistoException("No valid profile/standard pairs found in assessments input.")

    return pairs


# =============================================================================
# MAIN FUNCTION
# =============================================================================

def main():
    try:
        args = demisto.args()

        # Parse input (JSON or legacy format)
        assessment_pairs = parse_assessments_input(args)
        demisto.info(f"Finding anchors for {len(assessment_pairs)} profile/standard pair(s)")

        # Find anchor for each pair
        anchors = []
        errors = []

        for profile_name, standard_name in assessment_pairs:
            try:
                anchor = get_assessment_anchor(profile_name, standard_name)
                anchors.append(anchor)
                demisto.info(f"Found anchor for {profile_name}/{standard_name}")
            except Exception as e:
                error_msg = f"{profile_name}/{standard_name}: {str(e)}"
                errors.append(error_msg)
                demisto.error(f"Failed to get anchor: {error_msg}")

        if not anchors:
            raise DemistoException(f"Failed to find any anchors. Errors: {'; '.join(errors)}")

        # Build readable output
        readable = f"## Assessment Anchors Found\n\n"
        readable += f"**Total Pairs:** {len(assessment_pairs)}\n"
        readable += f"**Successful:** {len(anchors)}\n"
        readable += f"**Failed:** {len(errors)}\n\n"

        for anchor in anchors:
            eval_time = anchor.get('last_evaluation_time')
            eval_datetime = timestamp_to_datestring(eval_time) if eval_time else "Unknown"

            readable += f"### {anchor.get('profile_name')} / {anchor.get('standard_name')}\n\n"
            readable += f"| Property | Value |\n"
            readable += f"| :--- | :--- |\n"
            readable += f"| Profile Revision | `{anchor.get('assessment_profile_revision')}` |\n"
            readable += f"| Asset Group ID | `{anchor.get('asset_group_id')}` |\n"
            readable += f"| Last Evaluation | {eval_datetime} |\n\n"

        if errors:
            readable += "### Errors\n\n"
            for error in errors:
                readable += f"- {error}\n"

        demisto.info(f"Successfully found {len(anchors)} anchor(s)")

        # Output as list for multi-pair support
        return_results(CommandResults(
            outputs_prefix="ComplianceAnchors",
            outputs_key_field="assessment_profile_revision",
            outputs=anchors,
            readable_output=readable
        ))

    except Exception as e:
        demisto.error(f"Failed to get assessment anchor: {e}")
        return_error(f"ComplianceGetAnchor failed: {e}")


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ in ('__main__', '__builtin__', 'builtins'):
    main()
