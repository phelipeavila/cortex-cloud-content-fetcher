"""ComplianceLoadConfig - Cortex XSOAR Script

Loads compliance script configuration from an XSOAR List and validates it.
Sets configuration to context for use by other compliance scripts.

Note: Cortex API authentication is handled automatically by core-api-post.
      Only HTTP Collector API keys are needed in the configuration.

Arguments:
    list_name (str): Optional. Name of the XSOAR List containing config JSON.
                     Defaults to 'Script_Compliance_Config'.

Outputs:
    Context path: ComplianceConfig
        - TENANT_EXTERNAL_URL: Tenant external URL
        - COLLECTOR_URL: HTTP Collector URL
        - PAGE_SIZE: Pagination size
        - BATCH_SIZE: Batch size for pushing records
        - DRY_RUN: Whether dry run mode is enabled
        - ConfigLoaded: True if config loaded successfully
        - AssessmentsLoaded: True if assessments found in config

    Context path: ComplianceAssessments (if ASSESSMENTS defined in config)
        - List of assessment objects with standard and profiles
"""

# from CommonServerPython import *
import json


# =============================================================================
# CONSTANTS
# =============================================================================

DEFAULT_LIST_NAME = "Script_Compliance_Config"
REQUIRED_FIELDS = ['TENANT_EXTERNAL_URL']


# =============================================================================
# MAIN FUNCTION
# =============================================================================

def main():
    try:
        args = demisto.args()
        list_name = args.get('list_name', DEFAULT_LIST_NAME)

        demisto.info(f"Loading configuration from XSOAR List: {list_name}")

        # Fetch the configuration from XSOAR Lists
        res = demisto.executeCommand("getList", {"listName": list_name})

        if is_error(res) or not res[0].get('Contents'):
            raise DemistoException(f"Configuration List '{list_name}' not found or empty.")

        # Parse JSON
        try:
            config_data = json.loads(res[0]['Contents'])
        except ValueError as e:
            raise DemistoException(f"The content of List '{list_name}' is not valid JSON: {e}")

        # Validate required fields
        missing_fields = [f for f in REQUIRED_FIELDS if not config_data.get(f)]
        if missing_fields:
            raise DemistoException(f"Missing required configuration fields: {', '.join(missing_fields)}")

        # Build collector URL
        tenant_url = config_data.get('TENANT_EXTERNAL_URL')
        collector_url = f"https://api-{tenant_url}/logs/v1/event"

        # Check for assessments in config
        assessments_raw = config_data.get('ASSESSMENTS', [])
        assessments_loaded = False
        total_pairs = 0

        # Normalize to list (handle single dict or list)
        if isinstance(assessments_raw, dict):
            assessments = [assessments_raw]
        elif isinstance(assessments_raw, list):
            assessments = assessments_raw
        else:
            assessments = []

        if assessments:
            # Validate assessments structure
            valid_assessments = []
            for item in assessments:
                if isinstance(item, dict) and item.get('standard') and item.get('profiles'):
                    valid_assessments.append(item)
                    profiles = item.get('profiles', [])
                    if isinstance(profiles, str):
                        total_pairs += 1
                    else:
                        total_pairs += len(profiles)

            if valid_assessments:
                assessments_loaded = True
                demisto.setContext("ComplianceAssessments", valid_assessments)
                demisto.info(f"Loaded {len(valid_assessments)} assessment(s) with {total_pairs} total pair(s)")

        # Prepare output
        output_data = {
            "TENANT_EXTERNAL_URL": tenant_url,
            "COLLECTOR_URL": collector_url,
            "PAGE_SIZE": config_data.get('PAGE_SIZE', 100),
            "BATCH_SIZE": config_data.get('BATCH_SIZE', 500),
            "DRY_RUN": config_data.get('DRY_RUN', True),
            "ConfigLoaded": True,
            "AssessmentsLoaded": assessments_loaded
        }

        # Store full config (including collector API keys) for internal use
        full_config = {
            **output_data,
            "COMPLIANCE_API_KEY": config_data.get('COMPLIANCE_API_KEY'),
            "CONTROLS_API_KEY": config_data.get('Controls_API_KEY'),
        }

        # Set full config to context (for other scripts)
        demisto.setContext("ComplianceConfigInternal", full_config)

        # Build readable output
        dry_run_status = "ENABLED" if output_data['DRY_RUN'] else "DISABLED"
        readable = f"## Compliance Configuration Loaded\n\n"
        readable += f"| Setting | Value |\n"
        readable += f"| :--- | :--- |\n"
        readable += f"| Tenant URL | `{tenant_url}` |\n"
        readable += f"| Page Size | {output_data['PAGE_SIZE']} |\n"
        readable += f"| Batch Size | {output_data['BATCH_SIZE']} |\n"
        readable += f"| Dry Run | **{dry_run_status}** |\n"

        if assessments_loaded:
            readable += f"\n### Assessments Loaded\n\n"
            readable += f"**Total Standards:** {len(valid_assessments)}\n"
            readable += f"**Total Pairs:** {total_pairs}\n\n"
            for item in valid_assessments:
                profiles = item.get('profiles', [])
                if isinstance(profiles, str):
                    profiles = [profiles]
                readable += f"- **{item.get('standard')}**: {', '.join(profiles)}\n"
        else:
            readable += f"\n*No assessments defined in config. Pass `assessments` argument to scripts.*\n"

        readable += f"\n*Note: Cortex API auth handled by core-api-post*\n"

        demisto.info("Configuration loaded successfully")

        return_results(CommandResults(
            outputs_prefix="ComplianceConfig",
            outputs_key_field="TENANT_EXTERNAL_URL",
            outputs=output_data,
            readable_output=readable
        ))

    except Exception as e:
        demisto.error(f"Failed to load configuration: {e}")
        return_error(f"ComplianceLoadConfig failed: {e}")


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ in ('__main__', '__builtin__', 'builtins'):
    main()
