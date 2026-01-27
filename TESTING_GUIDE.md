# Compliance Scripts Testing Guide

This guide provides input examples for testing each compliance script in Cortex XSOAR/XSIAM.

---

## Prerequisites

### 1. Create the Configuration List

Before testing, create an XSOAR List named `Script_Compliance_Config` with the following JSON:

```json
{
    "TENANT_EXTERNAL_URL": "your-tenant.xdr.us.paloaltonetworks.com",
    "COMPLIANCE_API_KEY": "your-http-collector-api-key-for-assets",
    "Controls_API_KEY": "your-http-collector-api-key-for-summaries",
    "PAGE_SIZE": 100,
    "BATCH_SIZE": 500,
    "DRY_RUN": true,
    "ASSESSMENTS": [
        {
            "standard": "CIS Critical Security Controls v8.1",
            "profiles": ["CIS Critical Security Controls v8.1", "CIS Critical Security Controls v8.1-AWS", "CIS Critical Security Controls v8.1-Azure", "CIS Critical Security Controls v8.1-OCI"]
        },
        {
            "standard": "CIS Azure Kubernetes Service (AKS) Benchmark v1.5",
            "profiles": ["CIS Azure Kubernetes Service (AKS) Benchmark v1.5"]
        }
    ]
}
```

> **Note:** Set `DRY_RUN` to `true` for initial testing to avoid pushing data to collectors.

**Configuration Fields:**
- `TENANT_EXTERNAL_URL`: Your Cortex tenant URL
- `COMPLIANCE_API_KEY`: HTTP Collector API key for assets data
- `Controls_API_KEY`: HTTP Collector API key for summary data
- `PAGE_SIZE`: Pagination size for API calls (default: 100)
- `BATCH_SIZE`: Batch size for pushing records (default: 500)
- `DRY_RUN`: Set to `true` to skip pushing to collectors
- `ASSESSMENTS`: (Optional) Define assessments in config so you don't need to pass them as arguments

**Note:** `ASSESSMENTS` can be an array (multiple standards) or a single object:

```json
{
    "ASSESSMENTS": {
        "standard": "CIS Critical Security Controls v8.1",
        "profiles": ["Profile1", "Profile2"]
    }
}
```

### 2. Alternative: Create a Separate Assessments List

Instead of including `ASSESSMENTS` in the config, you can create a separate XSOAR List named `Compliance_Assessments` with the JSON defining your profile/standard pairs:

```json
[
    {
        "standard": "CIS Amazon Web Services Foundations Benchmark v3.0.0 Level 1",
        "profiles": ["Production", "Development", "Staging"]
    },
    {
        "standard": "CIS Microsoft Azure Foundations Benchmark v2.1.0 Level 1",
        "profiles": ["Production", "Development"]
    },
    {
        "standard": "NIST 800-53 Rev. 5",
        "profiles": ["Production", "Compliance"]
    },
    {
        "standard": "PCI DSS v4.0",
        "profiles": ["Production"]
    }
]
```

**JSON Structure:**
- `standard`: The exact compliance standard name (must match Cortex)
- `profiles`: Array of profile names to assess against this standard

**Benefits of grouping by standard:**
- Each standard is listed once (no repetition)
- Easy to see which profiles use which standard
- Controls are fetched once per standard (efficient)
- Supports one-to-many: one standard with multiple profiles

### 3. CommonServerPython Import

If you encounter errors related to `CommonServerPython`, you may need to remove or comment out the import line from the scripts:

```python
# from CommonServerPython import *
```

The XSOAR runtime provides these functions globally in most environments.

---

## Script Testing Order

Run scripts in this order (each depends on context from previous scripts):

1. ComplianceLoadConfig
2. ComplianceGetAnchor
3. ComplianceGetControls
4. ComplianceFetchAssets
5. ComplianceCreateSummary

Or use the orchestrator to run all at once:

6. ComplianceOrchestrator

---

## Key Concepts

### What is an Assessment Anchor?

An **assessment anchor** is a reference point that identifies a specific compliance assessment run in Cortex. It contains the data needed to fetch the correct assets and results for a particular profile/standard combination.

**Anchor Fields:**

| Field | Description |
|-------|-------------|
| `assessment_profile_revision` | Unique ID for this specific assessment run |
| `last_evaluation_time` | Timestamp when the assessment was last evaluated |
| `asset_group_id` | The asset group that was assessed |
| `profile_name` | The profile name (e.g., "Production") |
| `standard_name` | The standard name (e.g., "CIS Controls v8.1") |

**Why it's needed:**

The Cortex API requires the `assessment_profile_revision` to fetch:
- Compliance assets (which resources passed/failed)
- Assessment results (control status)

**Flow:**

```
Profile + Standard
       │
       ▼
┌─────────────────────────────────────────┐
│  GET /compliance/get_assessment_results │
│  Filter by profile + standard           │
│  Get the LATEST evaluation              │
└─────────────────────────────────────────┘
       │
       ▼
   ANCHOR
   {
     assessment_profile_revision: "abc123",
     last_evaluation_time: 1705123456000,
     asset_group_id: "xyz789"
   }
       │
       ▼
   Used by ComplianceFetchAssets
   to get actual compliance data
```

**Analogy:** Think of it like a bookmark. When you run a compliance assessment, Cortex creates a "snapshot" of the results. The anchor is the bookmark that points to that specific snapshot, so later scripts can retrieve the exact data from that assessment run.

---

## Context Path Reference

| Script | Context Output Path | Type |
|--------|---------------------|------|
| ComplianceLoadConfig | `ComplianceConfig`, `ComplianceConfigInternal`, `ComplianceAssessments`* | Object, Object, List |
| ComplianceGetAnchor | `ComplianceAnchors` | List of anchors |
| ComplianceGetControls | `ComplianceControlsCatalog` | Dict keyed by standard |
| ComplianceFetchAssets | `ComplianceAssetsResults`, `ComplianceAssetsData`, `ComplianceAssetsProgress` | List, Dict, Object |
| ComplianceCreateSummary | `ComplianceSummaryResults` | List of summaries |
| ComplianceOrchestrator | `ComplianceResults` | Object with results |

*`ComplianceAssessments` is only populated if ASSESSMENTS is defined in the config.

---

## Script 1: ComplianceLoadConfig

**Purpose:** Loads configuration from XSOAR List and sets it to context. If `ASSESSMENTS` is defined in the config, it will be loaded to `ComplianceAssessments` context.

### Inputs

| Argument | Required | Description | Example |
|----------|----------|-------------|---------|
| `list_name` | No | Name of config list | `Script_Compliance_Config` |

### Test Command (War Room)

```
!ComplianceLoadConfig
```

Or with custom list name:

```
!ComplianceLoadConfig list_name="My_Custom_Config_List"
```

### Expected Output

- Configuration table displayed in War Room
- Context key `ComplianceConfigInternal` populated
- Context key `ComplianceAssessments` populated (if ASSESSMENTS defined in config)

### Verify Context

```
!Print value=${ComplianceConfig.ConfigLoaded}
!Print value=${ComplianceConfig.AssessmentsLoaded}
!Print value=${ComplianceAssessments}
```

---

## Script 2: ComplianceGetAnchor

**Purpose:** Finds the latest assessment run for profile/standard pair(s).

### Inputs

| Argument | Required | Description | Example |
|----------|----------|-------------|---------|
| `assessments` | No* | JSON array or XSOAR List name | `Compliance_Assessments` |

*If not provided, reads from `ComplianceAssessments` context (set by ComplianceLoadConfig from ASSESSMENTS in config).

### Test Commands (War Room)

**Using XSOAR List (Recommended):**
```
!ComplianceGetAnchor assessments="Compliance_Assessments"
```

**Using direct JSON:**
```
!ComplianceGetAnchor assessments='[{"standard": "CIS Amazon Web Services Foundations Benchmark v3.0.0 Level 1", "profiles": ["Production", "Development"]}]'
```

### Expected Output

- Anchor details table for each pair (Profile Revision, Asset Group ID, Last Evaluation time)
- Context key `ComplianceAnchors` populated (list of anchor objects)

### Verify Context

```
!Print value=${ComplianceAnchors}
```

---

## Script 3: ComplianceGetControls

**Purpose:** Fetches all controls for compliance standard(s). Automatically deduplicates.

### Inputs

| Argument | Required | Description | Example |
|----------|----------|-------------|---------|
| `assessments` | No* | JSON array or XSOAR List name | `Compliance_Assessments` |

*If not provided, reads from `ComplianceAssessments` context (set by ComplianceLoadConfig from ASSESSMENTS in config).

### Test Commands (War Room)

**Using XSOAR List (Recommended):**
```
!ComplianceGetControls assessments="Compliance_Assessments"
```

**Using direct JSON:**
```
!ComplianceGetControls assessments='[{"standard": "CIS Amazon Web Services Foundations Benchmark v3.0.0 Level 1", "profiles": ["Production"]}, {"standard": "NIST 800-53 Rev. 5", "profiles": ["Production"]}]'
```

### Expected Output

- Controls summary by severity for each standard
- Context key `ComplianceControlsCatalog` populated (dict keyed by standard name)

### Verify Context

```
!Print value=${ComplianceControlsCatalog}
```

---

## Script 4: ComplianceFetchAssets

**Purpose:** Fetches compliance assets for all anchors in context, enriches them, and optionally pushes to collector. Supports progress tracking for resumable execution on timeouts.

### Prerequisites

- `ComplianceAnchors` must be populated (run ComplianceGetAnchor first)
- `ComplianceControlsCatalog` must be populated (run ComplianceGetControls first)

### Inputs

| Argument | Required | Description | Example |
|----------|----------|-------------|---------|
| `push_to_collector` | No | Push to HTTP Collector | `true` or `false` |
| `max_anchors` | No | Max anchors to process per run (0 = all) | `1`, `2`, `5` |
| `reset` | No | Clear previous progress and start fresh | `true` or `false` |

### Test Commands (War Room)

**With collector push (respects DRY_RUN config):**
```
!ComplianceFetchAssets push_to_collector="true"
```

**Skip collector push:**
```
!ComplianceFetchAssets push_to_collector="false"
```

**Process only 1 anchor per run (to avoid timeout):**
```
!ComplianceFetchAssets max_anchors="1"
```

**Reset progress and start fresh:**
```
!ComplianceFetchAssets reset="true"
```

### Progress Tracking

If the script times out, progress is automatically saved. The next run will resume from where it left off:

```bash
# First run - processes some anchors before timeout
!ComplianceFetchAssets max_anchors="2"

# Second run - resumes from remaining anchors
!ComplianceFetchAssets max_anchors="2"

# Continue until all complete
!ComplianceFetchAssets max_anchors="2"
```

To start over (clear previous progress):
```
!ComplianceFetchAssets reset="true"
```

### Expected Output

- Progress status (completed/total anchors, pending count)
- Assets summary for each profile/standard pair processed this run
- CSV file(s) attached to War Room (one per pair)
- Context key `ComplianceAssetsResults` populated
- Context key `ComplianceAssetsData` populated (cumulative across runs)
- Context key `ComplianceAssetsProgress` populated (tracks progress)

### Verify Context

```
!Print value=${ComplianceAssetsResults}
!Print value=${ComplianceAssetsProgress}
```

---

## Script 5: ComplianceCreateSummary

**Purpose:** Creates control-level summary for all pairs in context and optionally pushes to collector.

### Prerequisites

- `ComplianceAnchors` must be populated
- `ComplianceControlsCatalog` must be populated
- `ComplianceAssetsData` must be populated (run ComplianceFetchAssets first)

### Inputs

| Argument | Required | Description | Example |
|----------|----------|-------------|---------|
| `push_to_collector` | No | Push to HTTP Collector | `true` or `false` |

### Test Commands (War Room)

**With collector push:**
```
!ComplianceCreateSummary push_to_collector="true"
```

**Skip collector push:**
```
!ComplianceCreateSummary push_to_collector="false"
```

### Expected Output

- Compliance score and control status summary for each pair
- CSV file(s) attached to War Room (one per pair)
- Context key `ComplianceSummaryResults` populated

### Verify Context

```
!Print value=${ComplianceSummaryResults}
```

---

## Script 6: ComplianceOrchestrator

**Purpose:** Runs all stages for multiple profile/standard pairs in a single command.

### Inputs

| Argument | Required | Description | Example |
|----------|----------|-------------|---------|
| `assessments` | No* | JSON array or XSOAR List name | `Compliance_Assessments` |
| `push_to_collector` | No | Push to HTTP Collector | `true` or `false` |

*If not provided, reads from `ComplianceAssessments` context (set by ComplianceLoadConfig from ASSESSMENTS in config).

### Test Commands (War Room)

**Using ASSESSMENTS from config (Recommended):**
```
!ComplianceOrchestrator push_to_collector="false"
```

**Using XSOAR List:**
```
!ComplianceOrchestrator assessments="Compliance_Assessments" push_to_collector="false"
```

**Using direct JSON:**
```
!ComplianceOrchestrator assessments='[{"standard": "CIS Amazon Web Services Foundations Benchmark v3.0.0 Level 1", "profiles": ["Production", "Development"]}, {"standard": "NIST 800-53 Rev. 5", "profiles": ["Production"]}]' push_to_collector="false"
```

### Expected Output

- Per-pair summaries
- Overall compliance score
- Context key `ComplianceResults` populated

---

## Assessments JSON Examples

### Example 1: Single Standard, Multiple Profiles

```json
[
    {
        "standard": "CIS Amazon Web Services Foundations Benchmark v3.0.0 Level 1",
        "profiles": ["Production", "Development", "Staging", "QA"]
    }
]
```

This assesses the AWS CIS benchmark against 4 different profiles.

### Example 2: Multiple Standards, Single Profile

```json
[
    {
        "standard": "CIS Amazon Web Services Foundations Benchmark v3.0.0 Level 1",
        "profiles": ["Production"]
    },
    {
        "standard": "NIST 800-53 Rev. 5",
        "profiles": ["Production"]
    },
    {
        "standard": "PCI DSS v4.0",
        "profiles": ["Production"]
    }
]
```

This assesses the Production profile against 3 different standards.

### Example 3: Multi-Cloud Assessment

```json
[
    {
        "standard": "CIS Amazon Web Services Foundations Benchmark v3.0.0 Level 1",
        "profiles": ["AWS-Production", "AWS-Development"]
    },
    {
        "standard": "CIS Microsoft Azure Foundations Benchmark v2.1.0 Level 1",
        "profiles": ["Azure-Production", "Azure-Development"]
    },
    {
        "standard": "CIS Google Cloud Platform Foundation Benchmark v3.0.0 Level 1",
        "profiles": ["GCP-Production"]
    }
]
```

### Example 4: Compliance-Focused Assessment

```json
[
    {
        "standard": "NIST 800-53 Rev. 5",
        "profiles": ["Production", "Compliance-Audit"]
    },
    {
        "standard": "PCI DSS v4.0",
        "profiles": ["Production", "PCI-Scope"]
    },
    {
        "standard": "HIPAA",
        "profiles": ["Production", "Healthcare-Systems"]
    },
    {
        "standard": "SOC 2 Type II",
        "profiles": ["Production"]
    }
]
```

---

## Common Compliance Standards

Here are some standard names you might use (verify exact names in your Cortex environment):

### CIS Benchmarks
- `CIS Amazon Web Services Foundations Benchmark v3.0.0 Level 1`
- `CIS Amazon Web Services Foundations Benchmark v3.0.0 Level 2`
- `CIS Microsoft Azure Foundations Benchmark v2.1.0 Level 1`
- `CIS Microsoft Azure Foundations Benchmark v2.1.0 Level 2`
- `CIS Google Cloud Platform Foundation Benchmark v3.0.0 Level 1`
- `CIS Google Cloud Platform Foundation Benchmark v3.0.0 Level 2`
- `CIS Kubernetes Benchmark v1.8.0 Level 1`

### Regulatory Standards
- `NIST 800-53 Rev. 5`
- `NIST CSF v1.1`
- `PCI DSS v4.0`
- `HIPAA`
- `SOC 2 Type II`
- `ISO 27001:2022`
- `GDPR`

### Cloud Provider Standards
- `AWS Well-Architected Framework`
- `Azure Security Benchmark v3`
- `Google Cloud Security Best Practices`

---

## Troubleshooting

### Find Available Standards

Run this XQL query in Cortex to list available standards:

```xql
dataset = compliance_standards
| fields name, description
| dedup name
| sort asc name
```

### Find Available Profiles

Run this to list assessment profiles:

```xql
dataset = compliance_assessment_results
| filter TYPE = "profile"
| fields ASSESSMENT_PROFILE, STANDARD_NAME
| dedup ASSESSMENT_PROFILE, STANDARD_NAME
| sort asc ASSESSMENT_PROFILE
```

### Check Context Between Scripts

```
!Print value=${ComplianceConfigInternal}
!Print value=${ComplianceAnchors}
!Print value=${ComplianceControlsCatalog}
!Print value=${ComplianceAssetsResults}
!Print value=${ComplianceAssetsData}
!Print value=${ComplianceSummaryResults}
```

### Clear Context (if needed)

```
!DeleteContext key=ComplianceConfigInternal
!DeleteContext key=ComplianceAnchors
!DeleteContext key=ComplianceControlsCatalog
!DeleteContext key=ComplianceAssetsResults
!DeleteContext key=ComplianceAssetsData
!DeleteContext key=ComplianceAssetsProgress
!DeleteContext key=ComplianceSummaryResults
!DeleteContext key=ComplianceResults
!DeleteContext key=ComplianceAssessments
```

### Clear Progress Only (to restart asset fetching)

```
!DeleteContext key=ComplianceAssetsProgress
!DeleteContext key=ComplianceAssetsData
```

Or use the reset argument:
```
!ComplianceFetchAssets reset="true"
```

### Common Errors

**Error: `'str' object has no attribute 'get'`**
- You passed the config list name (`Script_Compliance_Config`) instead of an assessments list
- The config list contains an object with many fields, not an array of assessments
- Solution: Either don't pass `assessments` argument (let it read from context), or pass a list that contains only the assessments array

**Error: `the JSON object must be str, bytes or bytearray, not dict`**
- This was a bug in older versions where XSOAR passed data as Python objects instead of strings
- Update to the latest script versions which handle both formats

---

## Full Test Sequence Example

### Using ASSESSMENTS from Config (Recommended)

```bash
# Step 1: Create Script_Compliance_Config list with ASSESSMENTS included

# Step 2: Run orchestrator (no assessments argument needed)
!ComplianceOrchestrator push_to_collector="false"

# Step 3: Verify results
!Print value=${ComplianceResults}
```

### Using Separate XSOAR List

```bash
# Step 1: Create the Compliance_Assessments list in XSOAR with your JSON

# Step 2: Run orchestrator with list reference
!ComplianceOrchestrator assessments="Compliance_Assessments" push_to_collector="false"

# Step 3: Verify results
!Print value=${ComplianceResults}
```

### Manual Step-by-Step (Using Config)

```bash
# Step 1: Load config (also loads ASSESSMENTS to context)
!ComplianceLoadConfig

# Step 2: Get anchors (reads from context)
!ComplianceGetAnchor

# Step 3: Get controls (reads from context)
!ComplianceGetControls

# Step 4: Fetch assets
!ComplianceFetchAssets push_to_collector="false"

# Step 5: Create summary
!ComplianceCreateSummary push_to_collector="false"

# Verify results
!Print value=${ComplianceSummaryResults}
```

### Manual Step-by-Step with JSON Argument

```bash
# Step 1: Load config
!ComplianceLoadConfig

# Step 2: Get anchors using JSON
!ComplianceGetAnchor assessments='[{"standard": "CIS Amazon Web Services Foundations Benchmark v3.0.0 Level 1", "profiles": ["Production", "Development"]}]'

# Step 3: Get controls using JSON
!ComplianceGetControls assessments='[{"standard": "CIS Amazon Web Services Foundations Benchmark v3.0.0 Level 1", "profiles": ["Production", "Development"]}]'

# Step 4: Fetch assets
!ComplianceFetchAssets push_to_collector="false"

# Step 5: Create summary
!ComplianceCreateSummary push_to_collector="false"

# Verify results
!Print value=${ComplianceSummaryResults}
```

---

## Notes

1. **Standard names must match exactly** - Copy the exact name from Cortex
2. **Profile names are case-sensitive** - Verify the exact profile name
3. **Test with DRY_RUN=true first** - Avoid pushing test data to collectors
4. **Check context between scripts** - Ensure data flows correctly
5. **Scripts timeout** - Large datasets may take time; consider increasing script timeout
6. **CommonServerPython** - If import errors occur, comment out the `from CommonServerPython import *` line
7. **XSOAR Lists** - Store JSON in lists for reusable, maintainable configurations
8. **Flexible input formats** - Scripts accept assessments as:
   - JSON array: `[{"standard": "...", "profiles": [...]}]`
   - Single JSON object: `{"standard": "...", "profiles": [...]}`
   - XSOAR List name: `"Compliance_Assessments"`
   - From context (no argument needed if ASSESSMENTS defined in config)
