# Cortex Cloud Content Fetcher

A collection of Python scripts for Cortex XSOAR/XSIAM that automate the fetching, enrichment, and delivery of compliance assessment data from Palo Alto Networks Cortex to HTTP Collectors for analysis and reporting.

## Overview

This project provides a modular framework for extracting cloud compliance data from Cortex XDR/XSIAM and pushing it to HTTP Collectors for centralized reporting and analysis. The scripts handle multiple compliance standards, profiles, and cloud providers in a scalable and maintainable way.

### Key Features

- **Multi-Standard Support**: Process multiple compliance standards (CIS, NIST, PCI DSS, HIPAA, etc.) in a single run
- **Multi-Profile Assessment**: Assess multiple profiles (Production, Development, etc.) against the same standard
- **Automatic Enrichment**: Enriches asset data with control details, severity, rules, and metadata
- **Batch Processing**: Handles large datasets with pagination and batch processing
- **Progress Tracking**: Resumable execution for long-running operations
- **Dry Run Mode**: Test configurations without pushing data to collectors
- **Modular Design**: Individual scripts for each stage or orchestrator for end-to-end execution
- **Error Handling**: Automatic retry logic for network and server errors

## Architecture

### Scripts

The project consists of six main scripts organized in a modular pipeline:

#### 1. **ComplianceLoadConfig**
Loads configuration from XSOAR Lists and sets up the execution environment.

- **Input**: Configuration list name (default: `Script_Compliance_Config`)
- **Output**: Configuration context for subsequent scripts
- **Context**: `ComplianceConfig`, `ComplianceConfigInternal`, `ComplianceAssessments`

#### 2. **ComplianceGetAnchor**
Finds the latest assessment run for each profile/standard pair.

- **Input**: Assessments (JSON array or XSOAR List name)
- **Output**: Assessment anchors containing revision IDs and timestamps
- **Context**: `ComplianceAnchors`

#### 3. **ComplianceGetControls**
Fetches all compliance controls for the specified standards.

- **Input**: Assessments (JSON array or XSOAR List name)
- **Output**: Control catalog with details, severity, rules
- **Context**: `ComplianceControlsCatalog`

#### 4. **ComplianceFetchAssets**
Fetches compliance assets for all anchors and enriches them with control data.

- **Input**: `push_to_collector` (bool), `max_anchors` (int), `reset` (bool)
- **Output**: Enriched asset data with compliance status
- **Context**: `ComplianceAssetsResults`, `ComplianceAssetsData`, `ComplianceAssetsProgress`

#### 5. **ComplianceCreateSummary**
Creates control-level summary reports with aggregated compliance scores.

- **Input**: `push_to_collector` (bool)
- **Output**: Summary reports with per-control compliance scores
- **Context**: `ComplianceSummaryResults`

#### 6. **ComplianceOrchestrator**
Runs all stages in sequence for end-to-end execution.

- **Input**: `assessments` (optional), `push_to_collector` (bool)
- **Output**: Complete compliance results with overall scores
- **Context**: `ComplianceResults`

### Legacy Script

- **compliance_data_fetcher.py**: Original monolithic implementation containing all stages in a single script

## Quick Start

### Prerequisites

1. Cortex XSOAR/XSIAM environment
2. API credentials for Cortex
3. HTTP Collector API keys

### Configuration

Create an XSOAR List named `Script_Compliance_Config` with the following JSON:

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
            "profiles": ["Production", "Development"]
        }
    ]
}
```

### Usage

#### Option 1: Using the Orchestrator (Recommended)

```bash
# Load config (includes ASSESSMENTS)
!ComplianceOrchestrator push_to_collector="false"
```

#### Option 2: Step-by-Step Execution

```bash
# Step 1: Load configuration
!ComplianceLoadConfig

# Step 2: Get assessment anchors
!ComplianceGetAnchor

# Step 3: Get controls catalog
!ComplianceGetControls

# Step 4: Fetch and enrich assets
!ComplianceFetchAssets push_to_collector="false"

# Step 5: Create summary reports
!ComplianceCreateSummary push_to_collector="false"
```

#### Option 3: Using Separate Assessments List

Create an XSOAR List named `Compliance_Assessments`:

```json
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
```

Then run:

```bash
!ComplianceOrchestrator assessments="Compliance_Assessments" push_to_collector="false"
```

## Configuration Reference

### Required Fields

| Field | Description | Example |
|-------|-------------|---------|
| `TENANT_EXTERNAL_URL` | Your Cortex tenant URL | `tenant.xdr.us.paloaltonetworks.com` |
| `COMPLIANCE_API_KEY` | HTTP Collector API key for asset data | `your-api-key` |
| `Controls_API_KEY` | HTTP Collector API key for summary data | `your-api-key` |

### Optional Fields

| Field | Description | Default |
|-------|-------------|---------|
| `PAGE_SIZE` | Pagination size for API calls | `100` |
| `BATCH_SIZE` | Batch size for pushing records | `500` |
| `DRY_RUN` | Skip pushing to collectors (testing) | `true` |
| `ASSESSMENTS` | Array of standard/profile pairs | `[]` |

### Assessment Structure

Each assessment object contains:

- **standard**: Exact compliance standard name (must match Cortex)
- **profiles**: Array of profile names to assess against this standard

Example:

```json
{
    "standard": "CIS Amazon Web Services Foundations Benchmark v3.0.0 Level 1",
    "profiles": ["Production", "Development", "Staging"]
}
```

This creates three profile/standard pairs:
- (Production, CIS AWS...)
- (Development, CIS AWS...)
- (Staging, CIS AWS...)

## Data Flow

```
1. Load Config
   ↓
2. Get Anchors (Profile + Standard → Assessment Revision ID)
   ↓
3. Get Controls (Standard → Control Catalog)
   ↓
4. Fetch Assets (Revision ID → Raw Assets)
   ↓
   Enrich Assets (+ Control Details)
   ↓
   Push to Collector (Asset Records)
   ↓
5. Create Summary (Aggregate by Control)
   ↓
   Push to Collector (Summary Records)
```

## Output Data

### Asset Records

Enriched compliance asset data with fields:

- Assessment metadata (profile, standard, revision)
- Asset details (ID, name, type, provider, region, tags)
- Compliance status (PASSED, FAILED, NOT_ASSESSED)
- Control details (ID, name, severity, rules)
- Evaluation timestamp

### Summary Records

Per-control aggregated compliance data:

- Control identification (ID, name, severity)
- Asset counts (passed, failed, not assessed)
- Compliance score (% of assets passing)
- Overall status (PASSED, FAILED, NOT_EVALUATED)
- Rule count

## Advanced Features

### Progress Tracking

The `ComplianceFetchAssets` script supports resumable execution for large datasets:

```bash
# Process 2 anchors per run
!ComplianceFetchAssets max_anchors="2"

# Continue processing remaining anchors
!ComplianceFetchAssets max_anchors="2"

# Reset and start over
!ComplianceFetchAssets reset="true"
```

### Dry Run Mode

Test configurations without pushing data:

```json
{
    "DRY_RUN": true
}
```

When enabled, scripts process data but skip HTTP Collector pushes.

### Error Handling

- **Automatic Retry**: Network errors and 5xx/429 responses trigger exponential backoff retry
- **Graceful Degradation**: Failed controls are skipped; execution continues
- **Progress Persistence**: Long-running operations save progress for resume

## Common Use Cases

### Multi-Cloud Assessment

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

### Compliance-Focused Assessment

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
    }
]
```

## Troubleshooting

### Find Available Standards

Run this XQL query in Cortex:

```xql
dataset = compliance_standards
| fields name, description
| dedup name
| sort asc name
```

### Find Available Profiles

```xql
dataset = compliance_assessment_results
| filter TYPE = "profile"
| fields ASSESSMENT_PROFILE, STANDARD_NAME
| dedup ASSESSMENT_PROFILE, STANDARD_NAME
| sort asc ASSESSMENT_PROFILE
```

### Clear Context

```bash
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

### Common Errors

**Error: No assessment run found**
- Verify profile and standard names match exactly (case-sensitive)
- Check that an assessment has been run in Cortex

**Error: API key missing**
- Ensure both `COMPLIANCE_API_KEY` and `Controls_API_KEY` are configured
- Verify API keys are valid and have appropriate permissions

**Timeout during execution**
- Use `max_anchors` parameter to process fewer anchors per run
- Increase script timeout in XSOAR settings
- Progress is automatically saved and will resume on next run

## Testing

Refer to [TESTING_GUIDE.md](./TESTING_GUIDE.md) for detailed testing instructions, including:

- Step-by-step testing procedures for each script
- Input examples and expected outputs
- Context verification commands
- Full test sequences

## Best Practices

1. **Start with DRY_RUN=true** to validate configuration
2. **Use standard names from the exact format in Cortex** (copy/paste recommended)
3. **Test with a single profile/standard pair** before running full assessments
4. **Group assessments by standard** for efficient control fetching
5. **Monitor script timeouts** for large datasets and use progress tracking
6. **Use separate HTTP Collectors** for assets and summaries to enable different retention policies

## Requirements

- Cortex XSOAR 6.x or Cortex XSIAM
- Python 3.x with libraries:
  - pandas
  - numpy
  - requests
- Valid Cortex API credentials
- HTTP Collector endpoints configured

## License

Internal use - Palo Alto Networks

## Support

For questions or issues, refer to the [TESTING_GUIDE.md](./TESTING_GUIDE.md) or contact your Cortex administrator.
