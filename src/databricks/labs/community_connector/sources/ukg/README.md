# Lakeflow UKG Pro Community Connector

This documentation provides setup instructions and reference information for the UKG Pro (formerly UltiPro) source connector. The connector ingests HR, payroll, recruiting, and onboarding data from UKG Pro's REST APIs into Databricks.

## Prerequisites

- A UKG Pro service account with API access (created under System Configuration > Security > Service Account Administration).
- Your tenant's service hostname (e.g., `service5.ultipro.com`), found in the UKG admin console under System Configuration > Security > Web Services.
- (Optional) OAuth 2.0 client credentials for recruiting and onboarding data.

## Setup

### Required Connection Parameters

| Parameter | Type | Required | Description | Example |
|---|---|---|---|---|
| `service_host` | String | Yes | Your UKG Pro service hostname | `service5.ultipro.com` |
| `username` | String | Yes | Service account username | `MYSERVICEACCT` |
| `password` | String | Yes | Service account password | `*****` |
| `customer_api_key` | String | Yes | 5-character customer API key | `ABCDE` |
| `client_id` | String | No | OAuth client ID (for recruiting/onboarding tables) | `my-client-id` |
| `client_secret` | String | No | OAuth client secret (for recruiting/onboarding tables) | `*****` |
| `tenant_alias` | String | No | Tenant alias (for recruiting/onboarding URL paths) | `mycompany` |
| `externalOptionsAllowList` | String | Yes | Comma-separated options to pass through. Set to: `max_records_per_batch,per_page` | `max_records_per_batch,per_page` |

### Obtaining Credentials

1. **Service Account**: A UKG Pro admin creates the service account under System Configuration > Security > Service Account Administration. The username, password, and customer API key are displayed on that page.
2. **Service Host**: Found on the same admin page under Web Services. The hostname varies by datacenter (e.g., `service2.ultipro.com`, `service5.ultipro.com`, `servicet.ultipro.com` for test).
3. **OAuth Credentials** (optional): Required only for recruiting and onboarding tables. Contact your UKG admin to provision OAuth client credentials with the appropriate scopes (`recruiting.domain`, `onboarding`).

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:
1. Follow the Lakeflow Community Connector UI flow from the "Add Data" page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Set `externalOptionsAllowList` to `max_records_per_batch,per_page` to enable per-table configuration.

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The connector supports 27 objects across five API categories.

### Personnel (CDC)

| Object | Primary Keys | Ingestion Mode | Cursor Field | Delete Sync |
|---|---|---|---|---|
| `person_details` | `employeeId`, `companyId` | CDC | `datetimeChanged` | No |
| `employment_details` | `employeeID`, `companyID` | CDC | `dateTimeChanged` | No |
| `compensation_details` | `employeeId`, `companyId` | CDC | `dateTimeChanged` | No |
| `employee_demographic_details` | `employeeId`, `companyId` | CDC | `dateTimeChanged` | No |
| `supervisor_details` | `employeeId`, `companyId` | CDC | `dateTimeChanged` | No |
| `job_history_details` | `employeeId`, `companyId` | CDC | `dateTimeChanged` | No |
| `pto_plans` | `employeeId`, `companyId`, `planName` | CDC | `dateTimeChanged` | No |
| `employee_education` | `employeeId`, `companyId` | CDC | `dateTimeChanged` | No |
| `employee_contact` | `employeeId`, `companyId` | CDC | `dateTimeChanged` | No |
| `employee_deductions` | `employeeId`, `companyId` | CDC | `dateTimeChanged` | No |

### Configuration / Org Structure (Snapshot)

| Object | Primary Keys | Ingestion Mode | Cursor Field | Delete Sync |
|---|---|---|---|---|
| `company_details` | `companyId` | Snapshot | — | No |
| `locations` | `locationCode` | Snapshot | — | No |
| `org_levels` | `code`, `level` | Snapshot | — | No |
| `jobs` | `jobCode` | Snapshot | — | No |

### Payroll (CDC)

| Object | Primary Keys | Ingestion Mode | Cursor Field | Delete Sync |
|---|---|---|---|---|
| `earnings_history` | `employeeId`, `companyId` | CDC | `dateTimeChanged` | No |

### Recruiting (CDC, requires OAuth)

| Object | Primary Keys | Ingestion Mode | Cursor Field | Delete Sync |
|---|---|---|---|---|
| `applications` | `id` | CDC | `updated_at` | No |
| `candidates` | `id` | CDC | `updated_at` | No |
| `opportunities` | `id` | CDC | `updated_at` | No |
| `offers` | `id` | CDC | `updated_at` | No |
| `recruiting_processes` | `id` | Snapshot | — | No |
| `applicant_sources` | `id` | Snapshot | — | No |
| `job_boards` | `id` | Snapshot | — | No |

### Onboarding (Snapshot, requires OAuth)

| Object | Primary Keys | Ingestion Mode | Cursor Field | Delete Sync |
|---|---|---|---|---|
| `new_hires_in_progress` | `id` | Snapshot | — | No |
| `new_hires_completed` | `id` | Snapshot | — | No |
| `new_hires_canceled` | `id` | Snapshot | — | No |

### Object Descriptions

**person_details** — Core employee personal information including name, address, contact info, national ID, demographics, and military service details.

**employment_details** — Employment records including job code, status, hire/termination dates, supervisor, pay group, org level assignments, and position.

**compensation_details** — Compensation data including salary, hourly rates, pay scale, performance review info, and scheduled hours/FTE.

**employee_demographic_details** — Demographic attributes including gender, ethnicity, disability status, and military service.

**supervisor_details** — Supervisor relationships linking employees to their managers.

**job_history_details** — Historical job changes with effective dates and reason codes.

**pto_plans** — PTO plan balances including accrued, used, available, and carried-over hours.

**company_details** — Organization/company reference data including name, address, and FEIN.

**locations** — Work location reference data with addresses and active status.

**org_levels** — Organization hierarchy levels with budget and reporting categories.

**jobs** — Job code definitions with titles, grades, and EEO/FLSA classifications.

**applications** — Recruiting applications linking candidates to job opportunities.

**candidates** — Candidate records with contact information and qualifications.

**opportunities** — Job requisitions/postings with descriptions, locations, and status.

**new_hires_in_progress / completed / canceled** — Onboarding records segmented by status.

## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | Table name in the source system |
| `destination_catalog` | No | Target catalog (defaults to pipeline's default) |
| `destination_schema` | No | Target schema (defaults to pipeline's default) |
| `destination_table` | No | Target table name (defaults to `source_table`) |

### Common `table_configuration` options

| Option | Required | Description |
|---|---|---|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Only applicable to CDC or Snapshot tables. |
| `primary_keys` | No | List of columns to override the connector's default primary keys |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking |

### Special `table_configuration` options

| Option | Applicable Objects | Required | Description | Default |
|---|---|---|---|---|
| `max_records_per_batch` | All CDC objects | No | Maximum records to fetch per batch | `10000` |
| `per_page` | All objects | No | Records per API page request | Endpoint maximum |

## Data Type Mapping

| Source Type | Spark SQL Type |
|---|---|
| String fields | `StringType` |
| Boolean fields | `BooleanType` |
| Numeric amounts (salary, rates, hours) | `DoubleType` |
| Integer counts | `LongType` |
| Date/datetime strings | `StringType` (ISO 8601 format) |

## How to Run

### Step 1: Clone/Copy the Source Connector Code
Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline
1. Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`).
2. Configure each object with the appropriate table-specific options:

```json
{
  "pipeline_spec": {
      "connection_name": "my_ukg_connection",
      "object": [
        {
            "table": {
                "source_table": "person_details",
                "table_configuration": {
                    "max_records_per_batch": "5000"
                }
            }
        },
        {
            "table": {
                "source_table": "employment_details",
                "table_configuration": {
                    "max_records_per_batch": "5000"
                }
            }
        },
        {
            "table": {
                "source_table": "company_details"
            }
        },
        {
            "table": {
                "source_table": "applications",
                "table_configuration": {
                    "max_records_per_batch": "100"
                }
            }
        }
      ]
  }
}
```
3. (Optional) Customize the source connector code if needed for special use cases.

### Step 3: Run and Schedule the Pipeline

#### Best Practices

- **Start Small**: Begin with core personnel tables (`person_details`, `employment_details`, `company_details`) before adding more objects.
- **Use Incremental Sync**: CDC tables (personnel, payroll, recruiting) only fetch records modified since the last sync, reducing API calls significantly.
- **Mind Rate Limits**: UKG Pro enforces per-tenant rate limits (HTTP 429). The connector retries automatically with exponential backoff, but avoid running too many tables concurrently.
- **Snapshot Tables Are Small**: Configuration tables (`company_details`, `locations`, `org_levels`, `jobs`) are typically small reference datasets safe to full-refresh on every run.
- **OAuth Tables Are Optional**: Recruiting and onboarding tables only appear if `client_id` and `client_secret` are provided. You can start with core HCM data using only Basic Auth.

#### Troubleshooting

**Common Issues:**

- **Authentication errors (401)**: Verify your `username`, `password`, and `customer_api_key` match the service account in UKG Pro. Ensure the service account is active and not locked.
- **OAuth token errors**: Confirm `client_id`, `client_secret`, and `tenant_alias` are correct. The OAuth token endpoint must be accessible from your Databricks workspace.
- **Table not found**: Ensure `source_table` matches one of the supported object names exactly (e.g., `person_details`, not `PersonDetails`).
- **No new data on incremental sync**: The connector caps its cursor at initialization time. If no records were modified since the last sync, the connector correctly returns no data. This is expected behavior.
- **Rate limiting (HTTP 429)**: The connector handles rate limiting automatically with exponential backoff (up to 5 retries). If errors persist, reduce `max_records_per_batch` or space out pipeline runs.
- **Missing recruiting/onboarding tables**: These tables require OAuth credentials. Add `client_id`, `client_secret`, and `tenant_alias` to your connection parameters.

## Authentication Details

### Core HCM (Basic Auth)
All personnel, configuration, and payroll endpoints use HTTP Basic Authentication with the `US-Customer-Api-Key` header. Credentials are validated on every request.

### Recruiting & Onboarding (OAuth 2.0)
These endpoints use OAuth 2.0 client credentials flow. The connector automatically fetches and caches Bearer tokens, refreshing them before expiry or on 401 responses.

## References

- [UKG Pro Developer Hub](https://developer.ukg.com/hcm/reference/welcome-to-the-ukg-pro-api)
- [UKG Pro OpenAPI Specs](https://developer.ukg.com/hcm/openapi)
- [API Rate Limiting](https://developer.ukg.com/hcm/docs/api-rate-limiting)
- [Service Account Setup](https://developer.ukg.com/hcm/docs/web-service-account)
- [Lakeflow Community Connectors Repository](https://github.com/databrickslabs/lakeflow-community-connectors)
