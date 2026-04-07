# UKG Pro Connector — Development Context

## Overview

Building a Lakeflow Community Connector for **UKG Pro** (formerly UltiPro), a major HR/HCM platform. The connector implements the `LakeflowConnect` interface to ingest HR data into Databricks via the Spark Declarative Pipeline.

## UKG Pro API Summary

### Authentication — Basic Auth (Core HCM REST API)

Every request requires two headers:

| Header | Value |
|--------|-------|
| `Authorization` | `Basic base64(username:password)` — service account credentials |
| `US-Customer-Api-Key` | 5-character customer-specific API key |

Service accounts are created by a UKG Pro admin under System Configuration > Security > Service Account Administration.

**Newer APIs** (Recruiting, Onboarding, Platform) use **OAuth 2.0 client credentials**:
```
POST https://{hostname}/authentication/v1/token
Content-Type: application/x-www-form-urlencoded
grant_type=client_credentials&client_id=CLIENT_ID&client_secret=CLIENT_SECRET
```
Returns a Bearer token (~1 hour expiry) + refresh token (7 day expiry).

### Base URL / Tenant Structure

URLs are per-customer, per-datacenter:

| Region | Pattern |
|--------|---------|
| US | `https://service{N}.ultipro.com` (e.g., service2, service5) |
| Canada | `https://service{N}.ultipro.ca` |
| Test | `https://servicet.ultipro.com` |

The hostname is found in the UKG admin console under System Configuration > Security > Web Services.

### Pagination

Offset-based with two query parameters:

| Parameter | Default | Max |
|-----------|---------|-----|
| `page` | 1 | — |
| `per_Page` | 100 | 10,000 (for most endpoints post-Jan 2022) |

### Rate Limits

- Per tenant, per service account, over a 1-minute window.
- Specific thresholds **not publicly documented**.
- Returns **HTTP 429** when exceeded — no `Retry-After` header.
- Recommended: exponential backoff with 1s initial delay.

### Incremental / CDC Support

Three mechanisms:

1. **`/personnel/v1/employee-changes?startDate=&endDate=`** — Returns employees whose records changed in date range. UKG recommends max 8 calls/day (3-hour intervals).
2. **`dateTimeChanged` filter** — Many endpoints support `?dateTimeChanged={date}` for filtering modified records.
3. **`dateTimeCreated` filter** — Some endpoints support filtering for new records.

### Response Format

JSON arrays of objects. Example (Person Details):
```json
[
  {
    "employeeId": "UUID",
    "companyId": "string",
    "firstName": "string",
    "lastName": "string",
    "emailAddress": "string",
    "dateOfBirth": "YYYY-MM-DD",
    "dateTimeCreated": "ISO 8601",
    "dateTimeChanged": "ISO 8601"
  }
]
```

## Core API Endpoints (Target Tables)

### Personnel (highest priority)
| Endpoint | Description |
|----------|-------------|
| `/personnel/v1/person-details` | Name, address, contact, DOB, national ID |
| `/personnel/v1/employee-employment-details` | Job, status, hire dates, supervisor, pay group |
| `/personnel/v1/compensation-details` | Compensation records |
| `/personnel/v1/employee-changes` | Delta sync endpoint (changes by date range) |
| `/personnel/v1/employee-ids` | Employee UUID listing |
| `/personnel/v1/employee-demographic-details` | Gender, ethnicity, etc. |
| `/personnel/v1/employee-supervisor-details` | Supervisor relationships |
| `/personnel/v1/employee-job-history-details` | Job history |
| `/personnel/v1/pto-plans` | PTO plan details |
| `/personnel/v1/employee-education` | Education records |
| `/personnel/v1/employee-contact` | Emergency contacts |
| `/personnel/v1/employee-direct-deposit` | Direct deposit details |
| `/personnel/v1/employee-deductions` | Employee deductions |

### Configuration / Org Structure
| Endpoint | Description |
|----------|-------------|
| `/configuration/v1/company-details` | Company information |
| `/configuration/v1/locations` | Location configurations |
| `/configuration/v1/org-levels` | Organization levels |
| `/configuration/v1/jobs` | Job configurations |
| `/configuration/v1/positions` | Position information |
| `/configuration/v1/code-tables` | Code table lookups |

### Payroll
| Endpoint | Description |
|----------|-------------|
| `/payroll/v1/earnings-history` | Historical earnings |
| `/payroll/v1/pay-details-report` | Finalized pay (earnings, deductions, taxes) |
| `/payroll/v1/paygroup-pay-periods` | Pay group periods |

### Recruiting (OAuth 2.0 auth, base path: `/{tenantalias}/api/`)

Uses OAuth 2.0 with scopes like `recruiting.domain.applications.read`, etc.
Pagination: `page` + `per_page`. Date filtering: `updated_after` / `updated_before` (ISO 8601).

| Endpoint | Description | Key Response Fields |
|----------|-------------|---------------------|
| `GET /api/applications` | Job applications | id, candidate, opportunity, applied_date, screening_question_responses, recruiting_process, hire_details, updated_at |
| `GET /api/candidates` | Candidate records | id, first_name, last_name, email, phone, skills, work_experience, education, remote_updated_at |
| `GET /api/opportunities` | Job requisitions/postings | id, titles, descriptions, compensation, job info, company, locations, published_date |
| `GET /api/offers` | Offer records | id, candidate, application, job info, approval status, offer actions |
| `GET /api/background-check-order-requests` | Background check orders | order details, candidate reference |
| `GET /api/background-check-results-requests` | Background check results | results, candidate reference |
| `GET /api/recruiting-processes` | Recruiting process definitions (reference) | process ID, stages |
| `GET /api/applicant-sources` | Applicant source definitions (reference) | id, name, archival status |
| `GET /api/job-boards` | Job board definitions (reference) | board details |
| `GET /api/physical-locations` | Physical location definitions (reference) | location details |

### Onboarding (OAuth 2.0 auth, base path: `/talent/onboarding/v2/tenants/{tenantIdentifier}/`)

Uses OAuth 2.0 with `onboarding` scope.
Pagination: `page` + `per_page` (default 30, max 50). Date filtering: `start_date_after` / `start_date_before`.

| Endpoint | Description | Key Filters |
|----------|-------------|-------------|
| `GET /new-hires/in-progress` | New hires currently onboarding | start_date_after/before, onboarding_status (InitialReview, Launched, FinalReview, PendingUpdates), job, work_location, org levels |
| `GET /new-hires/completed` | New hires who finished onboarding | start_date_after/before (required, max 12-month span) |
| `GET /new-hires/canceled` | Canceled onboarding records | start_date_after/before, job, work_location, org levels |
| `GET /new-hires/{id}` | Single new hire by ID | — |

## Available OpenAPI Specs (13 total)

| API Name | Spec ID |
|----------|---------|
| Pro Employee Data | 67ae1981009874000fcea6c9 |
| Configuration Setup | 67ae1970922b49002af71b3e |
| Payroll | 67ae199ddd6b8400580b0f39 |
| Recruiting | 67ae19a6034853000f5abb34 |
| Onboarding | 67ae197b35cecb0052829f92 |
| UTA (Time & Attendance) | 67ae19b133e750002ab6ed8a |
| Audit | 67ae196a74f4ba0011428cae |
| PRO Security | 67ae19974f8d910059c7069b |
| Pro Platform Configuration | 67ae19907de5560036102c8e |
| Pro Import Tools | 67ae198b18fd5c0068b73e59 |
| Learning Courses | 6979260abc26cec7924517a8 |
| Pro HCM Webhook Events | 689dfc3a1e5dfaf25ed49e59 |
| Audit (v2) | 689e4fa91623a92a91c65730 |

## Connector Design Decisions

### Dual Auth Strategy
The connector needs to support **two auth mechanisms**:
1. **Basic Auth** — for core HCM endpoints (personnel, configuration, payroll)
2. **OAuth 2.0** — for recruiting and onboarding endpoints

Both can be initialized in `__init__`; the OAuth token should be lazily fetched and cached/refreshed on 401.

### Required Credentials (connector_spec.yaml)
Additional OAuth credentials needed for recruiting/onboarding:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `client_id` | string | no | OAuth client ID (required for recruiting/onboarding tables) |
| `client_secret` | string (secret) | no | OAuth client secret |
| `tenant_alias` | string | no | Tenant alias for recruiting/onboarding URL paths |

### Required Credentials (connector_spec.yaml)
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `service_host` | string | yes | Per-tenant hostname (e.g., `service5.ultipro.com`) |
| `username` | string | yes | Service account username |
| `password` | string (secret) | yes | Service account password |
| `customer_api_key` | string (secret) | yes | 5-character customer API key |

### Ingestion Types by Table Category
- **Personnel endpoints** (person-details, employment-details, compensation-details): **CDC** using `dateTimeChanged` as cursor field
- **Configuration endpoints** (company-details, locations, org-levels, jobs): **Snapshot** — small reference data, full refresh each run
- **Payroll endpoints** (earnings-history, pay-details-report): **CDC** or **Append** depending on endpoint semantics
- **Recruiting endpoints** (applications, candidates, opportunities, offers): **CDC** using `updated_after`/`updated_at` as cursor
- **Recruiting reference endpoints** (recruiting-processes, applicant-sources, job-boards): **Snapshot** — small reference data
- **Onboarding endpoints** (new-hires in-progress/completed/canceled): **Snapshot** or **CDC** using `start_date_after` filter

### Implementation Patterns to Follow
- **Auth**: Pre-compute Basic Auth header + API key header in `__init__`
- **Retry**: Exponential backoff on 429/500/503 (follow Example connector pattern)
- **Pagination**: Loop through `page` param, stop when response has fewer records than `per_Page`
- **CDC cursor**: Use `dateTimeChanged` field; cap at `_init_time` to prevent chasing
- **Batch control**: Respect `max_records_per_batch` from table_options
- **Schemas**: Hard-coded StructType definitions (UKG API doesn't expose schema metadata)

## Files to Create

| File | Purpose |
|------|---------|
| `sources/ukg/ukg.py` | Main connector class |
| `sources/ukg/ukg_schemas.py` | Hard-coded schemas, metadata, type mappings |
| `sources/ukg/connector_spec.yaml` | Connection parameters and options allowlist |
| `sources/ukg/README.md` | User-facing documentation |
| `tests/unit/sources/ukg/test_ukg_lakeflow_connect.py` | Test class extending LakeflowConnectTests |
| `tests/unit/sources/ukg/configs/dev_config.json` | Test credentials |

## Reference Files
- Interface: `src/databricks/labs/community_connector/interface/lakeflow_connect.py`
- Example connector: `src/databricks/labs/community_connector/sources/example/example.py`
- Zendesk connector: `src/databricks/labs/community_connector/sources/zendesk/zendesk.py`
- Test suite: `tests/unit/sources/test_suite.py`

## API Documentation Links
- Developer hub: https://developer.ukg.com/hcm/reference/welcome-to-the-ukg-pro-api
- OpenAPI specs: https://developer.ukg.com/hcm/openapi
- Rate limiting: https://developer.ukg.com/hcm/docs/api-rate-limiting
- Service account setup: https://developer.ukg.com/hcm/docs/web-service-account
- Changes by date: https://developer.ukg.com/hcm/reference/employeechanges_get_all
- Recruiting OpenAPI spec: https://developer.ukg.com/hcm/openapi/67ae19a6034853000f5abb34
- Onboarding OpenAPI spec: https://developer.ukg.com/hcm/openapi/67ae197b35cecb0052829f92
- Recruiting auth & permissions: https://developer.ukg.com/hcm/docs/onboarding-and-recruiting-authorization-and-permissions
