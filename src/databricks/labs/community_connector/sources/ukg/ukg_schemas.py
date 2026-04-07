# pylint: disable=too-many-lines
from typing import Any, Dict

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Table registry — drives generic read logic in the connector
# ---------------------------------------------------------------------------
# auth: "basic" (core HCM) or "oauth" (recruiting/onboarding)
# ingestion_type: "cdc" | "snapshot" | "append"
# cursor_field: field name in the response used for incremental reads
# cursor_filter_param: query parameter name used to filter by cursor value
# primary_keys: list of fields that uniquely identify a record
# pagination: dict with page_param, per_page_param, max_per_page
# response_key: if the response wraps records in a key, specify it here

TABLE_REGISTRY: Dict[str, Dict[str, Any]] = {
    # -----------------------------------------------------------------------
    # Personnel endpoints (Basic Auth, CDC via dateTimeChanged)
    # -----------------------------------------------------------------------
    "person_details": {
        "endpoint": "/personnel/v1/person-details",
        "auth": "basic",
        "ingestion_type": "cdc",
        "cursor_field": "datetimeChanged",
        "cursor_filter_param": "dateTimeChanged",
        "primary_keys": ["employeeId", "companyId"],
        "pagination": {
            "page_param": "page",
            "per_page_param": "per_Page",
            "max_per_page": 10000,
        },
    },
    "employment_details": {
        "endpoint": "/personnel/v1/employee-employment-details",
        "auth": "basic",
        "ingestion_type": "cdc",
        "cursor_field": "dateTimeChanged",
        "cursor_filter_param": "dateTimeChanged",
        "primary_keys": ["employeeID", "companyID"],
        "pagination": {
            "page_param": "page",
            "per_page_param": "per_Page",
            "max_per_page": 10000,
        },
    },
    "compensation_details": {
        "endpoint": "/personnel/v1/compensation-details",
        "auth": "basic",
        "ingestion_type": "cdc",
        "cursor_field": "dateTimeChanged",
        "cursor_filter_param": "dateTimeChanged",
        "primary_keys": ["employeeId", "companyId"],
        "pagination": {
            "page_param": "page",
            "per_page_param": "per_Page",
            "max_per_page": 10000,
        },
    },
    "employee_demographic_details": {
        "endpoint": "/personnel/v1/employee-demographic-details",
        "auth": "basic",
        "ingestion_type": "cdc",
        "cursor_field": "dateTimeChanged",
        "cursor_filter_param": "dateTimeChanged",
        "primary_keys": ["employeeId", "companyId"],
        "pagination": {
            "page_param": "page",
            "per_page_param": "per_Page",
            "max_per_page": 10000,
        },
    },
    "supervisor_details": {
        "endpoint": "/personnel/v1/employee-supervisor-details",
        "auth": "basic",
        "ingestion_type": "cdc",
        "cursor_field": "dateTimeChanged",
        "cursor_filter_param": "dateTimeChanged",
        "primary_keys": ["employeeId", "companyId"],
        "pagination": {
            "page_param": "page",
            "per_page_param": "per_Page",
            "max_per_page": 10000,
        },
    },
    "job_history_details": {
        "endpoint": "/personnel/v1/employee-job-history-details",
        "auth": "basic",
        "ingestion_type": "cdc",
        "cursor_field": "dateTimeChanged",
        "cursor_filter_param": "dateTimeChanged",
        "primary_keys": ["employeeId", "companyId"],
        "pagination": {
            "page_param": "page",
            "per_page_param": "per_Page",
            "max_per_page": 10000,
        },
    },
    "pto_plans": {
        "endpoint": "/personnel/v1/pto-plans",
        "auth": "basic",
        "ingestion_type": "cdc",
        "cursor_field": "dateTimeChanged",
        "cursor_filter_param": "dateTimeChanged",
        "primary_keys": ["employeeId", "companyId", "planName"],
        "pagination": {
            "page_param": "page",
            "per_page_param": "per_Page",
            "max_per_page": 10000,
        },
    },
    "employee_education": {
        "endpoint": "/personnel/v1/employee-education",
        "auth": "basic",
        "ingestion_type": "cdc",
        "cursor_field": "dateTimeChanged",
        "cursor_filter_param": "dateTimeChanged",
        "primary_keys": ["employeeId", "companyId"],
        "pagination": {
            "page_param": "page",
            "per_page_param": "per_Page",
            "max_per_page": 10000,
        },
    },
    "employee_contact": {
        "endpoint": "/personnel/v1/employee-contact",
        "auth": "basic",
        "ingestion_type": "cdc",
        "cursor_field": "dateTimeChanged",
        "cursor_filter_param": "dateTimeChanged",
        "primary_keys": ["employeeId", "companyId"],
        "pagination": {
            "page_param": "page",
            "per_page_param": "per_Page",
            "max_per_page": 10000,
        },
    },
    "employee_deductions": {
        "endpoint": "/personnel/v1/employee-deductions",
        "auth": "basic",
        "ingestion_type": "cdc",
        "cursor_field": "dateTimeChanged",
        "cursor_filter_param": "dateTimeChanged",
        "primary_keys": ["employeeId", "companyId"],
        "pagination": {
            "page_param": "page",
            "per_page_param": "per_Page",
            "max_per_page": 10000,
        },
    },
    # -----------------------------------------------------------------------
    # Configuration / Org Structure (Basic Auth, Snapshot)
    # -----------------------------------------------------------------------
    "company_details": {
        "endpoint": "/configuration/v1/company-details",
        "auth": "basic",
        "ingestion_type": "snapshot",
        "cursor_field": None,
        "cursor_filter_param": None,
        "primary_keys": ["companyId"],
        "pagination": {
            "page_param": "page",
            "per_page_param": "per_Page",
            "max_per_page": 10000,
        },
    },
    "locations": {
        "endpoint": "/configuration/v1/locations",
        "auth": "basic",
        "ingestion_type": "snapshot",
        "cursor_field": None,
        "cursor_filter_param": None,
        "primary_keys": ["locationCode"],
        "pagination": {
            "page_param": "page",
            "per_page_param": "per_Page",
            "max_per_page": 10000,
        },
    },
    "org_levels": {
        "endpoint": "/configuration/v1/org-levels",
        "auth": "basic",
        "ingestion_type": "snapshot",
        "cursor_field": None,
        "cursor_filter_param": None,
        "primary_keys": ["code", "level"],
        "pagination": {
            "page_param": "page",
            "per_page_param": "per_Page",
            "max_per_page": 10000,
        },
    },
    "jobs": {
        "endpoint": "/configuration/v1/jobs",
        "auth": "basic",
        "ingestion_type": "snapshot",
        "cursor_field": None,
        "cursor_filter_param": None,
        "primary_keys": ["jobCode"],
        "pagination": {
            "page_param": "page",
            "per_page_param": "per_Page",
            "max_per_page": 10000,
        },
    },
    # -----------------------------------------------------------------------
    # Payroll (Basic Auth, CDC)
    # -----------------------------------------------------------------------
    "earnings_history": {
        "endpoint": "/payroll/v1/earnings-history",
        "auth": "basic",
        "ingestion_type": "cdc",
        "cursor_field": "dateTimeChanged",
        "cursor_filter_param": "dateTimeChanged",
        "primary_keys": ["employeeId", "companyId"],
        "pagination": {
            "page_param": "page",
            "per_page_param": "per_Page",
            "max_per_page": 10000,
        },
    },
    # -----------------------------------------------------------------------
    # Recruiting (OAuth, CDC via updated_after)
    # -----------------------------------------------------------------------
    "applications": {
        "endpoint": "/api/applications",
        "auth": "oauth",
        "ingestion_type": "cdc",
        "cursor_field": "updated_at",
        "cursor_filter_param": "updated_after",
        "primary_keys": ["id"],
        "pagination": {
            "page_param": "page",
            "per_page_param": "per_page",
            "max_per_page": 100,
        },
    },
    "candidates": {
        "endpoint": "/api/candidates",
        "auth": "oauth",
        "ingestion_type": "cdc",
        "cursor_field": "updated_at",
        "cursor_filter_param": "updated_after",
        "primary_keys": ["id"],
        "pagination": {
            "page_param": "page",
            "per_page_param": "per_page",
            "max_per_page": 100,
        },
    },
    "opportunities": {
        "endpoint": "/api/opportunities",
        "auth": "oauth",
        "ingestion_type": "cdc",
        "cursor_field": "updated_at",
        "cursor_filter_param": "updated_after",
        "primary_keys": ["id"],
        "pagination": {
            "page_param": "page",
            "per_page_param": "per_page",
            "max_per_page": 100,
        },
    },
    "offers": {
        "endpoint": "/api/offers",
        "auth": "oauth",
        "ingestion_type": "cdc",
        "cursor_field": "updated_at",
        "cursor_filter_param": "updated_after",
        "primary_keys": ["id"],
        "pagination": {
            "page_param": "page",
            "per_page_param": "per_page",
            "max_per_page": 100,
        },
    },
    # Recruiting reference data (OAuth, Snapshot)
    "recruiting_processes": {
        "endpoint": "/api/recruiting-processes",
        "auth": "oauth",
        "ingestion_type": "snapshot",
        "cursor_field": None,
        "cursor_filter_param": None,
        "primary_keys": ["id"],
        "pagination": {
            "page_param": "page",
            "per_page_param": "per_page",
            "max_per_page": 100,
        },
    },
    "applicant_sources": {
        "endpoint": "/api/applicant-sources",
        "auth": "oauth",
        "ingestion_type": "snapshot",
        "cursor_field": None,
        "cursor_filter_param": None,
        "primary_keys": ["id"],
        "pagination": {
            "page_param": "page",
            "per_page_param": "per_page",
            "max_per_page": 100,
        },
    },
    "job_boards": {
        "endpoint": "/api/job-boards",
        "auth": "oauth",
        "ingestion_type": "snapshot",
        "cursor_field": None,
        "cursor_filter_param": None,
        "primary_keys": ["id"],
        "pagination": {
            "page_param": "page",
            "per_page_param": "per_page",
            "max_per_page": 100,
        },
    },
    # -----------------------------------------------------------------------
    # Onboarding (OAuth, Snapshot — status-segmented endpoints)
    # -----------------------------------------------------------------------
    "new_hires_in_progress": {
        "endpoint": "/talent/onboarding/v2/tenants/{tenant_identifier}/new-hires/in-progress",
        "auth": "oauth",
        "ingestion_type": "snapshot",
        "cursor_field": None,
        "cursor_filter_param": None,
        "primary_keys": ["id"],
        "pagination": {
            "page_param": "page",
            "per_page_param": "per_page",
            "max_per_page": 50,
        },
    },
    "new_hires_completed": {
        "endpoint": "/talent/onboarding/v2/tenants/{tenant_identifier}/new-hires/completed",
        "auth": "oauth",
        "ingestion_type": "snapshot",
        "cursor_field": None,
        "cursor_filter_param": None,
        "primary_keys": ["id"],
        "pagination": {
            "page_param": "page",
            "per_page_param": "per_page",
            "max_per_page": 50,
        },
    },
    "new_hires_canceled": {
        "endpoint": "/talent/onboarding/v2/tenants/{tenant_identifier}/new-hires/canceled",
        "auth": "oauth",
        "ingestion_type": "snapshot",
        "cursor_field": None,
        "cursor_filter_param": None,
        "primary_keys": ["id"],
        "pagination": {
            "page_param": "page",
            "per_page_param": "per_page",
            "max_per_page": 50,
        },
    },
}


# ---------------------------------------------------------------------------
# Table metadata — returned by read_table_metadata()
# ---------------------------------------------------------------------------

TABLE_METADATA: Dict[str, Dict[str, Any]] = {
    name: {
        "primary_keys": config["primary_keys"],
        "cursor_field": config["cursor_field"],
        "ingestion_type": config["ingestion_type"],
    }
    for name, config in TABLE_REGISTRY.items()
}


# ---------------------------------------------------------------------------
# Spark schemas — returned by get_table_schema()
# ---------------------------------------------------------------------------

TABLE_SCHEMAS: Dict[str, StructType] = {
    # -------------------------------------------------------------------
    # person_details
    # -------------------------------------------------------------------
    "person_details": StructType([
        StructField("employeeId", StringType(), False),
        StructField("companyId", StringType(), False),
        StructField("companyName", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("lastName", StringType(), True),
        StructField("middleName", StringType(), True),
        StructField("preferredName", StringType(), True),
        StructField("formerName", StringType(), True),
        StructField("namePrefixCode", StringType(), True),
        StructField("nameSufixCode", StringType(), True),
        StructField("emailAddress", StringType(), True),
        StructField("emailAddressAlternate", StringType(), True),
        StructField("dateOfBirth", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("maritalStatusCode", StringType(), True),
        StructField("addressLine1", StringType(), True),
        StructField("addressLine2", StringType(), True),
        StructField("addressCity", StringType(), True),
        StructField("addressState", StringType(), True),
        StructField("addressZipCode", StringType(), True),
        StructField("addressCountry", StringType(), True),
        StructField("addressCounty", StringType(), True),
        StructField("homePhone", StringType(), True),
        StructField("homePhoneCountry", StringType(), True),
        StructField("homePhoneIsPrivate", BooleanType(), True),
        StructField("nationalId", StringType(), True),
        StructField("nationalIdCountry", StringType(), True),
        StructField("ssn", StringType(), True),
        StructField("ssnIsSuppressed", BooleanType(), True),
        StructField("ethnicDescription", StringType(), True),
        StructField("ethnicIDCode", StringType(), True),
        StructField("isDisabled", BooleanType(), True),
        StructField("disabilityType", StringType(), True),
        StructField("militaryService", BooleanType(), True),
        StructField("militaryBranchServed", StringType(), True),
        StructField("militaryEra", StringType(), True),
        StructField("militarySeparationDate", StringType(), True),
        StructField("isSmoker", BooleanType(), True),
        StructField("isMultiPayGroup", BooleanType(), True),
        StructField("userName", StringType(), True),
        StructField("personId", StringType(), True),
        StructField("consentElectronicW2", BooleanType(), True),
        StructField("cobraIsActive", BooleanType(), True),
        StructField("cobraStatus", StringType(), True),
        StructField("cobraStatusDate", StringType(), True),
        StructField("cobraReason", StringType(), True),
        StructField("datetimeChanged", StringType(), True),
        StructField("datetimeCreated", StringType(), True),
    ]),
    # -------------------------------------------------------------------
    # employment_details
    # -------------------------------------------------------------------
    "employment_details": StructType([
        StructField("employeeID", StringType(), False),
        StructField("companyID", StringType(), False),
        StructField("companyName", StringType(), True),
        StructField("employeeNumber", StringType(), True),
        StructField("employeeStatusCode", StringType(), True),
        StructField("employeeTypeCode", StringType(), True),
        StructField("fullTimeOrPartTimeCode", StringType(), True),
        StructField("primaryJobCode", StringType(), True),
        StructField("jobTitle", StringType(), True),
        StructField("primaryWorkLocationCode", StringType(), True),
        StructField("primaryProjectCode", StringType(), True),
        StructField("deductionGroupCode", StringType(), True),
        StructField("earningGroupCode", StringType(), True),
        StructField("payGroup", StringType(), True),
        StructField("payGroupDescription", StringType(), True),
        StructField("supervisorId", StringType(), True),
        StructField("supervisorFirstName", StringType(), True),
        StructField("supervisorLastName", StringType(), True),
        StructField("originalHireDate", StringType(), True),
        StructField("lastHireDate", StringType(), True),
        StructField("dateOfTermination", StringType(), True),
        StructField("dateOfRetirement", StringType(), True),
        StructField("termReason", StringType(), True),
        StructField("termType", StringType(), True),
        StructField("isHomeCompany", BooleanType(), True),
        StructField("isAutoAllocated", BooleanType(), True),
        StructField("isAutopaid", BooleanType(), True),
        StructField("salaryOrHourly", StringType(), True),
        StructField("orgLevel1Code", StringType(), True),
        StructField("orgLevel2Code", StringType(), True),
        StructField("orgLevel3Code", StringType(), True),
        StructField("orgLevel4Code", StringType(), True),
        StructField("positionCode", StringType(), True),
        StructField("shift", StringType(), True),
        StructField("dateTimeChanged", StringType(), True),
        StructField("dateTimeCreated", StringType(), True),
    ]),
    # -------------------------------------------------------------------
    # compensation_details
    # -------------------------------------------------------------------
    "compensation_details": StructType([
        StructField("employeeId", StringType(), False),
        StructField("companyId", StringType(), False),
        StructField("annualSalary", DoubleType(), True),
        StructField("hourlyPayRate", DoubleType(), True),
        StructField("weeklyPayRate", DoubleType(), True),
        StructField("payPeriodRate", DoubleType(), True),
        StructField("currencyCode", StringType(), True),
        StructField("salaryOrHourly", StringType(), True),
        StructField("salaryOrHourlyCode", StringType(), True),
        StructField("payGroupCode", StringType(), True),
        StructField("payGroupDescription", StringType(), True),
        StructField("payPeriod", StringType(), True),
        StructField("numberOfPayments", LongType(), True),
        StructField("scheduledAnnualHrs", DoubleType(), True),
        StructField("scheduledFullTimeEquivalency", DoubleType(), True),
        StructField("jobSalaryGrade", StringType(), True),
        StructField("payScaleCode", StringType(), True),
        StructField("primaryShiftCode", StringType(), True),
        StructField("isHighlyCompensated", BooleanType(), True),
        StructField("isSeasonalWorker", BooleanType(), True),
        StructField("performanceReviewRating", StringType(), True),
        StructField("performanceReviewType", StringType(), True),
        StructField("dateOfLastSalaryReview", StringType(), True),
        StructField("dateOfNextSalaryReview", StringType(), True),
        StructField("dateTimeChanged", StringType(), True),
        StructField("dateTimeCreated", StringType(), True),
    ]),
    # -------------------------------------------------------------------
    # employee_demographic_details
    # -------------------------------------------------------------------
    "employee_demographic_details": StructType([
        StructField("employeeId", StringType(), False),
        StructField("companyId", StringType(), False),
        StructField("gender", StringType(), True),
        StructField("ethnicDescription", StringType(), True),
        StructField("ethnicIDCode", StringType(), True),
        StructField("dateOfBirth", StringType(), True),
        StructField("isDisabled", BooleanType(), True),
        StructField("disabilityType", StringType(), True),
        StructField("militaryService", BooleanType(), True),
        StructField("dateTimeChanged", StringType(), True),
        StructField("dateTimeCreated", StringType(), True),
    ]),
    # -------------------------------------------------------------------
    # supervisor_details
    # -------------------------------------------------------------------
    "supervisor_details": StructType([
        StructField("employeeId", StringType(), False),
        StructField("companyId", StringType(), False),
        StructField("supervisorId", StringType(), True),
        StructField("supervisorCompanyCode", StringType(), True),
        StructField("supervisorFirstName", StringType(), True),
        StructField("supervisorLastName", StringType(), True),
        StructField("supervisorEmployeeNumber", StringType(), True),
        StructField("dateTimeChanged", StringType(), True),
        StructField("dateTimeCreated", StringType(), True),
    ]),
    # -------------------------------------------------------------------
    # job_history_details
    # -------------------------------------------------------------------
    "job_history_details": StructType([
        StructField("employeeId", StringType(), False),
        StructField("companyId", StringType(), False),
        StructField("jobTitle", StringType(), True),
        StructField("jobCode", StringType(), True),
        StructField("jobGroupCode", StringType(), True),
        StructField("effectiveDate", StringType(), True),
        StructField("jobChangeReasonCode", StringType(), True),
        StructField("dateTimeChanged", StringType(), True),
        StructField("dateTimeCreated", StringType(), True),
    ]),
    # -------------------------------------------------------------------
    # pto_plans
    # -------------------------------------------------------------------
    "pto_plans": StructType([
        StructField("employeeId", StringType(), False),
        StructField("companyId", StringType(), False),
        StructField("planName", StringType(), False),
        StructField("planYearStartDate", StringType(), True),
        StructField("planYearEndDate", StringType(), True),
        StructField("hoursAccrued", DoubleType(), True),
        StructField("hoursUsed", DoubleType(), True),
        StructField("hoursAvailable", DoubleType(), True),
        StructField("hoursCarriedOver", DoubleType(), True),
        StructField("dateTimeChanged", StringType(), True),
        StructField("dateTimeCreated", StringType(), True),
    ]),
    # -------------------------------------------------------------------
    # employee_education
    # -------------------------------------------------------------------
    "employee_education": StructType([
        StructField("employeeId", StringType(), False),
        StructField("companyId", StringType(), False),
        StructField("schoolName", StringType(), True),
        StructField("degree", StringType(), True),
        StructField("major", StringType(), True),
        StructField("graduationDate", StringType(), True),
        StructField("gpa", StringType(), True),
        StructField("dateTimeChanged", StringType(), True),
        StructField("dateTimeCreated", StringType(), True),
    ]),
    # -------------------------------------------------------------------
    # employee_contact
    # -------------------------------------------------------------------
    "employee_contact": StructType([
        StructField("employeeId", StringType(), False),
        StructField("companyId", StringType(), False),
        StructField("contactName", StringType(), True),
        StructField("contactRelationship", StringType(), True),
        StructField("contactPhone", StringType(), True),
        StructField("contactAddress", StringType(), True),
        StructField("isPrimaryContact", BooleanType(), True),
        StructField("dateTimeChanged", StringType(), True),
        StructField("dateTimeCreated", StringType(), True),
    ]),
    # -------------------------------------------------------------------
    # employee_deductions
    # -------------------------------------------------------------------
    "employee_deductions": StructType([
        StructField("employeeId", StringType(), False),
        StructField("companyId", StringType(), False),
        StructField("deductionCode", StringType(), True),
        StructField("deductionDescription", StringType(), True),
        StructField("deductionAmount", DoubleType(), True),
        StructField("deductionGoal", DoubleType(), True),
        StructField("deductionTaken", DoubleType(), True),
        StructField("effectiveDate", StringType(), True),
        StructField("status", StringType(), True),
        StructField("dateTimeChanged", StringType(), True),
        StructField("dateTimeCreated", StringType(), True),
    ]),
    # -------------------------------------------------------------------
    # company_details
    # -------------------------------------------------------------------
    "company_details": StructType([
        StructField("companyId", StringType(), False),
        StructField("companyCode", StringType(), True),
        StructField("companyName", StringType(), True),
        StructField("masterCompanyId", StringType(), True),
        StructField("isMasterCompany", BooleanType(), True),
        StructField("addressLine1", StringType(), True),
        StructField("addressLine2", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zipCode", StringType(), True),
        StructField("country", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("fein", StringType(), True),
    ]),
    # -------------------------------------------------------------------
    # locations
    # -------------------------------------------------------------------
    "locations": StructType([
        StructField("locationCode", StringType(), False),
        StructField("locationDescription", StringType(), True),
        StructField("addressLine1", StringType(), True),
        StructField("addressLine2", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zipCode", StringType(), True),
        StructField("countryCode", StringType(), True),
        StructField("isActive", BooleanType(), True),
    ]),
    # -------------------------------------------------------------------
    # org_levels
    # -------------------------------------------------------------------
    "org_levels": StructType([
        StructField("code", StringType(), False),
        StructField("level", LongType(), False),
        StructField("description", StringType(), True),
        StructField("levelDescription", StringType(), True),
        StructField("budgetGroup", StringType(), True),
        StructField("reportingCategory", StringType(), True),
        StructField("isActive", BooleanType(), True),
    ]),
    # -------------------------------------------------------------------
    # jobs
    # -------------------------------------------------------------------
    "jobs": StructType([
        StructField("jobCode", StringType(), False),
        StructField("jobDescription", StringType(), True),
        StructField("jobTitle", StringType(), True),
        StructField("jobGroupCode", StringType(), True),
        StructField("salaryGrade", StringType(), True),
        StructField("eeoCode", StringType(), True),
        StructField("flsaCode", StringType(), True),
        StructField("isActive", BooleanType(), True),
    ]),
    # -------------------------------------------------------------------
    # earnings_history
    # -------------------------------------------------------------------
    "earnings_history": StructType([
        StructField("employeeId", StringType(), False),
        StructField("companyId", StringType(), False),
        StructField("earningCode", StringType(), True),
        StructField("earningDescription", StringType(), True),
        StructField("earningAmount", DoubleType(), True),
        StructField("earningHours", DoubleType(), True),
        StructField("payDate", StringType(), True),
        StructField("payPeriodStartDate", StringType(), True),
        StructField("payPeriodEndDate", StringType(), True),
        StructField("dateTimeChanged", StringType(), True),
        StructField("dateTimeCreated", StringType(), True),
    ]),
    # -------------------------------------------------------------------
    # Recruiting tables
    # -------------------------------------------------------------------
    "applications": StructType([
        StructField("id", StringType(), False),
        StructField("updated_at", StringType(), True),
        StructField("applied_date", StringType(), True),
        StructField("candidate_id", StringType(), True),
        StructField("candidate_first_name", StringType(), True),
        StructField("candidate_last_name", StringType(), True),
        StructField("opportunity_id", StringType(), True),
        StructField("opportunity_title", StringType(), True),
        StructField("applicant_source", StringType(), True),
        StructField("job_board", StringType(), True),
        StructField("is_processed", BooleanType(), True),
        StructField("creation_method", StringType(), True),
        StructField("available_start_date", StringType(), True),
    ]),
    "candidates": StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("title", StringType(), True),
        StructField("company", StringType(), True),
        StructField("email_address", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("created_at", StringType(), True),
    ]),
    "opportunities": StructType([
        StructField("id", StringType(), False),
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
        StructField("status", StringType(), True),
        StructField("company_name", StringType(), True),
        StructField("location", StringType(), True),
        StructField("job_category", StringType(), True),
        StructField("published_date", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("created_at", StringType(), True),
    ]),
    "offers": StructType([
        StructField("id", StringType(), False),
        StructField("candidate_id", StringType(), True),
        StructField("application_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("offer_date", StringType(), True),
        StructField("start_date", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("created_at", StringType(), True),
    ]),
    "recruiting_processes": StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
    ]),
    "applicant_sources": StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("is_archived", BooleanType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]),
    "job_boards": StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
    ]),
    # -------------------------------------------------------------------
    # Onboarding tables
    # -------------------------------------------------------------------
    "new_hires_in_progress": StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("start_date", StringType(), True),
        StructField("job", StringType(), True),
        StructField("work_location", StringType(), True),
        StructField("component_company", StringType(), True),
        StructField("onboarding_status", StringType(), True),
        StructField("onboarding_owner", StringType(), True),
    ]),
    "new_hires_completed": StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("start_date", StringType(), True),
        StructField("job", StringType(), True),
        StructField("work_location", StringType(), True),
        StructField("component_company", StringType(), True),
        StructField("completion_date", StringType(), True),
    ]),
    "new_hires_canceled": StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("start_date", StringType(), True),
        StructField("job", StringType(), True),
        StructField("work_location", StringType(), True),
        StructField("component_company", StringType(), True),
        StructField("cancellation_date", StringType(), True),
    ]),
}
