# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e2be6953-6183-4278-81e3-afa9dfe88c6d",
# META       "default_lakehouse_name": "lh_fhir_engineering",
# META       "default_lakehouse_workspace_id": "8edaab9a-2151-418e-9fef-63e0921486fc",
# META       "known_lakehouses": [
# META         {
# META           "id": "e2be6953-6183-4278-81e3-afa9dfe88c6d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************


# ==============================================================
# SYNAPX FHIR DATA ENGINEERING ASSESSMENT
# NOTEBOOK: nb_01_fhir_api_ingestion
# PLATFORM: Microsoft Fabric - PySpark
# LAYER: 01 Ingestion / Raw
# ==============================================================

# ==============================================================
# 1. IMPORTS
# ==============================================================

import requests
import json
import time
import uuid
import logging

from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, urljoin, parse_qs

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ==============================================================
# 2. CONFIGURATION
# ==============================================================

BASE_URL = "https://hapi.fhir.org/baseR4"

RESOURCES = [
    "Patient",
    "Encounter",
    "Observation",
    "Condition"
]

LOOKBACK_HOURS = 72

PAGE_SIZE = 100

MAX_PAGES_PER_RESOURCE = 10000

REQUEST_TIMEOUT = 120

REQUEST_DELAY_SECONDS = 0.2

RAW_ROOT = "Files/raw/fhir"

RUN_ID = str(uuid.uuid4())

RUN_STARTED_AT = datetime.now(timezone.utc)

RUN_STARTED_AT_ISO = RUN_STARTED_AT.isoformat()

# Fixed extraction boundary for this run.
# All resources use the same lower-bound timestamp.

LAST_UPDATED_FROM = (
    RUN_STARTED_AT - timedelta(hours=LOOKBACK_HOURS)
).strftime("%Y-%m-%dT%H:%M:%SZ")


# ==============================================================
# 3. LOGGING
# ==============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("FHIR_INGESTION")

logger.info("Starting FHIR ingestion")
logger.info(f"Run ID: {RUN_ID}")
logger.info(f"Lookback: {LOOKBACK_HOURS} hours")
logger.info(f"Last updated from: {LAST_UPDATED_FROM}")


# ==============================================================
# 4. FHIR API SESSION AND RETRY CONFIGURATION
# ==============================================================

session = requests.Session()

retry_strategy = Retry(
    total=5,
    connect=5,
    read=5,
    status=5,
    backoff_factor=2,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
    respect_retry_after_header=True
)

adapter = HTTPAdapter(
    max_retries=retry_strategy,
    pool_connections=10,
    pool_maxsize=10
)

session.mount("https://", adapter)

session.headers.update({
    "Accept": "application/fhir+json",
    "User-Agent": "Synapx-FHIR-Engineering-Assessment/1.0"
})


# ==============================================================
# 5. URL VALIDATION
# ==============================================================

BASE_PARSED = urlparse(BASE_URL)

ALLOWED_HOST = BASE_PARSED.hostname

EXPECTED_BASE_PATH = BASE_PARSED.path.rstrip("/")


def validate_url(url):

    parsed = urlparse(url)

    if parsed.scheme.lower() != "https":
        raise ValueError(
            f"Only HTTPS URLs are permitted: {url}"
        )

    if parsed.hostname != ALLOWED_HOST:
        raise ValueError(
            f"Unexpected API host: {parsed.hostname}"
        )

    if parsed.port not in (None, 443):
        raise ValueError(
            f"Unexpected API port: {parsed.port}"
        )

    if parsed.username or parsed.password:
        raise ValueError(
            "Credentials in URLs are not permitted"
        )

    if (
        parsed.path != EXPECTED_BASE_PATH
        and not parsed.path.startswith(
            EXPECTED_BASE_PATH + "/"
        )
    ):
        raise ValueError(
            f"Unexpected FHIR API path: {parsed.path}"
        )

    return True


# ==============================================================
# 6. LAKEHOUSE FILE UTILITIES
# ==============================================================

def create_directory(path):

    result = notebookutils.fs.mkdirs(path)

    if not notebookutils.fs.exists(path):
        raise RuntimeError(
            f"Unable to create Lakehouse directory: {path}"
        )

    return True


def save_text(path, content):

    parent_path = path.rsplit("/", 1)[0]

    create_directory(parent_path)

    result = notebookutils.fs.put(
        path,
        content,
        True
    )

    if not notebookutils.fs.exists(path):
        raise RuntimeError(
            f"File was not created: {path}"
        )

    logger.info(f"Saved file: {path}")

    return True


def save_json(path, payload):

    content = json.dumps(
        payload,
        indent=2,
        ensure_ascii=False,
        default=str
    )

    return save_text(path, content)


# ==============================================================
# 7. FHIR BUNDLE UTILITIES
# ==============================================================

def get_next_link(bundle):

    links = bundle.get("link", [])

    for link in links:

        if link.get("relation") == "next":

            next_url = link.get("url")

            if next_url:
                return next_url

    return None


def validate_bundle(bundle, resource):

    if not isinstance(bundle, dict):
        raise ValueError(
            f"Invalid JSON response for {resource}"
        )

    if bundle.get("resourceType") != "Bundle":
        raise ValueError(
            f"Expected Bundle for {resource}, "
            f"received {bundle.get('resourceType')}"
        )

    if bundle.get("entry") is None:
        bundle["entry"] = []

    if not isinstance(bundle["entry"], list):
        raise ValueError(
            f"Invalid Bundle.entry for {resource}"
        )

    for entry in bundle["entry"]:

        resource_data = entry.get("resource", {})

        actual_type = resource_data.get("resourceType")

        if actual_type and actual_type != resource:
            raise ValueError(
                f"Unexpected resource type: {actual_type}"
            )

    return bundle


# ==============================================================
# 8. API REQUEST FUNCTION
# ==============================================================

def fetch_page(url, initial_params=None):

    validate_url(url)

    api_call_timestamp = datetime.now(
        timezone.utc
    ).isoformat()

    response = session.get(
        url,
        params=initial_params,
        timeout=REQUEST_TIMEOUT
    )

    response.raise_for_status()

    raw_response = response.text

    try:
        bundle = json.loads(raw_response)

    except json.JSONDecodeError as error:
        raise ValueError(
            f"FHIR API returned invalid JSON: {error}"
        )

    # Validate the final URL as well.
    validate_url(response.url)

    return {
        "response": response,
        "bundle": bundle,
        "raw_response": raw_response,
        "api_call_timestamp": api_call_timestamp,
        "request_url": response.url
    }


# ==============================================================
# 9. RESOURCE INGESTION FUNCTION
# ==============================================================

def ingest_resource(resource):

    logger.info("=" * 60)
    logger.info(f"Starting resource ingestion: {resource}")
    logger.info("=" * 60)

    extraction_timestamp = datetime.now(
        timezone.utc
    ).isoformat()

    extraction_date = datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%d")

    resource_run_path = (
        f"{RAW_ROOT}/{resource.lower()}/"
        f"extraction_date={extraction_date}/"
        f"run_id={RUN_ID}"
    )

    initial_params = {
        "_lastUpdated": f"ge{LAST_UPDATED_FROM}",
        "_count": PAGE_SIZE
    }

    current_url = f"{BASE_URL}/{resource}"

    page_number = 0

    total_entries = 0

    saved_files = []

    page_metadata = []

    resource_status = "SUCCESS"

    resource_error = None

    try:

        create_directory(resource_run_path)

        while current_url:

            page_number += 1

            if page_number > MAX_PAGES_PER_RESOURCE:
                raise RuntimeError(
                    f"Maximum page limit exceeded for {resource}"
                )

            # Only the first request uses the search parameters.
            # Subsequent requests follow the server-provided next URL.

            request_params = (
                initial_params if page_number == 1 else None
            )

            page_result = fetch_page(
                current_url,
                request_params
            )

            response = page_result["response"]

            bundle = validate_bundle(
                page_result["bundle"],
                resource
            )

            raw_response = page_result["raw_response"]

            entries = bundle.get("entry", [])

            entries_count = len(entries)

            total_entries += entries_count

            # --------------------------------------------------
            # 9.1 SAVE ORIGINAL JSON RESPONSE
            # --------------------------------------------------

            raw_file_name = (
                f"page_{page_number:05d}.json"
            )

            raw_file_path = (
                f"{resource_run_path}/{raw_file_name}"
            )

            # Save original response text, not re-serialized JSON.
            save_text(
                raw_file_path,
                raw_response
            )

            saved_timestamp = datetime.now(
                timezone.utc
            ).isoformat()

            # --------------------------------------------------
            # 9.2 SAVE PAGE METADATA
            # --------------------------------------------------

            metadata = {
                "run_id": RUN_ID,
                "resource": resource,
                "page_number": page_number,

                "extraction_timestamp": extraction_timestamp,

                "api_call_timestamp": (
                    page_result["api_call_timestamp"]
                ),

                "data_saved_timestamp": saved_timestamp,

                "api_url_or_params": (
                    page_result["request_url"]
                ),

                "request_params": (
                    request_params or {}
                ),

                "raw_file_path": raw_file_path,

                "entries_returned": entries_count,

                "response_status": response.status_code,

                "fhir_bundle_total": bundle.get("total"),

                "fhir_bundle_type": bundle.get("type"),

                "fhir_bundle_id": bundle.get("id")
            }

            metadata_file_path = (
                f"{resource_run_path}/"
                f"page_{page_number:05d}.metadata.json"
            )

            save_json(
                metadata_file_path,
                metadata
            )

            saved_files.append(raw_file_path)

            page_metadata.append(metadata)

            logger.info(
                f"{resource} | "
                f"Page {page_number} | "
                f"Entries {entries_count} | "
                f"HTTP {response.status_code}"
            )

            # --------------------------------------------------
            # 9.3 GET NEXT PAGE
            # --------------------------------------------------

            next_link = get_next_link(bundle)

            if next_link:

                # Resolve relative URLs against the response URL.
                next_url = urljoin(
                    page_result["request_url"],
                    next_link
                )

                validate_url(next_url)

                current_url = next_url

                time.sleep(REQUEST_DELAY_SECONDS)

            else:

                current_url = None

        # ------------------------------------------------------
        # 9.4 RESOURCE MANIFEST
        # ------------------------------------------------------

        completed_timestamp = datetime.now(
            timezone.utc
        ).isoformat()

        manifest = {
            "run_id": RUN_ID,
            "resource": resource,
            "status": resource_status,

            "run_started_at": extraction_timestamp,

            "run_completed_at": completed_timestamp,

            "lookback_hours": LOOKBACK_HOURS,

            "last_updated_from": LAST_UPDATED_FROM,

            "pages_processed": page_number,

            "total_entries_received": total_entries,

            "files_saved": saved_files,

            "page_metadata": page_metadata
        }

        save_json(
            f"{resource_run_path}/manifest.json",
            manifest
        )

        logger.info(
            f"Completed {resource}: "
            f"{page_number} pages, "
            f"{total_entries} entries"
        )

        return manifest

    except Exception as error:

        resource_status = "FAILED"

        resource_error = str(error)

        failure_manifest = {
            "run_id": RUN_ID,
            "resource": resource,
            "status": resource_status,
            "error": resource_error,
            "pages_processed": page_number,
            "entries_received": total_entries,
            "files_saved": saved_files,
            "failed_at": datetime.now(
                timezone.utc
            ).isoformat()
        }

        try:

            save_json(
                f"{resource_run_path}/failure_manifest.json",
                failure_manifest
            )

        except Exception as manifest_error:

            logger.error(
                f"Could not save failure manifest: {manifest_error}"
            )

        logger.exception(
            f"Ingestion failed for {resource}"
        )

        raise


# ==============================================================
# 10. EXECUTE ALL RESOURCES IN REQUIRED ORDER
# ==============================================================

results = []

overall_status = "SUCCESS"

try:

    for resource in RESOURCES:

        result = ingest_resource(resource)

        results.append(result)

except Exception:

    overall_status = "FAILED"

    raise


# ==============================================================
# 11. SAVE OVERALL RUN SUMMARY
# ==============================================================

RUN_COMPLETED_AT = datetime.now(
    timezone.utc
).isoformat()

summary = {
    "run_id": RUN_ID,

    "run_started_at": RUN_STARTED_AT_ISO,

    "run_completed_at": RUN_COMPLETED_AT,

    "base_url": BASE_URL,

    "lookback_hours": LOOKBACK_HOURS,

    "last_updated_from": LAST_UPDATED_FROM,

    "resource_execution_order": RESOURCES,

    "resources": results,

    "status": overall_status
}

save_json(
    f"{RAW_ROOT}/run_summaries/{RUN_ID}.json",
    summary
)


# ==============================================================
# 12. FINAL EXECUTION REPORT
# ==============================================================

print("\n" + "=" * 70)
print("FHIR RAW INGESTION EXECUTION SUMMARY")
print("=" * 70)

print(json.dumps(summary, indent=2))

print("\nRun ID:", RUN_ID)

print("Status:", overall_status)

print("Raw Lakehouse path:", RAW_ROOT)

print("\nFHIR ingestion notebook execution completed.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
