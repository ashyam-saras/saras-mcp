# server.py

import os  # Import os module for environment variables

from google.api_core.exceptions import GoogleAPIError, NotFound
from google.cloud import bigquery  # Import BigQuery client
from google.oauth2 import service_account
from mcp.server.fastmcp import FastMCP

# Create an MCP server
mcp = FastMCP("bigquery")

# Get service account path from environment variable if available
DEFAULT_SERVICE_ACCOUNT_PATH = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
DEFAULT_PROJECT_ID = os.environ.get("GOOGLE_PROJECT_ID", "insightsprod")


# Add BigQuery execution tool
@mcp.tool()
def execute_bigquery(query: str, service_account_path: str = DEFAULT_SERVICE_ACCOUNT_PATH) -> dict:
    """Execute a custom BigQuery SQL query and return the results as structured data.

    This tool allows you to run arbitrary SQL queries against Google BigQuery. It handles
    authentication, execution, and result formatting, returning data in a consistent structure
    for further processing.

    Args:
        query: The SQL query to execute (required).
               Example: "SELECT * FROM `project.dataset.table` LIMIT 10"
        service_account_path: (Optional) Path to Google Cloud service account JSON credentials file.
                              If not provided, uses default application credentials.
                              Example: '/path/to/service-account.json'

    Returns:
        The result of the query execution in a structured format.
    """
    try:
        # Initialize the BigQuery client with the specified project and credentials if provided
        if service_account_path:
            # Load credentials from the service account file
            credentials = service_account.Credentials.from_service_account_file(
                service_account_path,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
            client = bigquery.Client(project=DEFAULT_PROJECT_ID, credentials=credentials)
        else:
            client = bigquery.Client(project=DEFAULT_PROJECT_ID)

        # Execute the query
        query_job = client.query(query, project=DEFAULT_PROJECT_ID)

        # Get the results
        results = query_job.result()

        # Convert results to a list of dictionaries for easier processing
        results_list = [dict(row) for row in results]

        return {"success": True, "results": results_list}

    except NotFound as e:
        # Handle the case where dataset or table is not found (404)
        return {
            "success": False,
            "error": "Not Found",
            "message": f"Dataset or table not found: {str(e)}",
            "code": 404,
        }

    except GoogleAPIError as e:
        # Handle other Google API errors
        return {"success": False, "error": "Google API Error", "message": str(e), "code": getattr(e, "code", 500)}

    except Exception as e:
        # Handle any other unexpected errors
        return {"success": False, "error": "Execution Error", "message": str(e), "code": 500}


# Add Client Details tool
@mcp.tool()
def get_client_details(
    client_id: str = "",
    client_name: str = "",
    service_account_path: None = DEFAULT_SERVICE_ACCOUNT_PATH,
) -> dict:
    """Retrieve detailed information about Pulse clients from the data warehouse.

    This tool fetches the most recent client metadata from the data warehouse, including
    client identification, enabled data sources (platforms), and repository information.
    You can search by either client ID or client name.

    Args:
        client_id: (Optional) Specific client ID to filter by. Used for exact matching.
                  Example: "123" or "456"
        client_name: (Optional) Client name to search for. Uses partial matching (LIKE operator).
                    Example: "Acme" will match "Acme Corp", "Acme Inc", etc.
        service_account_path: (Optional) Path to Google Cloud service account JSON credentials file.
                              If not provided, uses default application credentials.
                              Example: '/path/to/service-account.json'

    Returns:
        The result of the query execution in a structured format.

    Note: At least one of client_id or client_name must be provided.
    """
    try:
        # Initialize the BigQuery client with the specified project and credentials if provided
        if service_account_path:
            # Load credentials from the service account file
            credentials = service_account.Credentials.from_service_account_file(
                service_account_path,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
            client = bigquery.Client(project=DEFAULT_PROJECT_ID, credentials=credentials)
        else:
            client = bigquery.Client(project=DEFAULT_PROJECT_ID)

        # Base query to get client details
        base_query = """
        WITH
          sources AS (
          SELECT
            updated_at,
            client_id,
            client_name,
            sources
          FROM
            `insightsprod.edm_insights_metadata.client`
          QUALIFY
            ROW_NUMBER() OVER (PARTITION BY client_id, client_name ORDER BY updated_at DESC) = 1
          ORDER BY
            updated_at DESC),
          git as (
            select client_id, git_url from `insightsprod.edm_insights_metadata.client_git`
            qualify row_number() OVER (partition by client_id order by updated_at desc) = 1
          ),
          final AS (
          SELECT
            src.*,
            g.git_url
          FROM
            sources as src
          left join git as g
          on src.client_id = g.client_id )
        SELECT
          *
        FROM
          final
        """

        # Add filters if specified
        where_clauses = []

        if client_id:
            where_clauses.append(f"client_id = {client_id}")

        if client_name:
            where_clauses.append(f"LOWER(client_name) LIKE LOWER('%{client_name}%')")

        if where_clauses:
            base_query += " WHERE " + " AND ".join(where_clauses)

        # Order results
        base_query += " ORDER BY updated_at DESC"

        # Limit results to prevent excessive data transfer
        base_query += " LIMIT 1000"

        # Execute the query
        query_job = client.query(base_query, project=DEFAULT_PROJECT_ID)

        # Get the results
        results = query_job.result()

        # Convert results to a list of dictionaries
        results_list = [dict(row) for row in results]

        return {"success": True, "results": results_list}

    except NotFound as e:
        # Handle the case where dataset or table is not found
        return {
            "success": False,
            "error": "Not Found",
            "message": f"Client metadata tables not found: {str(e)}",
            "code": 404,
        }

    except GoogleAPIError as e:
        # Handle other Google API errors
        return {"success": False, "error": "Google API Error", "message": str(e), "code": getattr(e, "code", 500)}

    except Exception as e:
        # Handle any other unexpected errors
        return {"success": False, "error": "Execution Error", "message": str(e), "code": 500}


@mcp.tool()
def get_client_datasets(
    client_id: str = "",
    client_name: str = "",
    service_account_path: None = DEFAULT_SERVICE_ACCOUNT_PATH,
) -> dict:
    """Find all BigQuery datasets associated with a specific Pulse client.

    This tool searches for datasets that belong to a particular client by matching
    either client ID or client name in the dataset names. It uses the BigQuery
    INFORMATION_SCHEMA.SCHEMATA view to get comprehensive metadata about each dataset.

    Args:
        client_id: (Optional) Specific client ID to filter datasets by. The client ID will be matched
                  against dataset names using a LIKE operator (partial matching).
                  Example: "123" will match datasets containing "123" in their names
        client_name: (Optional) Client name to search for in dataset names. Uses partial matching (LIKE operator).
                    Example: "Acme" will match datasets containing "Acme" in their names
        service_account_path: (Optional) Path to Google Cloud service account JSON credentials file.
                              If not provided, uses default application credentials.
                              Example: '/path/to/service-account.json'

    Returns:
        A dictionary containing the list of datasets and their metadata.

    Note: Either client_id or client_name must be provided, or a ValueError will be raised.
    """
    try:
        # Initialize the BigQuery client with the specified project and credentials if provided
        if service_account_path:
            # Load credentials from the service account file
            credentials = service_account.Credentials.from_service_account_file(
                service_account_path,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
            client = bigquery.Client(project=DEFAULT_PROJECT_ID, credentials=credentials)
        else:
            client = bigquery.Client(project=DEFAULT_PROJECT_ID)

        # Create filter condition based on client_id or client_name
        client_filter = ""
        if client_id:
            client_filter = f"schema_name LIKE '%{client_id}%'"
        elif client_name:
            # For client_name, we'll match any dataset that might contain the client name
            # This is a simplification since dataset naming conventions might vary
            client_filter = f"schema_name LIKE '%{client_name}%'"
        else:
            raise ValueError("Either client_id or client_name must be provided.")

        # Query to get datasets from INFORMATION_SCHEMA
        query = f"""
        SELECT
          schema_name AS dataset_id,
          catalog_name AS project_id,
          creation_time,
          last_modified_time,
          location
        FROM
          `{DEFAULT_PROJECT_ID}.region-us-central1.INFORMATION_SCHEMA.SCHEMATA`
        WHERE {client_filter}
        ORDER BY
          last_modified_time DESC
        """

        # Execute the query
        query_job = client.query(query, project=DEFAULT_PROJECT_ID)

        # Get the results
        results = query_job.result()

        # Convert results to a list of dictionaries
        datasets_list = [dict(row) for row in results]

        return {"success": True, "results": datasets_list}

    except NotFound as e:
        # Handle the case where dataset or table is not found
        return {
            "success": False,
            "error": "Not Found",
            "message": f"Dataset information not found: {str(e)}",
            "code": 404,
        }

    except GoogleAPIError as e:
        # Handle other Google API errors
        return {"success": False, "error": "Google API Error", "message": str(e), "code": getattr(e, "code", 500)}

    except Exception as e:
        # Handle any other unexpected errors
        return {"success": False, "error": "Execution Error", "message": str(e), "code": 500}


@mcp.tool()
def get_dataset_tables(
    dataset_id: str,
    project_id: str = DEFAULT_PROJECT_ID,
    service_account_path: None = DEFAULT_SERVICE_ACCOUNT_PATH,
) -> dict:
    """Retrieve a comprehensive list of all tables in a specific BigQuery dataset with their metadata.

    This tool queries the BigQuery INFORMATION_SCHEMA.TABLES view to get detailed information
    about each table in the provided dataset. The results include table names, types,
    creation times, and other metadata that can help you understand the dataset structure.

    Args:
        dataset_id: The ID of the BigQuery dataset to list tables from (required).
            Example: 'my_dataset' or 'client_123_data'
        project_id: (Optional) The Google Cloud project ID where the dataset resides.
            If not provided, uses the default project from environment variables.
            Example: 'my-project' or 'analytics-prod'
        service_account_path: (Optional) Path to Google Cloud service account JSON credentials file.
            If not provided, uses default application credentials.
            Example: '/path/to/service-account.json'

    Returns:
        A dictionary containing the list of tables and their metadata.
    """
    try:
        # Initialize the BigQuery client with the specified project and credentials if provided
        if service_account_path:
            # Load credentials from the service account file
            credentials = service_account.Credentials.from_service_account_file(
                service_account_path,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
            client = bigquery.Client(project=project_id, credentials=credentials)
        else:
            client = bigquery.Client(project=project_id)

        # Query to get tables from INFORMATION_SCHEMA
        query = f"""
        SELECT
          table_catalog,
          table_schema,
          table_name,
          table_type,
          is_insertable_into,
          is_typed,
          creation_time,
          ddl
        FROM
          `{project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLES`
        ORDER BY
          table_name
        """

        # Execute the query
        query_job = client.query(query, project=project_id)

        # Get the results
        results = query_job.result()

        # Convert results to a list of dictionaries
        tables_list = [dict(row) for row in results]

        return {"success": True, "results": tables_list}

    except NotFound as e:
        # Handle the case where dataset is not found
        return {
            "success": False,
            "error": "Not Found",
            "message": f"Dataset '{dataset_id}' not found: {str(e)}",
            "code": 404,
        }

    except GoogleAPIError as e:
        # Handle other Google API errors
        return {"success": False, "error": "Google API Error", "message": str(e), "code": getattr(e, "code", 500)}

    except Exception as e:
        # Handle any other unexpected errors
        return {"success": False, "error": "Execution Error", "message": str(e), "code": 500}
