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
    """Execute a BigQuery SQL query and return the results.

    Args:
        query: SQL query to execute
        service_account_path: (Optional) Optional path to service account JSON credentials file

    Returns:
        Dictionary containing query results or error information
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
    """Retrieve Pulse client details from the data warehouse. It will return the most recent client details.
    This includes the client_id, client_name, sources (platforms enabled), and git_url.

    Args:
        client_id: (Optional) Specific client ID to filter by
        client_name: (Optional) Client name to search for (uses LIKE operator for partial matches)
        service_account_path: (Optional) Path to service account JSON credentials file

    Returns:
        Dictionary containing client details including updated_at, client_id, client_name, sources, and git_url
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
    """Retrieve Pulse client datasets from the data warehouse. Dataset names are found from the INFORMATION_SCHEMA
    tables.

    Args:
        client_id: (Optional) Specific client ID to filter by
        client_name: (Optional) Client name to search for (uses LIKE operator for partial matches)
        service_account_path: (Optional) Path to service account JSON credentials file

    Returns:
        Dictionary containing dataset information or error details
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
    """Retrieve a list of tables in a specific BigQuery dataset using INFORMATION_SCHEMA views.

    Args:
        dataset_id: The dataset ID to list tables from
        project_id: (Optional) The project ID where the dataset resides
        service_account_path: (Optional) Path to service account JSON credentials file

    Returns:
        Dictionary containing the list of tables and their metadata or error details
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
          creation_time
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
