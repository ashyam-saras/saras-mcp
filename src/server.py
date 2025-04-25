# server.py

import os  # Import os module for environment variables

import requests  # Import requests for HTTP calls
from dotenv import load_dotenv
from google.api_core.exceptions import GoogleAPIError, NotFound
from google.cloud import bigquery  # Import BigQuery client
from google.oauth2 import service_account
from mcp.server.fastmcp import FastMCP

# Load environment variables from .env file
load_dotenv()

# Create an MCP server
mcp = FastMCP("bigquery")

# Get service account path from environment variable if available
DEFAULT_SERVICE_ACCOUNT_PATH = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
DEFAULT_PROJECT_ID = os.environ.get("GOOGLE_PROJECT_ID")
DEFAULT_CLICKUP_API_KEY = os.environ.get("DEFAULT_CLICKUP_API_KEY")


# Add BigQuery execution tool
@mcp.tool()
def execute_bigquery(query: str, service_account_path: str = DEFAULT_SERVICE_ACCOUNT_PATH) -> dict:
    """Execute a custom BigQuery SQL query and return the results.

    Runs SQL queries against Google BigQuery, handling authentication and formatting
    results for easier processing.

    Args:
        query: The SQL query to execute.
        service_account_path: (Optional) Path to Google Cloud service account JSON.

    Returns:
        A dictionary with query results or error information.
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

    Fetches client metadata including identifiers, enabled data sources, and
    repository information. You can search by either client ID or client name.

    Args:
        client_id: (Optional) Specific client ID to filter by.
        client_name: (Optional) Client name to search for (uses partial matching).
        service_account_path: (Optional) Path to Google Cloud service account JSON.

    Returns:
        A dictionary with client information or error details.

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
    """Find BigQuery datasets associated with a specific Pulse client.

    Searches for datasets matching a client ID or name in the dataset names using
    the BigQuery INFORMATION_SCHEMA.SCHEMATA view.

    Args:
        client_id: (Optional) Client ID to filter datasets by (partial matching).
        client_name: (Optional) Client name to search for (partial matching).
        service_account_path: (Optional) Path to Google Cloud service account JSON.

    Returns:
        A dictionary with datasets information or error details.

    Note: Either client_id or client_name must be provided.
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
    """List all tables in a specific BigQuery dataset with their metadata.

    Queries the INFORMATION_SCHEMA.TABLES view to get information about each table
    in the dataset, including names, types, creation times, and other metadata.

    Args:
        dataset_id: The ID of the BigQuery dataset to list tables from.
        project_id: (Optional) The Google Cloud project ID where the dataset resides.
        service_account_path: (Optional) Path to Google Cloud service account JSON.

    Returns:
        A dictionary with tables information or error details.
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


@mcp.tool()
def get_clickup_task(
    task_id: str,
    api_key: str = DEFAULT_CLICKUP_API_KEY,
    include_subtasks: bool = False,
    include_comments: bool = False,
) -> dict:
    """Retrieve detailed information about a specific ClickUp task.

    Fetches task data including status, assignees, description, custom fields,
    and tags from the ClickUp API. Can optionally include subtasks and comments.

    Args:
        task_id: The unique identifier of the ClickUp task.
        api_key: (Optional) ClickUp API key for authentication.
        include_subtasks: (Optional) Whether to include subtask information.
        include_comments: (Optional) Whether to include task comments.

    Returns:
        A dictionary with task data or error information.

    API Reference: https://developer.clickup.com/reference/gettask
    """
    try:
        # Build the base URL for the ClickUp API request
        base_url = f"https://api.clickup.com/api/v2/task/{task_id}"

        # Add query parameters for optional data
        params = {}
        if include_subtasks:
            params["include_subtasks"] = "true"

        # Set up headers with API key authorization
        headers = {"Authorization": api_key, "Content-Type": "application/json"}

        # Make the API request to get task details
        response = requests.get(base_url, headers=headers, params=params)

        # Check if the response is successful
        response.raise_for_status()

        # Parse the JSON response
        task_data = response.json()

        # If comments are requested and not already included, get them in a separate request
        if include_comments and "comments" not in task_data:
            comments_url = f"https://api.clickup.com/api/v2/task/{task_id}/comment"
            comments_response = requests.get(comments_url, headers=headers)

            if comments_response.status_code == 200:
                comments_data = comments_response.json()
                task_data["comments"] = comments_data.get("comments", [])

        return {"success": True, "result": task_data}

    except requests.exceptions.HTTPError as e:
        # Handle HTTP errors with appropriate status codes
        status_code = e.response.status_code
        error_message = str(e)

        # Try to get more detailed error message from the response if possible
        try:
            error_data = e.response.json()
            if "err" in error_data:
                error_message = error_data["err"]
        except:
            pass

        if status_code == 401:
            return {
                "success": False,
                "error": "Authentication Error",
                "message": "Invalid API key or unauthorized access",
                "code": 401,
            }
        elif status_code == 404:
            return {
                "success": False,
                "error": "Not Found",
                "message": f"Task with ID '{task_id}' not found",
                "code": 404,
            }
        else:
            return {"success": False, "error": "ClickUp API Error", "message": error_message, "code": status_code}

    except requests.exceptions.ConnectionError:
        # Handle connection errors (network issues, DNS failure, etc.)
        return {
            "success": False,
            "error": "Connection Error",
            "message": "Failed to connect to the ClickUp API. Please check your internet connection.",
            "code": 503,
        }

    except Exception as e:
        # Handle any other unexpected errors
        return {"success": False, "error": "Execution Error", "message": str(e), "code": 500}
