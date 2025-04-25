# Pulse Backend MCP Server

A Model Context Protocol (MCP) server that provides BigQuery access and specialized data tools for developers within our company to increase productivity.

## Overview

This MCP server implements the [Model Context Protocol](https://modelcontextprotocol.io/introduction) to enable LLM-powered applications to access company data and execute specialized data functions in a controlled manner. The server exposes several tools for interacting with BigQuery and company-specific data structures.

## Project Structure

```
pulse-backend-mcp/
├── README.md                 # Project documentation
├── pyproject.toml            # Python project configuration
├── uv.lock                   # Dependency lock file
└── src/                      # Source code directory
    └── server.py             # Main MCP server implementation
```

## What is MCP?

The Model Context Protocol (MCP) is an open protocol that standardizes how applications provide context to LLMs. Similar to how USB-C provides a standardized way to connect devices to peripherals, MCP provides a standardized way to connect AI models to different data sources and tools.

### MCP Architecture

MCP follows a client-server architecture:

- **MCP Hosts**: Programs like Claude Desktop, IDEs, or AI tools that want to access data through MCP
- **MCP Clients**: Protocol clients that maintain 1:1 connections with servers
- **MCP Servers**: Lightweight programs (like this one) that expose specific capabilities through the standardized protocol
- **Data Sources**: Your databases, files, or services that MCP servers can securely access

### Communication Flow

1. The host application (e.g., Claude Desktop) initializes a connection to our MCP server
2. The client discovers the available tools through the `tools/list` endpoint
3. When prompted by a user, the LLM can use our tools to execute BigQuery queries or retrieve client information
4. Our server executes the requested operations and returns results to the client
5. The client presents the results to the user within the host application

## Key Features

- **BigQuery Integration**: Execute SQL queries against company BigQuery datasets
- **Client Data Access**: Retrieve client details and datasets from our data warehouse
- **Extensible Architecture**: Add new tools to support additional use cases

## Prerequisites

- Python 3.13 or higher
- Google Cloud account with BigQuery access
- Service account credentials with appropriate permissions

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/saras-mcp.git
   cd saras-mcp
   ```

2. Create a virtual environment:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows, use: venv\Scripts\activate
   ```

3. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

## Configuration

1. Set up Google Cloud credentials by either:
   - Setting the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to point to your service account key file:
     ```
     export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
     ```
   - Passing the service account path directly to the tools when calling them

2. (Optional) Adjust the default project ID in the tool definitions if needed

## Usage

Start the MCP server in the inspector:

```
mcp dev src/server.py
```

The server will start on the default MCP port (typically 8080). You can now connect MCP-compatible clients to this server.

### Testing with MCP Inspector

To test your server implementation:

1. Install the [MCP Inspector](https://modelcontextprotocol.io/inspector)
2. Connect to your running server
3. Explore available tools and test their functionality

## Available Tools

### 1. execute_bigquery

Execute BigQuery SQL queries and receive results as structured data.

**Parameters:**
- `query` (string, required): The SQL query to execute
- `project_id` (string, optional): Google Cloud project ID (default: "insightsprod")
- `service_account_path` (string, optional): Path to service account JSON credentials

**Tool Annotations:**
- Read-only: Yes (doesn't modify data)
- Open World: Yes (interacts with external BigQuery service)

### 2. get_client_details

Retrieve client information from our data warehouse.

**Parameters:**
- `client_id` (string, optional): Specific client ID to filter by
- `client_name` (string, optional): Client name to search for (supports partial matches)
- `project_id` (string, optional): Google Cloud project ID (default: "insightsprod")
- `service_account_path` (string, optional): Path to service account JSON credentials

**Tool Annotations:**
- Read-only: Yes (doesn't modify data)
- Open World: No (operates on internal data warehouse)

### 3. get_client_datasets

Retrieve available datasets for a specific client.

**Parameters:**
- `client_id` (string, optional): Specific client ID to filter by
- `client_name` (string, optional): Client name to search for (supports partial matches)
- `project_id` (string, optional): Google Cloud project ID (default: "insightsprod")
- `service_account_path` (string, optional): Path to service account JSON credentials

**Tool Annotations:**
- Read-only: Yes (doesn't modify data)
- Open World: No (operates on internal data warehouse)

## Extending the Server

### Adding New Tools

To add a new tool to the MCP server:

1. Add a new function to `server.py` decorated with `@mcp.tool()`
2. Define the parameters and return type for your function
3. Add comprehensive docstrings to document the tool's purpose and usage
4. Implement error handling for a robust user experience

Example:

```python
@mcp.tool()
def my_new_tool(param1: str, param2: int = 0) -> dict:
    """Description of what the tool does.

    Args:
        param1: Description of param1
        param2: (Optional) Description of param2

    Returns:
        Dictionary containing the results or error information
    """
    try:
        # Implementation
        return {"success": True, "results": [...]}
    except Exception as e:
        return {
            "success": False, 
            "error": "Error Type", 
            "message": str(e),
            "code": 500
        }
```

### Proper Error Handling

For tools that might encounter errors:

1. Use the appropriate error structure
2. Return specific error codes when possible
3. Provide meaningful error messages

Example:

```python
try:
    # Tool operation
    result = perform_operation()
    return {"success": True, "results": result}
except NotFound as e:
    return {
        "success": False,
        "error": "Not Found",
        "message": str(e),
        "code": 404,
    }
except Exception as e:
    return {
        "success": False,
        "error": "Execution Error",
        "message": str(e),
        "code": 500,
    }
```

### Tool Annotations

When defining tools, consider adding annotations to help clients understand the tool's behavior:

- `readOnlyHint`: Indicates if the tool modifies its environment
- `destructiveHint`: Indicates if the tool may perform destructive operations
- `idempotentHint`: Indicates if repeated calls with the same arguments have no additional effect
- `openWorldHint`: Indicates if the tool interacts with external entities

## Security Considerations

When developing MCP servers, follow these security best practices:

1. **Input Validation**
   - Validate all parameters against their schemas
   - Sanitize SQL queries to prevent injection attacks
   - Check parameter sizes and ranges

2. **Access Control**
   - Implement appropriate authentication when needed
   - Use proper authorization for accessing sensitive data
   - Consider rate limiting for resource-intensive operations

3. **Error Handling**
   - Don't expose internal errors to clients
   - Log security-relevant errors
   - Clean up resources appropriately after errors

## MCP Protocol Resources

- [MCP Introduction](https://modelcontextprotocol.io/introduction)
- [Core Architecture](https://modelcontextprotocol.io/docs/concepts/architecture)
- [Resources](https://modelcontextprotocol.io/docs/concepts/resources)
- [Tools](https://modelcontextprotocol.io/docs/concepts/tools)
- [MCP Server Developer Guide](https://modelcontextprotocol.io/quickstart/server)

## Contributing

1. Create a new branch for your feature or bugfix
2. Add appropriate tests for your changes
3. Submit a pull request with a clear description of the changes

## License

[Your License Here]
