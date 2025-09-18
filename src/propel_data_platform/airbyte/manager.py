import time
import traceback
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests


@dataclass
class AirbyteConfig:
    """Configuration for Airbyte API client."""

    base_url: str
    client_id: str
    client_secret: str
    _access_token: Optional[str] = None
    _token_expiry: Optional[float] = None

    def get_access_token(self) -> str:
        """Get a valid access token, refreshing if necessary.
        Returns:
            str: Valid access token
        Raises:
            requests.exceptions.RequestException: If token request fails
        """
        if (
            self._access_token is None
            or self._token_expiry is None
            or time.time() >= self._token_expiry
        ):
            self._refresh_token()
        return self._access_token

    def _refresh_token(self) -> None:
        """Refresh the access token using client credentials.
        Raises:
            requests.exceptions.RequestException: If token refresh fails
        """
        token_url = urljoin(self.base_url, "api/public/v1/applications/token")

        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        headers = {"accept": "application/json", "content-type": "application/json"}
        response = requests.post(token_url, json=payload, headers=headers)
        response.raise_for_status()

        token_data = response.json()
        self._access_token = token_data["access_token"]
        assert token_data["token_type"] == "Bearer"
        self._token_expiry = time.time() + token_data.get("expires_in", 180) - 10


class AirbyteClient:
    """Client for interacting with Airbyte API."""

    def __init__(self, config: AirbyteConfig):
        """Initialize the Airbyte client.
        Args:
            config: AirbyteConfig object containing API configuration
        """
        self.config = config
        self.base_url = urljoin(config.base_url, "api/public/v1/")
        self._workspace_id = None

    def _get_headers(self) -> Dict[str, str]:
        """Get headers with current access token.
        Returns:
            Dict[str, str]: Headers with authorization
        """
        return {
            "accept": "application/json",
            "content-type": "application/json",
            "authorization": f"Bearer {self.config.get_access_token()}",
        }

    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Make HTTP request to Airbyte API.
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint
            params: Query parameters
            json: Request body
        Returns:
            API response as dictionary
        Raises:
            requests.exceptions.RequestException: If request fails
        """
        url = urljoin(self.base_url, endpoint)
        response = requests.request(
            method=method,
            url=url,
            headers=self._get_headers(),
            params=params,
            json=json,
        )
        response.raise_for_status()
        try:
            response = response.json()
        except Exception as e:
            print(traceback.format_exc())
        return response

    def _get(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Make GET request to Airbyte API."""
        return self._make_request("GET", endpoint, params=params)

    def _post(self, endpoint: str, json: Dict[str, Any]) -> Dict[str, Any]:
        """Make POST request to Airbyte API."""
        return self._make_request("POST", endpoint, json=json)

    def _put(self, endpoint: str, json: Dict[str, Any]) -> Dict[str, Any]:
        """Make PUT request to Airbyte API."""
        return self._make_request("PUT", endpoint, json=json)

    def _patch(self, endpoint: str, json: Dict[str, Any]) -> Dict[str, Any]:
        """Make PATCH request to Airbyte API."""
        return self._make_request("PATCH", endpoint, json=json)

    def _delete(self, endpoint: str) -> Dict[str, Any]:
        """Make DELETE request to Airbyte API."""
        return self._make_request("DELETE", endpoint)

    @property
    def workspace_id(self) -> str:
        """Get the workspace ID."""
        if self._workspace_id is None:
            self._workspace_id = self._get("workspaces")["data"][0]["workspaceId"]
        return self._workspace_id

    def list_sources(self) -> List[Dict[str, Any]]:
        """List all sources in the workspace.
        Returns:
            List of source configurations
        """
        sources = []
        while True:
            response = self._get("sources", {"offset": len(sources)})
            if len(response["data"]) == 0:
                break
            sources.extend(response["data"])
        return sources

    def get_source(self, source_id: str) -> Dict[str, Any]:
        """Get details of a specific source.
        Args:
            source_id: ID of the source to retrieve
        Returns:
            Source configuration
        """
        return self._get(f"sources/{source_id}")

    def create_source(
        self, name: str, definition_id: str, configuration: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create a new source.
        Args:
            name: Name of the source
            definition_id: ID of the source definition
            configuration: Configuration for the source
        """
        return self._post(
            "sources",
            {
                "name": name,
                "definitionId": definition_id,
                "workspaceId": self.workspace_id,
                "configuration": configuration,
            },
        )

    def update_source(
        self, source_id: str, source_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update an existing source
        Args:
            source_id: ID of the source to update
            source_config: Updated source configuration
        Returns:
            Updated source configuration
        """
        return self._patch(f"sources/{source_id}", source_config)

    def delete_source(self, source_id: str) -> None:
        """Delete a source.
        Args:
            source_id: ID of the source to delete
        """
        self._delete(f"sources/{source_id}")

    def list_destinations(self) -> List[Dict[str, Any]]:
        """List all destinations in the workspace.
        Returns:
            List of destination configurations
        """
        destinations = []
        while True:
            response = self._get("destinations", {"offset": len(destinations)})
            if len(response["data"]) == 0:
                break
            destinations.extend(response["data"])
        return destinations

    def get_destination(self, destination_id: str) -> Dict[str, Any]:
        """Get details of a specific destination.
        Args:
            destination_id: ID of the destination to retrieve
        Returns:
            Destination configuration
        """
        return self._get(f"destinations/{destination_id}")

    def update_destination(
        self, destination_id: str, destination_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update an existing destination.
        Args:
            destination_id: ID of the destination to update
            destination_config: Updated destination configuration
        Returns:
            Updated destination configuration
        """
        return self._patch(f"destinations/{destination_id}", destination_config)

    def delete_destination(self, destination_id: str) -> None:
        """Delete a destination.
        Args:
            destination_id: ID of the destination to delete
        """
        self._delete(f"destinations/{destination_id}")

    def list_connections(self) -> List[Dict[str, Any]]:
        """List all connections in the workspace.
        Returns:
            List of connection configurations
        """
        connections = []
        while True:
            response = self._get("connections", {"offset": len(connections)})
            if len(response["data"]) == 0:
                break
            connections.extend(response["data"])
        return connections

    def get_connection(self, connection_id: str) -> Dict[str, Any]:
        """Get details of a specific connection.
        Args:
            connection_id: ID of the connection to retrieve
        Returns:
            Connection configuration
        """
        return self._get(f"connections/{connection_id}")

    def create_connection(
        self,
        name: str,
        source_id: str,
        destination_id: str,
        configurations: Dict[str, Any],
        schedule: Dict[str, Any],
        namespace_format: str,
        prefix: str,
    ) -> Dict[str, Any]:
        """Create a new connection.
        Args:
            source_id: ID of the source
            destination_id: ID of the destination
            configuration: Configuration for the connection
            schedule: Schedule for the connection
            namespace_format: Namespace format for the connection
            prefix: Prefix for the connection
        """
        return self._post(
            "connections",
            {
                "name": name,
                "sourceId": source_id,
                "destinationId": destination_id,
                "configurations": configurations,
                "schedule": schedule,
                "nonBreakingSchemaUpdatesBehavior": "propagate_columns",
                "namespaceDefinition": "custom_format",
                "namespaceFormat": namespace_format,
                "prefix": prefix,
                "status": "active",
            },
        )

    def update_connection(
        self, connection_id: str, connection_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update an existing connection.
        Args:
            connection_id: ID of the connection to update
            connection_config: Updated connection configuration
        Returns:
            Updated connection configuration
        """
        return self._patch(f"connections/{connection_id}", connection_config)

    def delete_connection(self, connection_id: str) -> None:
        """Delete a connection.
        Args:
            connection_id: ID of the connection to delete
        """
        self._delete(f"connections/{connection_id}")

    def sync_connection(self, connection_id: str) -> Dict[str, Any]:
        """Sync a connection.
        Args:
            connection_id: ID of the connection to sync
        """
        return self._post("jobs", {"connectionId": connection_id, "jobType": "sync"})

    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get the status of a job.
        Args:
            job_id: ID of the job to retrieve
        Returns:
            Job status
        """
        return self._get(f"jobs/{job_id}")
