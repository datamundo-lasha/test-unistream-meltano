"""REST client handling for App Store Connect API.

This module contains the base stream class with common functionality
for authentication, HTTP requests, and response parsing.
"""

from __future__ import annotations

import csv
import gzip
import os
import shutil
import sys
import tempfile
from functools import cached_property
from typing import TYPE_CHECKING, Any

import requests
from singer_sdk.streams import Stream

from tap_appstoreconnect.auth import JWTAuthenticator

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class AppStoreConnectStream(Stream):
    """Base stream class for App Store Connect API.
    
    All App Store Connect streams inherit from this class and share:
    - JWT authentication
    - Base URL configuration
    - Common HTTP request handling
    - CSV download and parsing utilities
    """

    @property
    def url_base(self) -> str:
        """Get the API base URL from config.
        
        Returns:
            Base URL for API requests
        """
        return self.config.get(
            "api_url",
            "https://api.appstoreconnect.apple.com/v1"
        )
    
    @property
    def timeout(self) -> int:
        """Get request timeout from config.
        
        Returns:
            Timeout in seconds
        """
        return self.config.get("request_timeout", 300)
    
    @cached_property
    def authenticator(self) -> JWTAuthenticator:
        """Get JWT authenticator for API requests.
        
        Authenticator is cached to reuse tokens across requests.
        
        Returns:
            JWT authenticator instance
        """
        return JWTAuthenticator(
            stream=self,
            issuer_id=self.config["issuer_id"],
            key_id=self.config["key_id"],
            private_key=self.config["private_key"],
        )
    
    def http_get(
        self,
        url: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Make GET request to API.
        
        Args:
            url: Full URL for the request
            params: Optional query parameters
            
        Returns:
            Parsed JSON response
            
        Raises:
            requests.HTTPError: If request fails
        """
        self.logger.debug(f"GET {url}")
        
        response = requests.get(
            url,
            params=params,
            auth=self.authenticator,
            timeout=self.timeout,
        )
        response.raise_for_status()
        
        return response.json()
    
    def http_post(
        self,
        url: str,
        body: dict[str, Any],
    ) -> dict[str, Any]:
        """Make POST request to API.
        
        Args:
            url: Full URL for the request
            body: Request body as dictionary
            
        Returns:
            Parsed JSON response
            
        Raises:
            requests.HTTPError: If request fails
        """
        self.logger.debug(f"POST {url}")
        
        response = requests.post(
            url,
            json=body,
            auth=self.authenticator,
            headers={"Content-Type": "application/json"},
            timeout=self.timeout,
        )
        response.raise_for_status()
        
        return response.json()
    
    def download_and_extract_csv(
        self,
        url: str,
        output_path: str,
    ) -> str:
        """Download gzipped CSV file and extract it.
        
        App Store Connect API returns CSV files as gzipped archives.
        This method downloads and extracts them.
        
        Args:
            url: URL to download from
            output_path: Path to save extracted CSV
            
        Returns:
            Path to extracted CSV file
        """
        gz_path = output_path + ".gz"
        
        self.logger.debug(f"Downloading CSV from {url[:50]}...")
        
        # Download gzipped file
        with requests.get(url, stream=True, timeout=self.timeout) as response:
            response.raise_for_status()
            with open(gz_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1 << 20):  # 1MB chunks
                    if chunk:
                        f.write(chunk)
        
        # Extract gzip file
        self.logger.debug(f"Extracting CSV to {output_path}")
        with gzip.open(gz_path, "rb") as fin:
            with open(output_path, "wb") as fout:
                shutil.copyfileobj(fin, fout)
        
        # Clean up gzip file
        os.remove(gz_path)
        
        return output_path
    
    def parse_csv_file(
        self,
        csv_path: str,
        delimiter: str = "\t",
    ) -> list[dict[str, Any]]:
        """Parse CSV file and return list of records.
        
        Args:
            csv_path: Path to CSV file
            delimiter: CSV delimiter (App Store uses tab by default)
            
        Returns:
            List of dictionaries, one per row
        """
        records = []
        
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            for row in reader:
                records.append(row)
        
        return records
    
    @staticmethod
    def parse_int_safe(value: str | None) -> int:
        """Safely parse integer from string.
        
        Args:
            value: String value to parse
            
        Returns:
            Integer value, or 0 if parsing fails
        """
        if not value:
            return 0
        
        try:
            return int(value.replace(",", "").strip())
        except (ValueError, TypeError):
            return 0
    
    @staticmethod
    def create_temp_dir() -> str:
        """Create temporary directory for CSV downloads.
        
        Returns:
            Path to temporary directory
        """
        temp_dir = tempfile.mkdtemp(prefix="asc_tap_")
        return temp_dir
    
    @staticmethod
    def cleanup_temp_dir(temp_dir: str) -> None:
        """Clean up temporary directory.
        
        Args:
            temp_dir: Path to temporary directory to remove
        """
        if os.path.isdir(temp_dir):
            shutil.rmtree(temp_dir)
