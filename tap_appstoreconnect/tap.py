"""App Store Connect Analytics tap class.

This module contains the main Tap class which coordinates:
- Configuration schema definition
- Stream discovery
- Authentication setup
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_appstoreconnect import streams

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Sequence


class TapAppStoreConnect(Tap):
    """Singer tap for Apple App Store Connect Analytics API.
    
    Extracts app analytics data including downloads, sessions, active devices,
    and calculated metrics. Supports incremental replication based on date.
    """

    name = "tap-appstoreconnect"
    
    description = "Singer tap for Apple App Store Connect Analytics API"

    config_jsonschema = th.PropertiesList(
        # Required: Authentication credentials
        th.Property(
            "issuer_id",
            th.StringType(nullable=False),
            required=True,
            secret=True,
            title="App Store Connect Issuer ID",
            description="Your App Store Connect API Issuer ID from Users and Access > Integrations",
        ),
        th.Property(
            "key_id",
            th.StringType(nullable=False),
            required=True,
            secret=True,
            title="App Store Connect Key ID",
            description="Your App Store Connect API Key ID",
        ),
        th.Property(
            "private_key",
            th.StringType(nullable=False),
            required=True,
            secret=True,
            title="Private Key",
            description="Private key in PEM format for signing JWT tokens. Include full PEM format with BEGIN/END markers.",
        ),
        
        # Required: App configuration
        th.Property(
            "app_id",
            th.StringType(nullable=False),
            required=True,
            title="App Apple ID",
            description="Your app's Apple ID (e.g., '6463405199')",
        ),
        
        # Optional: Date range configuration
        th.Property(
            "start_date",
            th.DateTimeType(nullable=True),
            title="Start Date",
            description=(
                "Earliest date to sync. Format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ. "
                "If omitted, defaults to 30 days ago."
            ),
        ),
        th.Property(
            "end_date",
            th.DateTimeType(nullable=True),
            title="End Date",
            description=(
                "Latest date to sync. Format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ. "
                "If omitted, defaults to yesterday (App Store data has 1-day lag)."
            ),
        ),
        
        # Optional: Request configuration
        th.Property(
            "request_timeout",
            th.IntegerType(nullable=True),
            title="Request Timeout",
            default=300,
            description="HTTP request timeout in seconds (default: 300)",
        ),
        
        # Optional: API endpoint (for testing)
        th.Property(
            "api_url",
            th.StringType(nullable=True),
            title="API Base URL",
            default="https://api.appstoreconnect.apple.com/v1",
            description="App Store Connect API base URL (default: https://api.appstoreconnect.apple.com/v1)",
        ),
        
    ).to_dict()

    @override
    def discover_streams(self) -> Sequence[streams.AppStoreConnectStream]:
        """Return a list of discovered streams.
        
        Returns:
            List of stream instances for App Store Connect analytics.
        """
        return [
            streams.AppAnalyticsStream(self),
        ]
    
    @property
    def start_date_value(self) -> datetime:
        """Get start date from config or default to 30 days ago.
        
        Returns:
            Start date as datetime object
        """
        if start_date := self.config.get("start_date"):
            if isinstance(start_date, str):
                return datetime.fromisoformat(start_date.replace("Z", "+00:00"))
            return start_date
        
        # Default to 30 days ago
        return datetime.now(timezone.utc) - timedelta(days=30)
    
    @property
    def end_date_value(self) -> datetime:
        """Get end date from config or default to yesterday.
        
        App Store data typically has a 1-day lag, so default to yesterday.
        
        Returns:
            End date as datetime object
        """
        if end_date := self.config.get("end_date"):
            if isinstance(end_date, str):
                return datetime.fromisoformat(end_date.replace("Z", "+00:00"))
            return end_date
        
        # Default to yesterday (App Store data has 1-day lag)
        return datetime.now(timezone.utc) - timedelta(days=1)


if __name__ == "__main__":
    TapAppStoreConnect.cli()
