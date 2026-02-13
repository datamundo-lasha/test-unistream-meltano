"""Stream type classes for tap-appstoreconnect.

This module contains the AppAnalyticsStream class which extracts
daily app analytics metrics from the App Store Connect API.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from requests.exceptions import HTTPError
from singer_sdk import typing as th

from tap_appstoreconnect.client import AppStoreConnectStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class AppAnalyticsStream(AppStoreConnectStream):
    """Stream for App Store Connect analytics data.
    
    Extracts daily metrics including:
    - First-time downloads
    - Redownloads
    - Updates
    - Deletions
    - Sessions
    - Active devices
    - Calculated metrics (avg sessions, loss rate)
    
    Endpoint: App Store Connect Analytics API
    Replication: Full table (always syncs from start_date)
    Primary Key: date
    """
    
    name = "app_analytics"
    
    primary_keys = ("date",)
    replication_key = None  # Full table sync - always extract from start_date
    is_sorted = True
    
    # Report configuration
    DOWNLOADS_REPORT_NAME = "App Downloads Standard"
    USAGE_REPORT_CATEGORY = "APP_USAGE"
    APP_SESSIONS_REPORT_NAME = "App Sessions Standard"
    USAGE_REPORT_PREFER = ["Installation and Deletion", "Install", "Deletion"]
    ACCESS_TYPE = "ONGOING"
    CSV_DELIMITER = "\t"
    
    # Deduplication key columns for downloads
    DEDUP_KEY_COLS = [
        "Date", "App Name", "App Apple Identifier", "Event", "Download Type",
        "App Version", "Device", "Platform Version", "Source Type", "Source Info",
        "Campaign", "Page Type", "Page Title", "App Download Date", "Territory",
        "Unique Devices",
    ]
    
    schema = th.PropertiesList(
        th.Property(
            "date",
            th.StringType,
            required=True,
            description="Date of the metrics (YYYY-MM-DD format)",
        ),
        th.Property(
            "first_time_download",
            th.IntegerType,
            description="Number of first-time app downloads",
        ),
        th.Property(
            "redownload",
            th.IntegerType,
            description="Number of app redownloads",
        ),
        th.Property(
            "updates",
            th.IntegerType,
            description="Number of app updates",
        ),
        th.Property(
            "deletions",
            th.IntegerType,
            description="Number of app deletions",
        ),
        th.Property(
            "total_sessions",
            th.IntegerType,
            description="Total number of app sessions",
        ),
        th.Property(
            "total_active_devices",
            th.IntegerType,
            description="Total number of active devices",
        ),
        th.Property(
            "avg_sessions_per_device",
            th.NumberType,
            description="Average sessions per active device",
        ),
        th.Property(
            "user_loss_rate_percent",
            th.NumberType,
            description="Percentage of users lost (deletions / active devices * 100)",
        ),
    ).to_dict()
    
    @override
    def get_records(self, context: Context | None) -> list[dict[str, Any]]:
        """Get records from App Store Connect API.
        
        This method orchestrates the complete data extraction process:
        1. Ensures analytics report request exists
        2. Finds relevant reports (downloads, sessions, deletes)
        3. Gets report instances within date range
        4. Downloads and parses CSV segments
        5. Aggregates metrics by date
        6. Calculates derived metrics
        
        Args:
            context: Stream partition or context dict
            
        Yields:
            Records containing daily app analytics metrics
        """
        self.logger.info("Starting App Store Connect analytics extraction")
        
        # Create temp directory for CSV downloads
        temp_dir = self.create_temp_dir()
        
        try:
            # Step 1: Ensure report request exists
            request_id = self._ensure_request_id()
            self.logger.info(f"Using analytics report request: {request_id}")
            
            # Step 2: Find reports
            downloads_report_id = self._find_downloads_report(request_id)
            deletes_report_id = self._find_deletes_report(request_id)
            sessions_report_id = self._find_sessions_report(request_id)
            
            self.logger.info(f"Found downloads report: {downloads_report_id}")
            self.logger.info(f"Found deletes report: {deletes_report_id}")
            if sessions_report_id:
                self.logger.info(f"Found sessions report: {sessions_report_id}")
            
            # Step 3: Get instances and determine date range
            start_date = self._tap.start_date_value
            end_date = self._tap.end_date_value
            
            self.logger.info(f"Full table sync - Date range: {start_date.date()} to {end_date.date()}")
            
            # Step 4: Get all instances for each report
            downloads_instances = self._get_instances(downloads_report_id)
            deletes_instances = self._get_instances(deletes_report_id)
            sessions_instances = self._get_instances(sessions_report_id) if sessions_report_id else []
            
            self.logger.info(f"Found {len(downloads_instances)} download instances")
            self.logger.info(f"Found {len(deletes_instances)} delete instances")
            self.logger.info(f"Found {len(sessions_instances)} session instances")
            
            # Step 5: Process instances and aggregate by date
            metrics_by_date = self._process_all_instances(
                temp_dir=temp_dir,
                downloads_instances=downloads_instances,
                deletes_instances=deletes_instances,
                sessions_instances=sessions_instances,
                start_date=start_date,
                end_date=end_date,
            )
            
            # Step 6: Yield records for each date
            for date_str, metrics in sorted(metrics_by_date.items()):
                # Calculate derived metrics
                active_devices = metrics.get("total_active_devices", 0)
                sessions = metrics.get("total_sessions", 0)
                deletions = metrics.get("deletions", 0)
                
                avg_sessions = (sessions / active_devices) if active_devices > 0 else 0
                loss_rate = (deletions / active_devices * 100) if active_devices > 0 else 0
                
                record = {
                    "date": date_str,
                    "first_time_download": metrics.get("first_time_download", 0),
                    "redownload": metrics.get("redownload", 0),
                    "updates": metrics.get("updates", 0),
                    "deletions": deletions,
                    "total_sessions": sessions,
                    "total_active_devices": active_devices,
                    "avg_sessions_per_device": round(avg_sessions, 2),
                    "user_loss_rate_percent": round(loss_rate, 2),
                }
                
                self.logger.info(f"Yielding record for {date_str}: {record}")
                yield record
        
        finally:
            # Cleanup temp directory
            self.cleanup_temp_dir(temp_dir)
            self.logger.info("Cleaned up temporary files")
    
    def _ensure_request_id(self) -> str:
        """Ensure analytics report request exists, create if needed.
        
        Returns:
            Analytics report request ID
        """
        app_id = self.config["app_id"]
        url = f"{self.url_base}/apps/{app_id}/analyticsReportRequests"
        
        # Check for existing request
        data = self.http_get(url, params={"limit": 200})
        
        for item in data.get("data", []):
            attrs = item.get("attributes", {}) or {}
            if attrs.get("accessType") == self.ACCESS_TYPE:
                request_id = item["id"]
                self.logger.debug(f"Found existing request: {request_id}")
                return request_id
        
        # Create new request
        self.logger.info("Creating new analytics report request")
        body = {
            "data": {
                "type": "analyticsReportRequests",
                "attributes": {"accessType": self.ACCESS_TYPE},
                "relationships": {
                    "app": {
                        "data": {
                            "type": "apps",
                            "id": app_id,
                        }
                    }
                },
            }
        }
        
        try:
            response = self.http_post(f"{self.url_base}/analyticsReportRequests", body)
            request_id = response["data"]["id"]
            self.logger.info(f"Created request: {request_id}")
            return request_id
        except Exception as e:
            if "409" in str(e):  # Conflict - request already exists
                return self._ensure_request_id()
            raise
    
    def _find_downloads_report(self, request_id: str) -> str:
        """Find downloads report by name.
        
        Args:
            request_id: Analytics report request ID
            
        Returns:
            Downloads report ID
        """
        return self._find_report_by_name(
            request_id=request_id,
            name_exact=self.DOWNLOADS_REPORT_NAME,
        )
    
    def _find_deletes_report(self, request_id: str) -> str:
        """Find deletions report.
        
        Args:
            request_id: Analytics report request ID
            
        Returns:
            Deletions report ID
        """
        return self._find_report_by_name(
            request_id=request_id,
            category=self.USAGE_REPORT_CATEGORY,
            name_prefer_list=self.USAGE_REPORT_PREFER,
        )
    
    def _find_sessions_report(self, request_id: str) -> str | None:
        """Find sessions report.
        
        Args:
            request_id: Analytics report request ID
            
        Returns:
            Sessions report ID or None if not found
        """
        try:
            return self._find_report_by_name(
                request_id=request_id,
                category=self.USAGE_REPORT_CATEGORY,
                name_exact=self.APP_SESSIONS_REPORT_NAME,
            )
        except Exception as e:
            self.logger.warning(f"Sessions report not found: {e}")
            return None
    
    def _find_report_by_name(
        self,
        request_id: str,
        name_exact: str | None = None,
        category: str | None = None,
        name_prefer_list: list[str] | None = None,
    ) -> str:
        """Find report by name or category.
        
        Args:
            request_id: Analytics report request ID
            name_exact: Exact report name to match
            category: Report category to filter by
            name_prefer_list: List of name keywords to prefer (ordered)
            
        Returns:
            Report ID
            
        Raises:
            RuntimeError: If report not found
        """
        url = f"{self.url_base}/analyticsReportRequests/{request_id}/reports"
        params = {"limit": 200}
        
        if category:
            params["filter[category]"] = category
        
        reports = self.http_get(url, params=params).get("data", [])
        
        if not reports:
            raise RuntimeError(f"No reports found for request {request_id}")
        
        # Find by exact name
        if name_exact:
            for report in reports:
                attrs = report.get("attributes", {}) or {}
                name = (attrs.get("name") or "").strip().lower()
                if name == name_exact.strip().lower():
                    return report["id"]
        
        # Find by preference list
        if name_prefer_list:
            def name_score(report_name: str) -> int:
                n_lower = (report_name or "").lower()
                for i, keyword in enumerate(name_prefer_list):
                    if keyword.lower() in n_lower:
                        return 100 - i
                return 0
            
            reports.sort(
                key=lambda r: name_score((r.get("attributes", {}) or {}).get("name")),
                reverse=True,
            )
            
            if name_score((reports[0].get("attributes", {}) or {}).get("name")) > 0:
                return reports[0]["id"]
        
        raise RuntimeError(f"Could not find report with criteria: {name_exact or name_prefer_list}")
    
    def _get_instances(self, report_id: str) -> list[dict[str, Any]]:
        """Get all report instances, sorted by date (newest first).
        
        Args:
            report_id: Report ID to get instances for
            
        Returns:
            List of report instances, sorted newest first
        """
        if not report_id:
            return []
        
        url = f"{self.url_base}/analyticsReports/{report_id}/instances"
        
        try:
            instances = self.http_get(url).get("data", [])
        except Exception as e:
            self.logger.warning(f"Failed to get instances for {report_id}: {e}")
            return []
        
        if not instances:
            return []
        
        # Sort by period end date (newest first)
        def get_period_end(instance):
            attrs = instance.get("attributes", {}) or {}
            period_end = attrs.get("periodEnd")
            if not period_end:
                return datetime.fromtimestamp(0, tz=timezone.utc)
            return datetime.fromisoformat(period_end.replace("Z", "+00:00"))
        
        instances.sort(key=get_period_end, reverse=True)
        
        return instances
    
    def _process_all_instances(
        self,
        temp_dir: str,
        downloads_instances: list[dict],
        deletes_instances: list[dict],
        sessions_instances: list[dict],
        start_date: datetime,
        end_date: datetime,
    ) -> dict[str, dict[str, int]]:
        """Process all instances and aggregate metrics by date.
        
        Args:
            temp_dir: Temporary directory for CSV downloads
            downloads_instances: Download report instances
            deletes_instances: Delete report instances
            sessions_instances: Session report instances
            start_date: Start date for filtering
            end_date: End date for filtering
            
        Returns:
            Dictionary mapping date strings to metrics dictionaries
        """
        metrics_by_date: dict[str, dict[str, int]] = {}
        
        # Process downloads (first 5 instances for safety)
        self.logger.info("Processing download instances...")
        for i, instance in enumerate(downloads_instances[:5]):
            instance_id = instance["id"]
            self.logger.debug(f"Processing download instance {i+1}/5: {instance_id[:8]}...")
            
            downloads_data = self._process_downloads_instance(
                instance_id=instance_id,
                temp_dir=temp_dir,
                start_date=start_date,
                end_date=end_date,
            )
            
            # Merge into metrics_by_date
            for date_str, metrics in downloads_data.items():
                if date_str not in metrics_by_date:
                    metrics_by_date[date_str] = {}
                
                for key, value in metrics.items():
                    metrics_by_date[date_str][key] = metrics_by_date[date_str].get(key, 0) + value
        
        # Process deletes (first 5 instances)
        self.logger.info("Processing delete instances...")
        for i, instance in enumerate(deletes_instances[:5]):
            instance_id = instance["id"]
            self.logger.debug(f"Processing delete instance {i+1}/5: {instance_id[:8]}...")
            
            deletes_data = self._process_deletes_instance(
                instance_id=instance_id,
                temp_dir=temp_dir,
                start_date=start_date,
                end_date=end_date,
            )
            
            for date_str, deletions in deletes_data.items():
                if date_str not in metrics_by_date:
                    metrics_by_date[date_str] = {}
                metrics_by_date[date_str]["deletions"] = metrics_by_date[date_str].get("deletions", 0) + deletions
        
        # Process sessions (first 5 instances)
        if sessions_instances:
            self.logger.info("Processing session instances...")
            for i, instance in enumerate(sessions_instances[:5]):
                instance_id = instance["id"]
                self.logger.debug(f"Processing session instance {i+1}/5: {instance_id[:8]}...")
                
                sessions_data = self._process_sessions_instance(
                    instance_id=instance_id,
                    temp_dir=temp_dir,
                    start_date=start_date,
                    end_date=end_date,
                )
                
                for date_str, metrics in sessions_data.items():
                    if date_str not in metrics_by_date:
                        metrics_by_date[date_str] = {}
                    
                    for key, value in metrics.items():
                        metrics_by_date[date_str][key] = metrics_by_date[date_str].get(key, 0) + value
        
        return metrics_by_date
    
    def _process_downloads_instance(
        self,
        instance_id: str,
        temp_dir: str,
        start_date: datetime,
        end_date: datetime,
    ) -> dict[str, dict[str, int]]:
        """Process downloads instance and extract metrics.
        
        Args:
            instance_id: Report instance ID
            temp_dir: Temporary directory for CSV files
            start_date: Start date for filtering
            end_date: End date for filtering
            
        Returns:
            Dictionary mapping dates to download metrics
        """
        url = f"{self.url_base}/analyticsReportInstances/{instance_id}/segments"
        
        try:
            segments = self.http_get(url).get("data", [])
        except HTTPError as e:
            if e.response.status_code == 404:
                self.logger.warning(
                    f"Segments not found for downloads instance {instance_id} - "
                    f"instance may still be processing. Skipping."
                )
                return {}
            raise
        
        metrics_by_date: dict[str, dict[str, int]] = {}
        seen_keys: set[tuple] = set()
        
        for i, segment in enumerate(segments):
            attrs = segment.get("attributes", {}) or {}
            seg_url = attrs.get("url")
            
            if not seg_url:
                continue
            
            # Download and extract CSV
            csv_path = os.path.join(temp_dir, f"downloads_{instance_id}_{i}.csv")
            self.download_and_extract_csv(seg_url, csv_path)
            
            # Parse CSV
            records = self.parse_csv_file(csv_path, delimiter=self.CSV_DELIMITER)
            
            for row in records:
                # Get date
                date_str = (row.get("Date") or "")[:10]
                if not date_str:
                    continue
                
                # Filter by date range
                try:
                    row_date = datetime.fromisoformat(date_str).date()
                    start_of_day = start_date.date()
                    end_of_day = end_date.date()
                    if row_date < start_of_day:
                        continue
                    if row_date > end_of_day:
                        continue
                except ValueError:
                    continue
                
                # Deduplicate using key columns
                key = tuple((row.get(col) or "").strip() for col in self.DEDUP_KEY_COLS)
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                
                # Get download type and count
                download_type = (row.get("Download Type") or "").strip().lower()
                count = self.parse_int_safe(row.get("Counts") or row.get("Unique Devices"))
                
                # Initialize date metrics
                if date_str not in metrics_by_date:
                    metrics_by_date[date_str] = {
                        "first_time_download": 0,
                        "redownload": 0,
                        "updates": 0,
                    }
                
                # Categorize by download type
                if "update" in download_type:
                    metrics_by_date[date_str]["updates"] += count
                elif "first" in download_type and "time" in download_type:
                    metrics_by_date[date_str]["first_time_download"] += count
                elif "redownload" in download_type:
                    metrics_by_date[date_str]["redownload"] += count
            
            # Clean up CSV file
            os.remove(csv_path)
        
        return metrics_by_date
    
    def _process_deletes_instance(
        self,
        instance_id: str,
        temp_dir: str,
        start_date: datetime,
        end_date: datetime,
    ) -> dict[str, int]:
        """Process deletes instance and extract deletion counts.
        
        Args:
            instance_id: Report instance ID
            temp_dir: Temporary directory for CSV files
            start_date: Start date for filtering
            end_date: End date for filtering
            
        Returns:
            Dictionary mapping dates to deletion counts
        """
        url = f"{self.url_base}/analyticsReportInstances/{instance_id}/segments"
        
        try:
            segments = self.http_get(url).get("data", [])
        except HTTPError as e:
            if e.response.status_code == 404:
                self.logger.warning(
                    f"Segments not found for deletes instance {instance_id} - "
                    f"instance may still be processing. Skipping."
                )
                return {}
            raise
        
        deletes_by_date: dict[str, int] = {}
        
        for i, segment in enumerate(segments):
            attrs = segment.get("attributes", {}) or {}
            seg_url = attrs.get("url")
            
            if not seg_url:
                continue
            
            # Download and extract CSV
            csv_path = os.path.join(temp_dir, f"deletes_{instance_id}_{i}.csv")
            self.download_and_extract_csv(seg_url, csv_path)
            
            # Parse CSV
            records = self.parse_csv_file(csv_path, delimiter=self.CSV_DELIMITER)
            
            for row in records:
                # Only process Delete events
                if (row.get("Event") or "").strip() != "Delete":
                    continue
                
                # Get date
                date_str = (row.get("Date") or "")[:10]
                if not date_str:
                    continue
                
                # Filter by date range
                try:
                    row_date = datetime.fromisoformat(date_str).date()
                    start_of_day = start_date.date()
                    end_of_day = end_date.date()
                    if row_date < start_of_day:
                        continue
                    if row_date > end_of_day:
                        continue
                except ValueError:
                    continue
                
                # Add to count
                count = self.parse_int_safe(row.get("Counts"))
                deletes_by_date[date_str] = deletes_by_date.get(date_str, 0) + count
            
            # Clean up CSV file
            os.remove(csv_path)
        
        return deletes_by_date
    
    def _process_sessions_instance(
        self,
        instance_id: str,
        temp_dir: str,
        start_date: datetime,
        end_date: datetime,
    ) -> dict[str, dict[str, int]]:
        """Process sessions instance and extract session/device metrics.
        
        Args:
            instance_id: Report instance ID
            temp_dir: Temporary directory for CSV files
            start_date: Start date for filtering
            end_date: End date for filtering
            
        Returns:
            Dictionary mapping dates to session metrics
        """
        url = f"{self.url_base}/analyticsReportInstances/{instance_id}/segments"
        
        try:
            segments = self.http_get(url).get("data", [])
        except HTTPError as e:
            if e.response.status_code == 404:
                self.logger.warning(
                    f"Segments not found for sessions instance {instance_id} - "
                    f"instance may still be processing. Skipping."
                )
                return {}
            raise
        
        metrics_by_date: dict[str, dict[str, int]] = {}
        
        for i, segment in enumerate(segments):
            attrs = segment.get("attributes", {}) or {}
            seg_url = attrs.get("url")
            
            if not seg_url:
                continue
            
            # Download and extract CSV
            csv_path = os.path.join(temp_dir, f"sessions_{instance_id}_{i}.csv")
            self.download_and_extract_csv(seg_url, csv_path)
            
            # Parse CSV
            records = self.parse_csv_file(csv_path, delimiter=self.CSV_DELIMITER)
            
            for row in records:
                # Get date
                date_str = (row.get("Date") or "")[:10]
                if not date_str:
                    continue
                
                # Filter by date range
                try:
                    row_date = datetime.fromisoformat(date_str).date()
                    start_of_day = start_date.date()
                    end_of_day = end_date.date()
                    if row_date < start_of_day:
                        continue
                    if row_date > end_of_day:
                        continue
                except ValueError:
                    continue
                
                # Initialize metrics
                if date_str not in metrics_by_date:
                    metrics_by_date[date_str] = {
                        "total_sessions": 0,
                        "total_active_devices": 0,
                    }
                
                # Get sessions and active devices
                sessions = self.parse_int_safe(row.get("Sessions"))
                active_devices = self.parse_int_safe(row.get("Unique Devices"))
                
                metrics_by_date[date_str]["total_sessions"] += sessions
                metrics_by_date[date_str]["total_active_devices"] += active_devices
            
            # Clean up CSV file
            os.remove(csv_path)
        
        return metrics_by_date
