"""Tests standard tap features using the built-in SDK tests library."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest
from singer_sdk.testing import get_tap_test_class

from tap_appstoreconnect.tap import TapAppStoreConnect

# Sample configuration for testing
SAMPLE_CONFIG = {
    "issuer_id": os.getenv("ASC_ISSUER_ID", "de6ee153-778d-444d-bc05-8fd0bc34de4f"),
    "key_id": os.getenv("ASC_KEY_ID", "W4U2LTT7BY"),
    "private_key": os.getenv(
        "ASC_PRIVATE_KEY",
        """-----BEGIN PRIVATE KEY-----
MIGTAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBHkwdwIBAQQgnHki4conP6YGq67H
nUbnAl8zvvbR3KDF3Zdk98xpJnCgCgYIKoZIzj0DAQehRANCAAR6/vFHxtJzUKNQ
w17ctRIjzmGT4ZF2jc/PAWTKBzGZ3/53oJoJbLtxWhJ9SnSfbJgiiRqqqLYby4Wr
HQVlFP3X
-----END PRIVATE KEY-----"""
    ),
    "app_id": os.getenv("ASC_APP_ID", "6463405199"),
    "start_date": (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d"),
    "end_date": (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d"),
}

# Standard SDK tests
TestTapAppStoreConnect = get_tap_test_class(
    tap_class=TapAppStoreConnect,
    config=SAMPLE_CONFIG,
)



class TestTapAppStoreConnectCustom:
    """Custom tests for tap-appstoreconnect."""
    
    @pytest.fixture
    def tap_instance(self):
        """Create tap instance for testing."""
        return TapAppStoreConnect(config=SAMPLE_CONFIG)
    
    def test_config_validation(self, tap_instance):
        """Test that configuration is valid."""
        assert tap_instance.config.get("issuer_id")
        assert tap_instance.config.get("key_id")
        assert tap_instance.config.get("private_key")
        assert tap_instance.config.get("app_id")
    
    def test_stream_discovery(self, tap_instance):
        """Test that streams are discovered correctly."""
        streams = tap_instance.discover_streams()
        
        assert len(streams) > 0, "No streams discovered"
        
        stream_names = [stream.name for stream in streams]
        assert "app_analytics" in stream_names
    
    def test_stream_schema(self, tap_instance):
        """Test that stream schema is valid."""
        streams = tap_instance.discover_streams()
        stream = streams[0]
        
        schema = stream.schema
        
        # Check required fields
        assert "type" in schema
        assert "properties" in schema
        
        # Check that primary key is in schema
        for pk in stream.primary_keys:
            assert pk in schema["properties"], f"Primary key {pk} not in schema"
        
        # Check that replication key is in schema
        if stream.replication_key:
            assert stream.replication_key in schema["properties"], \
                f"Replication key {stream.replication_key} not in schema"
    
    def test_date_properties(self, tap_instance):
        """Test date property accessors."""
        start = tap_instance.start_date_value
        end = tap_instance.end_date_value
        
        assert isinstance(start, datetime)
        assert isinstance(end, datetime)
        assert start < end, "Start date should be before end date"
    
    def test_authentication(self, tap_instance):
        """Test that authentication is configured."""
        streams = tap_instance.discover_streams()
        stream = streams[0]
        
        authenticator = stream.authenticator
        assert authenticator is not None
        
        # Test token generation
        token = authenticator.get_token()
        assert token is not None
        assert len(token) > 0
