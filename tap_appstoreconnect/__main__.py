"""App Store Connect Analytics tap entry point.

This module serves as the command-line entry point when the tap is run as:
    python -m tap_appstoreconnect [args]
    
The TapAppStoreConnect.cli() method provides all Singer CLI functionality.
"""

from __future__ import annotations

from tap_appstoreconnect.tap import TapAppStoreConnect

TapAppStoreConnect.cli()
