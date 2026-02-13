"""JWT authentication for App Store Connect API.

This module handles JWT token generation using ES256 algorithm
for authenticating with the App Store Connect API.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import jwt

if TYPE_CHECKING:
    from tap_appstoreconnect.client import AppStoreConnectStream


class JWTAuthenticator:
    """Authenticator for App Store Connect API using JWT tokens.
    
    Generates JWT tokens signed with ES256 algorithm for API authentication.
    Tokens are valid for 15 minutes and automatically refreshed.
    """

    def __init__(
        self,
        stream: AppStoreConnectStream,
        issuer_id: str,
        key_id: str,
        private_key: str,
    ) -> None:
        """Initialize JWT authenticator.
        
        Args:
            stream: Parent stream instance
            issuer_id: App Store Connect API Issuer ID
            key_id: App Store Connect API Key ID
            private_key: Private key in PEM format for signing
        """
        self.stream = stream
        self.issuer_id = issuer_id
        self.key_id = key_id
        self.private_key = private_key
        self._token: str | None = None
        self._token_expires_at: float = 0

    def get_token(self) -> str:
        """Get valid JWT token, generating new one if expired.
        
        Returns:
            Valid JWT token string
        """
        # Return cached token if still valid (with 1 minute buffer)
        if self._token and time.time() < (self._token_expires_at - 60):
            return self._token

        # Generate new token
        self._token = self._generate_token()
        return self._token

    def _generate_token(self) -> str:
        """Generate new JWT token.
        
        Returns:
            Signed JWT token
        """
        now = int(time.time())
        
        # Token payload
        payload = {
            "iss": self.issuer_id,
            "iat": now,
            "exp": now + (15 * 60),  # 15 minutes expiration
            "aud": "appstoreconnect-v1",
        }
        
        # Token headers
        headers = {
            "kid": self.key_id,
            "alg": "ES256",
            "typ": "JWT",
        }
        
        # Sign token with ES256
        token = jwt.encode(
            payload,
            self.private_key,
            algorithm="ES256",
            headers=headers,
        )
        
        # Update expiration time
        self._token_expires_at = float(now + (15 * 60))
        
        self.stream.logger.debug("Generated new JWT token (expires in 15 minutes)")
        
        return token

    def __call__(self, request):
        """Add JWT token to request headers.
        
        Args:
            request: HTTP request object
            
        Returns:
            Modified request with Authorization header
        """
        request.headers["Authorization"] = f"Bearer {self.get_token()}"
        return request
