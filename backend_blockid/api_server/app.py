"""
FastAPI/ASGI application entrypoint.

Build and configure the ASGI app; mount routes from server.
Run with: uvicorn backend_blockid.api_server.app:app --host 0.0.0.0 --port 8000
"""

from backend_blockid.api_server.server import app

__all__ = ["app"]
