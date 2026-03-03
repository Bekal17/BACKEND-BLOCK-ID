from __future__ import annotations

import os

import uvicorn

from backend_blockid.api_server.server import app


def main() -> None:
    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "8000"))
    uvicorn.run(app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()
