"""
Application settings and environment configuration.

Responsibilities:
- Load configuration from environment variables and .env files.
- Validate required settings and provide defaults for optional ones.
- Expose typed settings (Solana RPC URL, API port, DB URL, etc.)
  for use across listener, analysis engine, API server, and worker.
"""

# Placeholder: implement get_settings() returning a settings object/dataclass.
# Example: pydantic BaseSettings or dataclasses + os.getenv.


def get_settings():
    """
    Return the current application settings.

    Returns:
        Settings object with attributes such as solana_rpc_url,
        api_host, api_port, database_url, log_level, etc.
    """
    raise NotImplementedError("Config layer: implement get_settings()")
