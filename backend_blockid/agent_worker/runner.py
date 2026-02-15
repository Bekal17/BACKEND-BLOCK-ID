"""
Agent runner — main event loop and process lifecycle.

Responsibilities:
- Start and supervise the Solana listener, analysis pipeline, and API server
  (or connect to a message queue for distributed workers).
- Run the main event loop (asyncio or threading) and handle graceful shutdown.
- Emit or expose health status for monitoring.
"""


def run_agent() -> None:
    """
    Start the 24/7 agent: listener, analysis engine, API server, and worker loop.
    Blocks until shutdown (SIGINT/SIGTERM).
    """
    raise NotImplementedError(
        "Implement run_agent(): start listener, analysis pipeline, API, and event loop."
    )
