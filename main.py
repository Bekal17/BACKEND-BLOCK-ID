"""
Main entrypoint for the Backend BlockID agent.

Runs the 24/7 agent: starts the Solana listener, analysis pipeline,
API server, and worker loop. Use this for production or local long-running
processes.

For API-only mode:
    uvicorn backend_blockid.api_server.app:app --host 0.0.0.0 --port 8000
"""


def main() -> None:
    """Start the agent (listener + analysis + API + worker)."""
    # Delegate to agent_worker.runner once implemented
    from backend_blockid.agent_worker.runner import run_agent
    run_agent()


if __name__ == "__main__":
    main()
