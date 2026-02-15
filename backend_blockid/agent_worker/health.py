"""
Health checks and liveness/readiness probes.

Responsibilities:
- Check connectivity to Solana RPC, database, and optional dependencies.
- Expose liveness (process alive) and readiness (ready to serve traffic)
  for Kubernetes or load balancers.
"""
