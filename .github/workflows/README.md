# BlockID CI/CD

## Required GitHub Secrets

| Secret | Description |
|--------|-------------|
| `DOCKER_USER` | Docker Hub username (for push) |
| `DOCKER_PASSWORD` | Docker Hub password or token |
| `SERVER_HOST` | Deploy server hostname or IP |
| `SERVER_USER` | SSH username |
| `SERVER_KEY` | SSH private key (PEM) |
| `DEPLOY_PATH` | (Optional) Server path to project, default `~/blockid` |

## Workflow

1. **CI** (every push/PR): Lint, test, pipeline in TEST_MODE
2. **Build** (main only): Docker build and push to Docker Hub
3. **Deploy** (main only): SSH to server, `docker compose pull && up -d`
   - Skipped if commit message contains `[canary]`
4. **Deploy Canary** (main only, when commit has `[canary]`): Deploy to K8s canary (10% traffic), wait, optional promote
5. **Notify** (on failure): Placeholder for future Telegram/email

## BlockID Canary

Manual: Actions → BlockID Canary → Run workflow with `image_tag` (e.g. `main-abc1234`).

- Deploys new image to `blockid-api-canary` (1 replica, ~10% traffic)
- Waits `wait_minutes` (default 30)
- If `auto_promote`: promotes canary to stable
- On failure: scales canary to 0

## Artifacts

Test reports and pipeline logs are uploaded on each run (7-day retention).
