# Propel Data Platform

## Setup

1. Create a new Python 3.12 virtual environment.
2. Install the package in development mode:
   ```bash
   pip install -e '.[dev]'
   ```

## Local Deployment

Run workflows using the `db_workflow` command:

```bash
# Deploy and run a specific workflow
db_workflow deploy --env dev --client dorsia --workflow braze-canvas --run-now

# Deploy without running
db_workflow deploy --env dev --client dorsia --workflow braze-canvas

# Run all workflows for a client
db_workflow deploy --env dev --client dorsia

# Run all workflows for all clients
db_workflow deploy --env dev
```

## GitHub Deployment

1. Run secrets sync:
   ```bash
   ./sync_secrets.sh
   ```
2. Go to GitHub Actions and trigger the workflow manually
