# Repository Guide

## Goal

`agent-wiki-production` exposes workspace-scoped `llm-wiki` production actions
over MCP. It runs allowlisted long-running jobs such as `doctor`, `ingest`,
`build`, `export`, `polish`, and the default pipeline as background tasks.

## Architecture

- `production_mcp_server.py`: Starlette/uvicorn MCP server, bearer-auth
  middleware, HTML status page, tool definitions, job metadata, logs, progress,
  locks, cancellation, and subprocess execution.
- `Dockerfile`: extends `dotdrelle/llm-wiki:latest`, installs the Python MCP
  server dependencies, and runs inside the mounted workspace.
- `docker-compose.yml`: standalone local service. In normal use,
  `llm-wiki-manager` supplies the workspace mount and environment.
- `.env.example`: standalone configuration template.
- `.wiki/production-jobs/`: runtime job metadata, logs, and locks inside the
  mounted workspace.

## Constraints

- Never accept arbitrary shell commands. Only execute allowlisted production
  steps and the fixed command mappings in the server.
- One mutating job should run at a time per workspace. Preserve lock behavior
  when changing job execution.
- Jobs are asynchronous. Tool calls should return a `jobId` quickly, then expose
  status and logs through follow-up tools.
- `production_start_job` and `production_job_status` must preserve their native
  payloads and include additive `_activity` metadata for manager/orchestrator
  polling. `_activity.poll` should point to `production.production_job_status`.
- Keep the default pipeline as `ingest`, `build`, `export`, then `polish`.
  The legacy `copy` step is available only when explicitly requested and
  configured.
- Use `PRODUCTION_REQUIRE_CONFIRMATION=true` when a deployment needs an extra
  application-level guard for mutating jobs.
- LLM/model/provider secrets belong in the mounted workspace `.wikirc.yaml`, not
  in this service's README examples or code.
- Document path and token examples with placeholders such as
  `<absolute-path-to-llm-wiki-workspace>` and `<generated-local-token>`.

## Common Commands

```bash
docker compose --env-file .env up --build
```

When managed by `llm-wiki-manager`, use the manager wrapper:

```bash
./wiki-workspace up <workspace>
./wiki-workspace wiki <workspace> logs
./wiki-workspace wiki <workspace> doctor
./wiki-workspace wiki <workspace> build
```
