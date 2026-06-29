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
- The `stabilize` flag on `production_start_job` applies only to `build` steps.
  It passes `--stabilize` to `wiki build`, which preserves unchanged sections
  verbatim and merges only changed sections via LLM. It is a no-op when no
  existing deliverable is present. Do not add `--stabilize` to `ingest`,
  `export`, or `polish` commands.
- Use `PRODUCTION_REQUIRE_CONFIRMATION=true` when a deployment needs an extra
  application-level guard for mutating jobs.
- `production_start_job` accepts optional `configPath` (a `.wikirc.*` filename
  relative to the workspace root) to select a config profile for a single job.
  When supplied, the subprocess receives `WIKI_CONFIG_PATH=<configPath>`.
- `production_start_job` accepts optional `callerLabel` (max 120 chars) to
  identify the originating agent in job logs. The server logs
  `[start] ... caller=<callerLabel>` when set.
- The subprocess always receives `WIKI_RUN_CALLER=<job_id>` so `llm-wiki` CLI
  trace files can link back to the production job that launched them.
- Keep `_AGENT_VERSION` aligned with the coordinated `llm-wiki-manager`
  release version so status responses identify the deployed agent bundle.
  Current release line: `0.6.47`.
- MCP tool descriptions, `_activity` metadata, progress details, status page
  text, and logs intended for operators must stay in English. The workspace
  `.wikirc` language affects only LLM-generated wiki/deliverable content.
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
