# Repository Guide

Current coordinated release: **0.14.5**. Keep `_AGENT_VERSION` aligned with
the manager, engine, and external agents.

## Goal

`agent-wiki-production` exposes workspace-scoped `llm-wiki` production actions
over MCP. It runs allowlisted long-running jobs such as `doctor`, `ingest`,
`ingest_plan`, `ingest_apply`, `build`, `export`, `polish`, and the default
pipeline as background tasks.

Since 0.12.0 it is also a full **orchestrable agent** (planner + executor) for
the manager's agnostic orchestration: it implements `agent_describe`,
`agent_plan`, `agent_execute`, `agent_status` and `agent_cancel`. The
translation from orchestration operations (e.g. `knowledge.update`,
`document.build`) to internal production steps lives exclusively in this
server — Donna must never learn it. `agent_plan` resolves concrete input files
(never returns literal globs) and stamps mutating tasks with an
`idempotencyKey`; `agent_execute` persists key→job mappings in
`jobs/idempotency.json` so a retry with a known key returns the existing
job/result instead of starting a new one.

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
- Preserve scoped production locks when changing job execution: workspace-write
  for ingest/copy/ingest_apply/pipeline, read for ingest_plan, deliverable locks
  for targeted build/export/polish.
- Jobs are asynchronous. Tool calls should return a `jobId` quickly, then expose
  status and logs through follow-up tools.
- `production_start_job` and `production_job_status` must preserve their native
  payloads and include additive `_activity` metadata for manager/orchestrator
  polling. `_activity.poll` should point to `production.production_job_status`.
- Keep the default pipeline as `ingest`, `build`, `export`, then `polish`.
  The legacy `copy` step is available only when explicitly requested and
  configured.
- `production_start_job` supports targeted `inputs` for ingest/ingest_plan,
  plan-file `inputs` for ingest_apply, `templates` for build, and `deliverables`
  for export/polish. Ingest/copy/ingest_apply/pipeline hold a workspace-write
  lock; ingest_plan is read-only; targeted build/export/polish jobs hold
  deliverable locks so non-conflicting runtime tasks can execute in parallel.
- The `stabilize` flag on `production_start_job` applies only to `build` steps.
  It passes `--stabilize` to `wiki build`, which preserves unchanged sections
  verbatim and merges only changed sections via LLM. It is a no-op when no
  existing deliverable is present. Do not add `--stabilize` to `ingest`,
  `export`, or `polish` commands.
- Use `PRODUCTION_REQUIRE_CONFIRMATION=true` when a deployment needs an extra
  application-level guard for mutating jobs.
- **Concurrency levers** — the agent advertises its capacity in
  `agent_describe.limits`: `PRODUCTION_RECOMMENDED_CONCURRENCY` (default **4**)
  and `PRODUCTION_MAX_CONCURRENCY` (default **8**). The runtime takes the MIN of
  these, any manager ceiling (`WIKI_MANAGER_CAPABILITY_CONCURRENCY`) and per-task
  limits, so `recommendedConcurrency` is usually the effective number of tasks
  run in parallel — raising only `MAX` does nothing. `agent_plan` sizes its
  groups' `recommendedConcurrency` from the passed constraint capped by
  `_MAX_CONCURRENCY`. Real parallelism is then bounded by the locks above
  (`ingest_apply` is serialized). Profiles: low `2/4`, default `4/8`, high
  `8/16`; the wiki LLM backend must accept that many concurrent requests. Full
  guide: the manager's `docs/configuration.md` § "Parallelism & throughput".
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
  Current release line: `0.12.0`. Alignment is checked by
  `llm-wiki-manager/scripts/check-versions.js` and synced by the root
  `build-and-push.sh`.
- **Auth, scopes, rate limiting** (0.10.3): `MCP_AUTH_TOKEN` remains a legacy
  full-access (read+write) token; `MCP_READ_TOKEN`/`MCP_WRITE_TOKEN` grant
  scoped access instead. `_token_scopes` compares with `hmac.compare_digest`
  (constant-time). `_require_tool_scope` denies `_WRITE_TOOLS`
  (`agent_execute`, `agent_cancel`, `production_start_job`,
  `production_cancel_job`) to read-only callers;
  the current request's scope is threaded through a `contextvars.ContextVar`
  set by `_BearerAuthMiddleware`, not passed explicitly. Requests are
  rate-limited (`MCP_RATE_LIMIT_REQUESTS`/`MCP_RATE_LIMIT_WINDOW_SECONDS`,
  default 120/60s) keyed by token or remote IP. `_any_token_configured()` is
  the single "is any token set" check. This whole block is copy-pasted
  near-verbatim across all four agent repos plus `llm-wiki`'s `mcpHttp.ts`
  (TypeScript) — see `agent-cme/CLAUDE.md`'s fuller note on why that hasn't
  been consolidated into a shared package.
- **Multi-user status**: the wikiLLM workspace remains a single-user
  deployment baseline; the multi-user model is specified in
  `llm-wiki/docs/industrialisation.md` and planned next — see
  `agent-cme/CLAUDE.md`'s fuller note. This agent's token scoping is
  read/write, not per-user; do not deploy it as a shared endpoint for
  distinct end users before that lot lands.
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
