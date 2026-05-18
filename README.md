# agent-wiki-production

Workspace-scoped MCP server for running `llm-wiki` production jobs.

This agent is intentionally separate from `llm-wiki` chat/search. It mounts one
workspace at `/workspace`, exposes a small allowlisted set of production actions,
and runs long operations as background jobs.

## Tools

| Tool                        | Purpose                                                                                 |
| --------------------------- | --------------------------------------------------------------------------------------- |
| `production_status`         | Check workspace, allowlist, active lock, and recent jobs.                               |
| `production_list_templates` | List templates, expected deliverables, and unmatched deliverables.                      |
| `production_start_job`      | Start `copy`, `ingest`, `build`, `export`, `polish`, or a pipeline as a background job. |
| `production_job_status`     | Read one job status.                                                                    |
| `production_job_logs`       | Read the tail of one job log.                                                           |
| `production_cancel_job`     | Cancel a running job.                                                                   |
| `production_list_jobs`      | List recent jobs.                                                                       |

## Configuration

```bash
cp .env.example .env
```

Required:

```bash
export WORKSPACE_NAME=example
export WIKI_WORKSPACE_PATH=/absolute/path/to/llm-wiki-workspace
```

Optional:

```bash
export PRODUCTION_ALLOWED_STEPS=copy,ingest,build,export,polish,pipeline
export PRODUCTION_REQUIRE_CONFIRMATION=true
export MCP_AUTH_TOKEN=local-token
```

## Run Locally

```bash
docker compose --env-file .env up --build
```

The MCP endpoint is:

```txt
http://localhost:3336/mcp/
```

Browsers can open the endpoint to view the status page. MCP clients should send
Streamable HTTP requests to the same URL.

## Behavior

- Jobs are asynchronous and return a `jobId` immediately.
- One mutating job can run at a time per workspace.
- Job metadata and logs are written under `.wiki/production-jobs`.
- `production_job_status` includes a structured `progress` object derived from
  the llm-wiki trace file when available: phase, label, detail, percent,
  current ingest source, template/deliverable, batch index/count, and last trace
  event.
- Cancelling a running job terminates the active process, marks unfinished steps
  as `cancelled`, clears the workspace lock, and appends a cancellation log
  entry.
- The server enforces the step allowlist. It never accepts arbitrary shell commands.
- Real jobs require `confirm=true` when confirmation is enabled.
- `build` jobs accept an optional `templates` array, for example
  `["EAE-REAS-architecture.md"]`, so a targeted build does not rebuild every
  template.
- `export` and `polish` jobs require a `deliverables` array, for example
  `["EAE-REAS-architecture.md"]`, so export/polish runs only on the requested
  deliverable.
