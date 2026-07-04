# agent-wiki-production

[![License: PolyForm Noncommercial 1.0.0](https://img.shields.io/badge/license-PolyForm%20Noncommercial%201.0.0-blue)](LICENSE)

Workspace-scoped MCP server for running `llm-wiki` production jobs.

This agent is intentionally separate from `llm-wiki` chat/search. It mounts one
workspace at `/workspace`, exposes a small allowlisted set of production actions,
and runs long operations as background jobs.

## Tools

| Tool                        | Purpose                                                                                 |
| --------------------------- | --------------------------------------------------------------------------------------- |
| `production_status`         | Check workspace, allowlist, active lock, and recent jobs.                               |
| `production_list_templates` | List templates, expected deliverables, and unmatched deliverables.                      |
| `production_start_job`      | Start `doctor`, `copy`, `ingest`, `build`, `export`, `polish`, or a pipeline as a background job. |
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
export WORKSPACE_NAME=<workspace-name>
export WIKI_WORKSPACE_PATH=<absolute-path-to-llm-wiki-workspace>
```

Optional:

```bash
export PRODUCTION_ALLOWED_STEPS=doctor,copy,ingest,build,export,polish,pipeline
export PRODUCTION_REQUIRE_CONFIRMATION=false
export MCP_AUTH_TOKEN=<generated-local-token>
export WIKI_CONFIG_PATH=.wikirc.yaml.openai
export WIKI_IMPORTS=
```

`MCP_AUTH_TOKEN`, `WIKI_CONFIG_PATH`, and `WIKI_IMPORTS` default to empty strings in the standalone Docker Compose file. Leave `WIKI_IMPORTS` empty unless you explicitly use the legacy `copy` step.

`agent-wiki-production` runs the `llm-wiki` CLI inside the mounted workspace.
Configure LLM and vector provider keys in that workspace's `.wikirc.yaml`
(`llm.apiKey` and, when needed, `retrieval.vector.apiKey`). `production_start_job`
also accepts `configPath` to select a workspace-local profile such as
`.wikirc.yaml.openai` for a single job, and optional `callerLabel` (max 120
chars) to identify the originating agent in job logs.

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
- Mutating jobs use scoped locks: ingest/copy/pipeline take the workspace-write
  lock, targeted build jobs lock their expected deliverables, and export/polish
  jobs lock the requested deliverables. Non-conflicting targeted jobs can run in
  parallel.
- Job metadata and logs are written under `.wiki/production-jobs`.
- `production_job_status` includes a structured `progress` object derived from
  the llm-wiki trace file when available: phase, label, detail, percent,
  current ingest source, template/deliverable, batch index/count, and last trace
  event.
- `production_start_job` and `production_job_status` include additive
  `_activity` metadata with `poll.server=production` and
  `poll.tool=production_job_status`, so manager shells can monitor jobs without
  hard-coding production-specific status parsing.
- Cancelling a running job terminates the active process, marks unfinished steps
  as `cancelled`, clears the workspace lock, and appends a cancellation log
  entry.
- The server enforces the step allowlist. It never accepts arbitrary shell commands.
- The default `pipeline` runs `ingest`, `build`, `export`, then `polish`.
  The legacy `copy` step is available only when requested explicitly, for
  deployments that configure `WIKI_IMPORTS` and import path mappings.
- Bearer authentication controls who can call the agent. `PRODUCTION_REQUIRE_CONFIRMATION`
  is an optional extra application-level guard: set it to `true` if mutating jobs
  must also include `confirm=true` after explicit user approval.
- For scoped HTTP access, set `MCP_READ_TOKEN` for status/list/log clients and
  `MCP_WRITE_TOKEN` for clients allowed to start or cancel jobs. `MCP_AUTH_TOKEN`
  remains a legacy full-access read+write token. Rate limiting defaults to 120
  requests per 60 seconds and can be tuned with `MCP_RATE_LIMIT_REQUESTS` and
  `MCP_RATE_LIMIT_WINDOW_SECONDS`.
- `build` jobs accept an optional `templates` array, for example
  `["EAE-REAS-architecture.md"]`, so a targeted build does not rebuild every
  template.
- `ingest` jobs accept an optional `inputs` array, for example
  `["raw/untracked/doc-a.md", "doc-b.md"]`, so one runtime task can ingest a
  restricted source subset.
- `build` and `pipeline` jobs accept `stabilize: true` to pass
  `wiki build --stabilize`; existing deliverables keep unchanged sections
  verbatim while changed sections are merged from the fresh candidate.
- `export` and `polish` jobs require a `deliverables` array, for example
  `["EAE-REAS-architecture.md"]`, so export/polish runs only on the requested
  deliverable.

## License

Released under the **PolyForm Noncommercial License 1.0.0**. See [LICENSE](LICENSE).
