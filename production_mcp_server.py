#!/usr/bin/env python3
"""llm-wiki production MCP server with background jobs."""

import asyncio
import contextlib
import json
import os
import shutil
import signal
import time
import uuid
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from mcp.server import Server
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from mcp.types import TextContent, Tool
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import HTMLResponse, PlainTextResponse
from starlette.routing import Mount
from starlette.types import Receive, Scope, Send
import uvicorn


app = Server("agent-wiki-production")

_MCP_TOKEN = os.environ.get("MCP_AUTH_TOKEN", "")
_WORKSPACE_NAME = os.environ.get("WORKSPACE_NAME", "workspace")
_WORKSPACE_PATH = Path(os.environ.get("WIKI_WORKSPACE_PATH", "/workspace")).resolve()
_IMPORTS = os.environ.get("WIKI_IMPORTS", "")
_IMPORT_PATH_MAPPINGS = os.environ.get("PRODUCTION_IMPORT_PATH_MAPPINGS", "../agent-cme/data=/agent-cme-data")
_ALLOWED_STEPS = {
    item.strip()
    for item in os.environ.get("PRODUCTION_ALLOWED_STEPS", "copy,ingest,build,export,polish,pipeline").split(",")
    if item.strip()
}
_REQUIRE_CONFIRMATION = os.environ.get("PRODUCTION_REQUIRE_CONFIRMATION", "true").lower() not in {"0", "false", "no"}
_JOBS_DIR = Path(os.environ.get("PRODUCTION_JOBS_DIR", str(_WORKSPACE_PATH / ".wiki" / "production-jobs"))).resolve()
_LOCKS_DIR = Path(os.environ.get("PRODUCTION_LOCKS_DIR", str(_JOBS_DIR / "locks"))).resolve()
_WIKI_BIN = os.environ.get("WIKI_BIN", "/app/bin/wiki.js")
_ACTIVE_TASKS: dict[str, asyncio.Task[None]] = {}
_ACTIVE_PROCESSES: dict[str, asyncio.subprocess.Process] = {}

_STEP_COMMANDS: dict[str, list[str]] = {
    "ingest": ["node", _WIKI_BIN, "ingest"],
    "build": ["node", _WIKI_BIN, "build"],
    "export": ["node", _WIKI_BIN, "export"],
    "polish": ["node", _WIKI_BIN, "export", "--polish"],
}
_MUTATING_STEPS = {"copy", "ingest", "build", "export", "polish", "pipeline"}

if not _MCP_TOKEN:
    print("[production-mcp] Warning: MCP_AUTH_TOKEN is not configured; the endpoint accepts unauthenticated clients.")


class _BearerAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.method == "GET" and _wants_html(request):
            return await call_next(request)
        if _MCP_TOKEN:
            auth = request.headers.get("authorization", "")
            if auth != f"Bearer {_MCP_TOKEN}":
                return PlainTextResponse("Unauthorized", status_code=401)
        return await call_next(request)


def _wants_html(request: Request) -> bool:
    return "text/html" in request.headers.get("accept", "")


def _escape_html(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def _json_text(payload: dict[str, Any]) -> list[TextContent]:
    return [TextContent(type="text", text=json.dumps(payload, ensure_ascii=False, indent=2))]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_dirs() -> None:
    (_JOBS_DIR / "jobs").mkdir(parents=True, exist_ok=True)
    (_JOBS_DIR / "logs").mkdir(parents=True, exist_ok=True)
    _LOCKS_DIR.mkdir(parents=True, exist_ok=True)


def _job_path(job_id: str) -> Path:
    return _JOBS_DIR / "jobs" / f"{job_id}.json"


def _log_path(job_id: str) -> Path:
    return _JOBS_DIR / "logs" / f"{job_id}.log"


def _lock_path() -> Path:
    return _LOCKS_DIR / f"{_WORKSPACE_NAME}.lock"


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _append_log(job_id: str, line: str) -> None:
    path = _log_path(job_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line.rstrip("\n") + "\n")


def _load_job(job_id: str) -> dict[str, Any]:
    path = _job_path(job_id)
    if not path.exists():
        raise ValueError(f"Unknown jobId: {job_id}")
    return _read_json(path)


def _save_job(job: dict[str, Any]) -> None:
    job["updatedAt"] = _now()
    _write_json(_job_path(str(job["jobId"])), job)


def _recent_jobs(limit: int = 10) -> list[dict[str, Any]]:
    _ensure_dirs()
    jobs = []
    for path in (_JOBS_DIR / "jobs").glob("*.json"):
        with contextlib.suppress(Exception):
            jobs.append(_read_json(path))
    jobs.sort(key=lambda item: str(item.get("createdAt", "")), reverse=True)
    return jobs[: max(1, min(limit, 100))]


def _read_lock() -> dict[str, Any] | None:
    path = _lock_path()
    if not path.exists():
        return None
    with contextlib.suppress(Exception):
        return _read_json(path)
    return {"workspace": _WORKSPACE_NAME, "invalid": True}


def _active_lock() -> dict[str, Any] | None:
    lock = _read_lock()
    if not lock:
        return None
    job_id = str(lock.get("jobId", ""))
    if job_id in _ACTIVE_TASKS and not _ACTIVE_TASKS[job_id].done():
        return lock
    with contextlib.suppress(Exception):
        job = _load_job(job_id)
        if job.get("status") in {"queued", "running", "cancelling"}:
            job["status"] = "stale"
            job["finishedAt"] = _now()
            job["error"] = "Agent restarted while job was active."
            _save_job(job)
    with contextlib.suppress(FileNotFoundError):
        _lock_path().unlink()
    return None


def _create_lock(job_id: str) -> None:
    _ensure_dirs()
    _write_json(_lock_path(), {"workspace": _WORKSPACE_NAME, "jobId": job_id, "createdAt": _now()})


def _clear_lock(job_id: str) -> None:
    lock = _read_lock()
    if lock and str(lock.get("jobId")) == job_id:
        with contextlib.suppress(FileNotFoundError):
            _lock_path().unlink()


def _parse_imports() -> list[Path]:
    return [_map_import_path(item.strip()) for item in _IMPORTS.split("|") if item.strip()]


def _map_import_path(value: str) -> Path:
    for mapping in _IMPORT_PATH_MAPPINGS.split("|"):
        if "=" not in mapping:
            continue
        host_prefix, container_prefix = mapping.split("=", 1)
        host_prefix = host_prefix.rstrip("/")
        container_prefix = container_prefix.rstrip("/")
        if value == host_prefix or value.startswith(f"{host_prefix}/"):
            return Path(container_prefix + value[len(host_prefix) :]).expanduser()
    return Path(value).expanduser()


def _render_landing_page(endpoint_url: str, scheme: str) -> str:
    auth_status = (
        "Bearer token enabled"
        if _MCP_TOKEN
        else "Warning: MCP_AUTH_TOKEN is not configured; the endpoint accepts unauthenticated clients."
    )
    lock = _active_lock()
    lock_status = f"busy ({lock.get('jobId')})" if lock else "available"
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>agent-wiki-production MCP connector</title>
  <style>
    :root {{ color-scheme: light dark; --bg:#f8fafc; --panel:#fff; --text:#111827; --muted:#64748b; --line:#d8dee8; --accent:#2563eb; --code:#eef2ff; }}
    @media (prefers-color-scheme: dark) {{ :root {{ --bg:#0f172a; --panel:#111827; --text:#f8fafc; --muted:#94a3b8; --line:#253044; --accent:#60a5fa; --code:#1e293b; }} }}
    * {{ box-sizing: border-box; }}
    body {{ margin:0; min-height:100vh; background:var(--bg); color:var(--text); font:15px/1.55 ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; }}
    main {{ width:min(920px,calc(100% - 32px)); margin:0 auto; padding:56px 0; }}
    .eyebrow {{ color:var(--accent); font-weight:700; letter-spacing:.04em; text-transform:uppercase; font-size:12px; }}
    h1 {{ margin:8px 0 10px; font-size:clamp(32px,6vw,52px); line-height:1.05; letter-spacing:0; }}
    h2 {{ margin:0 0 16px; font-size:20px; letter-spacing:0; }}
    .lead {{ margin:0 0 28px; color:var(--muted); max-width:720px; font-size:17px; }}
    .panel {{ background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:22px; margin:18px 0; }}
    dl {{ display:grid; grid-template-columns:170px 1fr; gap:10px 18px; margin:0; }}
    dt {{ color:var(--muted); }}
    dd {{ margin:0; min-width:0; overflow-wrap:anywhere; }}
    code {{ background:var(--code); border:1px solid var(--line); border-radius:6px; padding:2px 6px; font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace; font-size:13px; }}
    ul {{ list-style:none; margin:0; padding:0; display:grid; gap:10px; }}
    li {{ display:grid; grid-template-columns:minmax(170px,240px) 1fr; gap:14px; align-items:start; padding:12px 0; border-top:1px solid var(--line); }}
    li:first-child {{ border-top:0; padding-top:0; }}
    li span {{ color:var(--muted); }}
    @media (max-width:640px) {{ dl,li {{ grid-template-columns:1fr; }} main {{ padding:32px 0; }} }}
  </style>
</head>
<body>
  <main>
    <div class="eyebrow">MCP Streamable HTTP</div>
    <h1>agent-wiki-production MCP connector</h1>
    <p class="lead">Workspace-scoped production jobs for llm-wiki. Commands are allowlisted and run asynchronously.</p>
    <section class="panel">
      <dl>
        <dt>Status</dt><dd>Ready</dd>
        <dt>Endpoint</dt><dd><code>{_escape_html(endpoint_url)}</code></dd>
        <dt>Transport</dt><dd>MCP Streamable HTTP over {_escape_html(scheme.upper())}</dd>
        <dt>Authentication</dt><dd>{_escape_html(auth_status)}</dd>
        <dt>Workspace</dt><dd><code>{_escape_html(_WORKSPACE_NAME)}</code> at <code>{_escape_html(str(_WORKSPACE_PATH))}</code></dd>
        <dt>Lock</dt><dd>{_escape_html(lock_status)}</dd>
        <dt>Allowed steps</dt><dd><code>{_escape_html(','.join(sorted(_ALLOWED_STEPS)))}</code></dd>
      </dl>
    </section>
    <section class="panel">
      <h2>Available tools</h2>
      <ul>
        <li><code>production_status</code><span>Check workspace and job readiness.</span></li>
        <li><code>production_list_templates</code><span>List build templates and matching deliverables.</span></li>
        <li><code>production_start_job</code><span>Start an allowlisted production job and get a jobId.</span></li>
        <li><code>production_job_status</code><span>Read one job status.</span></li>
        <li><code>production_job_logs</code><span>Read a log tail.</span></li>
        <li><code>production_cancel_job</code><span>Cancel a running job.</span></li>
        <li><code>production_list_jobs</code><span>List recent jobs.</span></li>
      </ul>
    </section>
  </main>
</body>
</html>"""


@app.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="production_status",
            description="Check agent-wiki-production configuration, active lock, allowed steps, and recent jobs.",
            inputSchema={"type": "object", "properties": {}, "additionalProperties": False},
        ),
        Tool(
            name="production_list_templates",
            description="List available llm-wiki templates and their expected deliverable outputs for targeted build/export/polish jobs.",
            inputSchema={
                "type": "object",
                "properties": {
                    "includeDeliverables": {
                        "type": "boolean",
                        "description": "Include existing deliverables that are not directly matched to a template.",
                    },
                },
                "additionalProperties": False,
            },
        ),
        Tool(
            name="production_start_job",
            description=(
                "Start an llm-wiki production job asynchronously. Use only after explicit user request. "
                "Mutating jobs require confirm=true when confirmation is enabled."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": ["copy", "ingest", "build", "export", "polish", "pipeline"],
                    },
                    "steps": {
                        "type": "array",
                        "items": {"type": "string", "enum": ["copy", "ingest", "build", "export", "polish"]},
                        "description": "Required for type=pipeline. Ordered steps to run.",
                    },
                    "templates": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional template files for build steps, relative to the workspace root or templates/.",
                    },
                    "deliverables": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Required for export or polish steps. Paths are relative to the workspace root or deliverables/.",
                    },
                    "confirm": {"type": "boolean", "description": "Set true after explicit user approval."},
                    "dryRun": {"type": "boolean", "description": "Validate and return the planned job without running it."},
                },
                "required": ["type"],
                "additionalProperties": False,
            },
        ),
        Tool(
            name="production_job_status",
            description="Read status for one production job.",
            inputSchema={
                "type": "object",
                "properties": {"jobId": {"type": "string"}},
                "required": ["jobId"],
                "additionalProperties": False,
            },
        ),
        Tool(
            name="production_job_logs",
            description="Read a production job log tail.",
            inputSchema={
                "type": "object",
                "properties": {
                    "jobId": {"type": "string"},
                    "tail": {"type": "integer", "minimum": 1, "maximum": 500},
                },
                "required": ["jobId"],
                "additionalProperties": False,
            },
        ),
        Tool(
            name="production_cancel_job",
            description="Cancel a running production job.",
            inputSchema={
                "type": "object",
                "properties": {"jobId": {"type": "string"}},
                "required": ["jobId"],
                "additionalProperties": False,
            },
        ),
        Tool(
            name="production_list_jobs",
            description="List recent production jobs for this workspace.",
            inputSchema={
                "type": "object",
                "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 100}},
                "additionalProperties": False,
            },
        ),
    ]


@app.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
    start = time.time()
    print(f"[production-mcp] tools/call {name}")
    try:
        match name:
            case "production_status":
                result = _tool_status()
            case "production_list_templates":
                result = _tool_list_templates(arguments)
            case "production_start_job":
                result = await _tool_start_job(arguments)
            case "production_job_status":
                result = _tool_job_status(arguments)
            case "production_job_logs":
                result = _tool_job_logs(arguments)
            case "production_cancel_job":
                result = await _tool_cancel_job(arguments)
            case "production_list_jobs":
                result = _tool_list_jobs(arguments)
            case _:
                raise ValueError(f"Unknown tool: {name}")
        print(f"[production-mcp] tools/result {name} ok {int((time.time() - start) * 1000)}ms")
        return result
    except Exception as exc:
        print(f"[production-mcp] tools/result {name} error {int((time.time() - start) * 1000)}ms {exc}")
        return _json_text({"ok": False, "error": str(exc)})


def _tool_status() -> list[TextContent]:
    return _json_text(
        {
            "ok": True,
            "service": "agent-wiki-production",
            "workspace": {"name": _WORKSPACE_NAME, "path": str(_WORKSPACE_PATH), "exists": _WORKSPACE_PATH.exists()},
            "allowedSteps": sorted(_ALLOWED_STEPS),
            "requireConfirmation": _REQUIRE_CONFIRMATION,
            "importsConfigured": len(_parse_imports()),
            "activeLock": _active_lock(),
            "recentJobs": [_job_summary(job) for job in _recent_jobs(5)],
        }
    )


def _tool_list_templates(args: dict[str, Any]) -> list[TextContent]:
    include_deliverables = bool(args.get("includeDeliverables", True))
    templates_dir = _WORKSPACE_PATH / "templates"
    deliverables_dir = _WORKSPACE_PATH / "deliverables"
    templates: list[dict[str, Any]] = []
    expected_outputs: set[str] = set()

    if templates_dir.exists():
        for path in sorted(templates_dir.rglob("*.md")):
            rel_template = _relative_workspace_path(path)
            rel_under_templates = path.relative_to(templates_dir).as_posix()
            output = _template_output(path, rel_under_templates)
            expected_outputs.add(output)
            templates.append(
                {
                    "template": rel_under_templates,
                    "templatePath": rel_template,
                    "deliverable": output,
                    "deliverablePath": f"deliverables/{output}",
                    "deliverableExists": (deliverables_dir / output).exists(),
                }
            )

    deliverables: list[dict[str, Any]] = []
    if include_deliverables and deliverables_dir.exists():
        for path in sorted(deliverables_dir.rglob("*.md")):
            rel_deliverable = path.relative_to(deliverables_dir).as_posix()
            if rel_deliverable in expected_outputs:
                continue
            deliverables.append(
                {
                    "deliverable": rel_deliverable,
                    "deliverablePath": _relative_workspace_path(path),
                }
            )

    return _json_text(
        {
            "ok": True,
            "workspace": _WORKSPACE_NAME,
            "templatesDir": "templates",
            "deliverablesDir": "deliverables",
            "templates": templates,
            "unmatchedDeliverables": deliverables,
        }
    )


async def _tool_start_job(args: dict[str, Any]) -> list[TextContent]:
    job_type = str(args.get("type", "")).strip()
    dry_run = bool(args.get("dryRun", False))
    confirm = bool(args.get("confirm", False))
    steps = _resolve_steps(job_type, args.get("steps"))
    templates = _resolve_string_list(args.get("templates"), "templates")
    deliverables = _resolve_string_list(args.get("deliverables"), "deliverables")

    if job_type not in _ALLOWED_STEPS:
        raise ValueError(f"Job type is not allowed: {job_type}")
    for step in steps:
        if step not in _ALLOWED_STEPS:
            raise ValueError(f"Step is not allowed: {step}")
    if not _WORKSPACE_PATH.exists():
        raise ValueError(f"Workspace path does not exist: {_WORKSPACE_PATH}")
    if any(step in {"export", "polish"} for step in steps) and not deliverables:
        raise ValueError("export and polish jobs require at least one deliverable.")

    plan = {
        "type": job_type,
        "steps": steps,
        "workspace": _WORKSPACE_NAME,
        "templates": templates,
        "deliverables": deliverables,
    }
    if dry_run:
        return _json_text({"ok": True, "dryRun": True, "plan": plan, "commands": _command_labels(steps, templates, deliverables)})
    if _REQUIRE_CONFIRMATION and not confirm and any(step in _MUTATING_STEPS for step in steps):
        raise ValueError("Production jobs require confirm=true after explicit user approval.")

    active = _active_lock()
    if active:
        return _json_text({"ok": False, "error": "workspace_busy", "activeJobId": active.get("jobId"), "lock": active})

    _ensure_dirs()
    job_id = f"prod_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    job = {
        "jobId": job_id,
        "workspace": _WORKSPACE_NAME,
        "type": job_type,
        "templates": templates,
        "deliverables": deliverables,
        "steps": [{"name": step, "status": "pending"} for step in steps],
        "status": "queued",
        "createdAt": _now(),
        "updatedAt": _now(),
        "startedAt": None,
        "finishedAt": None,
        "exitCode": None,
        "error": None,
        "logPath": str(_log_path(job_id)),
    }
    _save_job(job)
    _create_lock(job_id)
    task = asyncio.create_task(_run_job(job_id))
    _ACTIVE_TASKS[job_id] = task
    return _json_text({"ok": True, "jobId": job_id, "status": "queued", "plan": plan})


def _tool_job_status(args: dict[str, Any]) -> list[TextContent]:
    job_id = str(args.get("jobId", "")).strip()
    job = _load_job(job_id)
    return _json_text({"ok": True, "job": _with_duration(job), "logTail": _read_log_tail(job_id, 30)})


def _tool_job_logs(args: dict[str, Any]) -> list[TextContent]:
    job_id = str(args.get("jobId", "")).strip()
    tail = int(args.get("tail", 100))
    _load_job(job_id)
    return _json_text({"ok": True, "jobId": job_id, "tail": _read_log_tail(job_id, tail)})


async def _tool_cancel_job(args: dict[str, Any]) -> list[TextContent]:
    job_id = str(args.get("jobId", "")).strip()
    job = _load_job(job_id)
    if job.get("status") not in {"queued", "running"}:
        return _json_text({"ok": False, "error": "job_not_running", "job": _with_duration(job)})
    job["status"] = "cancelling"
    _save_job(job)
    proc = _ACTIVE_PROCESSES.get(job_id)
    if proc and proc.returncode is None:
        with contextlib.suppress(ProcessLookupError):
            proc.send_signal(signal.SIGTERM)
    task = _ACTIVE_TASKS.get(job_id)
    if task and not task.done():
        task.cancel()
    await asyncio.sleep(0)
    job["status"] = "cancelled"
    job["finishedAt"] = _now()
    job["exitCode"] = -15
    _append_log(job_id, "[cancelled] Job cancelled by request.")
    _save_job(job)
    _clear_lock(job_id)
    return _json_text({"ok": True, "job": _with_duration(job)})


def _tool_list_jobs(args: dict[str, Any]) -> list[TextContent]:
    limit = int(args.get("limit", 20))
    jobs = [_job_summary(_with_duration(job)) for job in _recent_jobs(limit)]
    return _json_text({"ok": True, "workspace": _WORKSPACE_NAME, "jobs": jobs})


def _resolve_steps(job_type: str, raw_steps: Any) -> list[str]:
    if job_type == "pipeline":
        if not isinstance(raw_steps, list) or not raw_steps:
            return ["copy", "ingest", "build", "export", "polish"]
        steps = [str(item).strip() for item in raw_steps if str(item).strip()]
    elif job_type in {"copy", "ingest", "build", "export", "polish"}:
        steps = [job_type]
    else:
        raise ValueError(f"Unknown production job type: {job_type}")
    invalid = [step for step in steps if step not in {"copy", "ingest", "build", "export", "polish"}]
    if invalid:
        raise ValueError(f"Unknown production step: {invalid[0]}")
    return steps


def _resolve_string_list(value: Any, name: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"{name} must be an array of strings.")
    items = [str(item).strip() for item in value if str(item).strip()]
    for item in items:
        if item.startswith("-"):
            raise ValueError(f"Invalid {name} path: {item}")
    return items


def _relative_workspace_path(path: Path) -> str:
    return path.relative_to(_WORKSPACE_PATH).as_posix()


def _template_output(path: Path, fallback: str) -> str:
    text = path.read_text(encoding="utf-8", errors="replace")
    if not text.startswith("---"):
        return fallback
    end = text.find("\n---", 3)
    if end < 0:
        return fallback
    frontmatter = text[3:end].splitlines()
    for line in frontmatter:
        key, sep, value = line.partition(":")
        if sep and key.strip() == "output":
            output = value.strip().strip("'\"")
            if output:
                return output
    return fallback


async def _run_job(job_id: str) -> None:
    job = _load_job(job_id)
    job["status"] = "running"
    job["startedAt"] = _now()
    _save_job(job)
    _append_log(job_id, f"[start] workspace={_WORKSPACE_NAME} type={job['type']}")
    try:
        for index, step in enumerate(job["steps"]):
            step["status"] = "running"
            step["startedAt"] = _now()
            _save_job(job)
            _append_log(job_id, f"[step:start] {step['name']}")
            if step["name"] == "copy":
                count = await asyncio.to_thread(_run_copy_step, job_id)
                step["result"] = {"copiedFiles": count}
                step["exitCode"] = 0
            else:
                exit_code = await _run_cli_step(
                    job_id,
                    step["name"],
                    job.get("templates", []),
                    job.get("deliverables", []),
                )
                step["exitCode"] = exit_code
                if exit_code != 0:
                    raise RuntimeError(f"Step failed: {step['name']} exitCode={exit_code}")
            step["status"] = "done"
            step["finishedAt"] = _now()
            _save_job(job)
            _append_log(job_id, f"[step:done] {step['name']}")
            if index < len(job["steps"]) - 1:
                _append_log(job_id, "")
        job["status"] = "done"
        job["exitCode"] = 0
    except asyncio.CancelledError:
        job["status"] = "cancelled"
        job["exitCode"] = -15
        raise
    except Exception as exc:
        job["status"] = "failed"
        job["exitCode"] = 1
        job["error"] = str(exc)
        _append_log(job_id, f"[error] {exc}")
    finally:
        job["finishedAt"] = _now()
        for step in job["steps"]:
            if step.get("status") == "running":
                step["status"] = job["status"]
                step["finishedAt"] = _now()
        _save_job(job)
        _clear_lock(job_id)
        _ACTIVE_PROCESSES.pop(job_id, None)
        _ACTIVE_TASKS.pop(job_id, None)
        _append_log(job_id, f"[finish] status={job['status']} exitCode={job['exitCode']}")


def _run_copy_step(job_id: str) -> int:
    imports = _parse_imports()
    if not imports:
        _append_log(job_id, "[copy] No WIKI_IMPORTS configured.")
        return 0
    target = _WORKSPACE_PATH / "raw" / "untracked"
    target.mkdir(parents=True, exist_ok=True)
    copied = 0
    for source in imports:
        source = source if source.is_absolute() else _WORKSPACE_PATH / source
        source = source.resolve()
        if not source.is_dir():
            raise ValueError(f"Import path is not a directory: {source}")
        _append_log(job_id, f"[copy] {source} -> {target}")
        for src in source.rglob("*.md"):
            rel = src.relative_to(source)
            dst = target / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
            copied += 1
    _append_log(job_id, f"[copy] copiedFiles={copied}")
    return copied


async def _run_cli_step(job_id: str, step: str, templates: list[str], deliverables: list[str]) -> int:
    for command in _step_commands(step, templates, deliverables):
        _append_log(job_id, f"[cmd] cwd={_WORKSPACE_PATH} {' '.join(command)}")
        proc = await asyncio.create_subprocess_exec(
            *command,
            cwd=str(_WORKSPACE_PATH),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        _ACTIVE_PROCESSES[job_id] = proc
        if proc.stdout is None:
            raise RuntimeError("subprocess stdout is None; expected PIPE")
        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            _append_log(job_id, line.decode("utf-8", errors="replace").rstrip("\n"))
        exit_code = await proc.wait()
        if exit_code != 0:
            return exit_code
    return 0


def _read_log_tail(job_id: str, tail: int) -> list[str]:
    path = _log_path(job_id)
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return lines[-max(1, min(tail, 500)) :]


def _with_duration(job: dict[str, Any]) -> dict[str, Any]:
    out = dict(job)
    start = out.get("startedAt") or out.get("createdAt")
    end = out.get("finishedAt") or _now()
    try:
        start_dt = datetime.fromisoformat(str(start))
        end_dt = datetime.fromisoformat(str(end))
        out["durationSeconds"] = round((end_dt - start_dt).total_seconds(), 3)
    except Exception:
        out["durationSeconds"] = None
    return out


def _job_summary(job: dict[str, Any]) -> dict[str, Any]:
    return {
        "jobId": job.get("jobId"),
        "workspace": job.get("workspace"),
        "type": job.get("type"),
        "status": job.get("status"),
        "createdAt": job.get("createdAt"),
        "updatedAt": job.get("updatedAt"),
        "durationSeconds": job.get("durationSeconds"),
        "error": job.get("error"),
    }


def _step_commands(step: str, templates: list[str], deliverables: list[str]) -> list[list[str]]:
    if step == "copy":
        return []
    command = [*_STEP_COMMANDS[step]]
    if step == "build":
        command.extend(templates)
        return [command]
    if step in {"export", "polish"}:
        return [[*command, deliverable] for deliverable in deliverables]
    return [command]


def _command_labels(steps: list[str], templates: list[str], deliverables: list[str]) -> list[str]:
    labels: list[str] = []
    for step in steps:
        if step == "copy":
            labels.append(f"copy {len(_parse_imports())} import(s) to raw/untracked")
            continue
        labels.extend(" ".join(command) for command in _step_commands(step, templates, deliverables))
    return labels


def create_starlette_app() -> Starlette:
    host = os.environ.get("MCP_HOST", "0.0.0.0")
    port = int(os.environ.get("MCP_PORT", "8080"))
    streamable_http = StreamableHTTPSessionManager(
        app=app,
        event_store=None,
        json_response=False,
        stateless=True,
    )

    async def handle_mcp(scope: Scope, receive: Receive, send: Send) -> None:
        path = scope.get("path", "")
        if path not in {"/mcp", "/mcp/"}:
            response = PlainTextResponse("Not found", status_code=404)
            await response(scope, receive, send)
            return
        request = Request(scope, receive)
        if request.method == "GET" and _wants_html(request):
            scheme = "https" if request.url.scheme == "https" else "http"
            endpoint_url = f"{scheme}://{request.headers.get('host', f'{host}:{port}')}/mcp/"
            response = HTMLResponse(_render_landing_page(endpoint_url, scheme))
            await response(scope, receive, send)
            return
        mcp_scope = dict(scope)
        mcp_scope["path"] = "/"
        mcp_scope["root_path"] = f"{scope.get('root_path', '').rstrip('/')}/mcp"
        await streamable_http.handle_request(mcp_scope, receive, send)

    @contextlib.asynccontextmanager
    async def lifespan(_app: Starlette) -> AsyncIterator[None]:
        _ensure_dirs()
        _active_lock()
        async with streamable_http.run():
            yield

    middleware = [
        Middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]),
        Middleware(_BearerAuthMiddleware),
    ]
    return Starlette(routes=[Mount("/", app=handle_mcp)], middleware=middleware, lifespan=lifespan)


def main() -> None:
    host = os.environ.get("MCP_HOST", "0.0.0.0")
    port = int(os.environ.get("MCP_PORT", "8080"))
    print(f"[production-mcp] Streamable HTTP on http://{host}:{port}/mcp")
    print(f"[production-mcp] workspace={_WORKSPACE_NAME} path={_WORKSPACE_PATH}")
    uvicorn.run(create_starlette_app(), host=host, port=port)


if __name__ == "__main__":
    main()
