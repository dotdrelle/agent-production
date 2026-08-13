#!/usr/bin/env python3
"""llm-wiki production MCP server with background jobs."""

import asyncio
import contextvars
import contextlib
import hmac
import json
import os
import re
import shutil
import signal
import time
import uuid
import hashlib
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
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

_AGENT_VERSION = "0.15.48"
_MCP_TOKEN = os.environ.get("MCP_AUTH_TOKEN", "")
_MCP_READ_TOKEN = os.environ.get("MCP_READ_TOKEN", "")
_MCP_WRITE_TOKEN = os.environ.get("MCP_WRITE_TOKEN", "")
_CURRENT_SCOPES: contextvars.ContextVar[set[str]] = contextvars.ContextVar("mcp_scopes", default={"read", "write"})
_RATE_LIMIT_REQUESTS = int(os.environ.get("MCP_RATE_LIMIT_REQUESTS", "120"))
_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("MCP_RATE_LIMIT_WINDOW_SECONDS", "60"))
_RATE_BUCKETS: dict[str, list[float]] = {}


def _int_env(name: str, default: int) -> int:
    try:
        return max(0, int(os.environ.get(name, str(default))))
    except ValueError:
        return default


_WORKSPACE_NAME = os.environ.get("WORKSPACE_NAME", "workspace")
_WORKSPACE_PATH = Path(os.environ.get("WIKI_WORKSPACE_PATH", "/workspace")).resolve()
_AGENT_INSTANCE_ID = os.environ.get("PRODUCTION_INSTANCE_ID", "production-main")
# Concurrency the agent advertises to the orchestrator (agent_describe.limits).
# These are the PRIMARY throughput levers: the runtime takes the MIN of the
# agent's recommendedConcurrency, its maxConcurrency, any manager ceiling
# (WIKI_MANAGER_CAPABILITY_CONCURRENCY) and per-task limits — so it is
# recommendedConcurrency that usually sets how many tasks run in parallel.
# Defaults are intermediate (effective ≈ 4 parallel): enough to actually use a
# capable machine instead of idling, while staying safe on a single LLM backend.
# Tune per docs/configuration.md — low profile 2/4, high profile 8/16. The LLM
# endpoint must accept this many concurrent requests, and ingest_apply stays
# serialized regardless (global workspace-write lock).
_RECOMMENDED_CONCURRENCY = _int_env("PRODUCTION_RECOMMENDED_CONCURRENCY", 4)
_MAX_CONCURRENCY = _int_env("PRODUCTION_MAX_CONCURRENCY", 8)
_MAX_TASKS_PER_PLAN = _int_env("PRODUCTION_MAX_TASKS_PER_PLAN", 0)
_MAX_TASK_DURATION_MS = _int_env("PRODUCTION_MAX_TASK_DURATION_MS", 0)
_LOG_PREFIX = f"[production-mcp/{_WORKSPACE_NAME}]"
_IMPORTS = os.environ.get("WIKI_IMPORTS", "")
_IMPORT_PATH_MAPPINGS = os.environ.get("PRODUCTION_IMPORT_PATH_MAPPINGS", "")
_ALLOWED_STEPS = {
    item.strip()
    for item in os.environ.get("PRODUCTION_ALLOWED_STEPS", "doctor,copy,ingest,ingest_plan,ingest_apply,taxonomy,build,export,polish,restore,pipeline").split(",")
    if item.strip()
}
_REQUIRE_CONFIRMATION = os.environ.get("PRODUCTION_REQUIRE_CONFIRMATION", "true").lower() not in {"0", "false", "no"}
_JOBS_DIR = Path(os.environ.get("PRODUCTION_JOBS_DIR", str(_WORKSPACE_PATH / ".wiki" / "production-jobs"))).resolve()
_LOCKS_DIR = Path(os.environ.get("PRODUCTION_LOCKS_DIR", str(_JOBS_DIR / "locks"))).resolve()
_WIKI_BIN = os.environ.get("WIKI_BIN", "/app/bin/wiki.js")
_ACTIVE_TASKS: dict[str, asyncio.Task[None]] = {}
_ACTIVE_PROCESSES: dict[str, asyncio.subprocess.Process] = {}

_STEP_COMMANDS: dict[str, list[str]] = {
    "doctor": ["node", _WIKI_BIN, "doctor"],
    "ingest": ["node", _WIKI_BIN, "ingest"],
    "ingest_plan": ["node", _WIKI_BIN, "ingest", "--plan-only"],
    "ingest_apply": ["node", _WIKI_BIN, "ingest", "--apply"],
    "taxonomy": ["node", _WIKI_BIN, "taxonomy", "--apply"],
    "build": ["node", _WIKI_BIN, "build"],
    "export": ["node", _WIKI_BIN, "export"],
    "polish": ["node", _WIKI_BIN, "export", "--polish"],
    "restore": ["node", _WIKI_BIN, "restore"],
}
_MUTATING_STEPS = {"copy", "ingest", "ingest_plan", "ingest_apply", "taxonomy", "build", "export", "polish", "restore", "pipeline"}
_CAPABILITY_STEP_MAP: dict[str, list[str]] = {
    "knowledge.update": ["ingest", "ingest_plan", "ingest_apply", "taxonomy"],
    "document.build": ["build"],
    "document.publish": ["export", "polish"],
    "workspace.diagnose": ["doctor"],
    "knowledge.pipeline": ["pipeline"],
    "workspace.restore": ["restore"],
}
_AGENT_OPERATION_TRANSLATION: dict[str, dict[str, Any]] = {
    "doctor": {"type": "doctor"},
    "ingest": {"type": "ingest"},
    "ingest_plan": {"type": "ingest_plan"},
    "ingest_apply": {"type": "ingest_apply"},
    "taxonomy": {"type": "taxonomy"},
    "build": {"type": "build"},
    "export": {"type": "export"},
    "polish": {"type": "polish"},
    "pipeline": {"type": "pipeline"},
    "restore": {"type": "restore"},
}

_WRITE_TOOLS = {"agent_execute", "agent_cancel", "production_start_job", "production_cancel_job"}

def _any_token_configured() -> bool:
    return bool(_MCP_TOKEN or _MCP_READ_TOKEN or _MCP_WRITE_TOKEN)


if not _any_token_configured():
    print(f"{_LOG_PREFIX} Warning: MCP_AUTH_TOKEN is not configured; the endpoint accepts unauthenticated clients.")


def _bearer_token(request: Request) -> str:
    auth = request.headers.get("authorization", "")
    return auth[7:] if auth.lower().startswith("bearer ") else ""


def _token_scopes(token: str) -> set[str] | None:
    if not _any_token_configured():
        return {"read", "write"}
    if _MCP_TOKEN and hmac.compare_digest(token, _MCP_TOKEN):
        return {"read", "write"}
    if _MCP_WRITE_TOKEN and hmac.compare_digest(token, _MCP_WRITE_TOKEN):
        return {"read", "write"}
    if _MCP_READ_TOKEN and hmac.compare_digest(token, _MCP_READ_TOKEN):
        return {"read"}
    return None


def _require_tool_scope(name: str) -> list[TextContent] | None:
    if name in _WRITE_TOOLS and "write" not in _CURRENT_SCOPES.get():
        return _json_text({"ok": False, "error": f"token does not have write scope for {name}"})
    return None


def _rate_limit_key(request: Request, token: str) -> str:
    forwarded = request.headers.get("x-forwarded-for", "").split(",")[0].strip()
    host = forwarded or (request.client.host if request.client else "unknown")
    return f"token:{token}" if token else f"ip:{host}"


def _rate_limited(key: str) -> bool:
    now = time.time()
    cutoff = now - max(1, _RATE_LIMIT_WINDOW_SECONDS)
    bucket = [item for item in _RATE_BUCKETS.get(key, []) if item > cutoff]
    if len(bucket) >= max(1, _RATE_LIMIT_REQUESTS):
        _RATE_BUCKETS[key] = bucket
        return True
    bucket.append(now)
    _RATE_BUCKETS[key] = bucket
    return False


class _BearerAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.method == "GET" and _wants_html(request):
            return await call_next(request)
        token_value = _bearer_token(request)
        scopes = _token_scopes(token_value)
        if scopes is None:
            return PlainTextResponse("Unauthorized", status_code=401)
        if _rate_limited(_rate_limit_key(request, token_value)):
            return PlainTextResponse("Rate limit exceeded", status_code=429)
        token = _CURRENT_SCOPES.set(scopes)
        try:
            return await call_next(request)
        finally:
            _CURRENT_SCOPES.reset(token)


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


def _json_payload(result: list[TextContent]) -> dict[str, Any]:
    if not result:
        return {}
    return json.loads(result[0].text)


def _mask_secret_text(value: Any) -> str:
    text = str(value)
    text = re.sub(r"(?i)(authorization:\s*bearer\s+)[^\s,;]+", r"\1***", text)
    text = re.sub(r"(?i)(api[_-]?key|token|secret|password)(['\"]?\s*[:=]\s*['\"]?)[^'\"\s,;]+", r"\1\2***", text)
    return text


def _terminal_status(status: Any) -> bool:
    return str(status or "").lower() in {"done", "failed", "cancelled", "canceled", "complete", "completed", "success", "error"}


def _active_status(status: Any) -> bool:
    return str(status or "").lower() in {"queued", "running", "cancelling"}


def _agent_request_workspace(args: dict[str, Any]) -> dict[str, Any]:
    workspace = args.get("workspace")
    if not isinstance(workspace, dict):
        raise ValueError("agent_execute requires a workspace object.")
    requested_path = workspace.get("path")
    if requested_path:
        resolved = Path(str(requested_path)).expanduser().resolve()
        if resolved != _WORKSPACE_PATH:
            raise ValueError(f"Requested workspace path does not match this agent workspace: {resolved}")
    name = str(workspace.get("name") or "").strip()
    if not name:
        name = _WORKSPACE_NAME
    return {
        "name": name,
        **({"path": str(requested_path)} if requested_path else {}),
        **({"revision": str(workspace.get("revision"))} if workspace.get("revision") is not None else {}),
    }


def _agent_start_job_args(args: dict[str, Any], workspace: dict[str, Any]) -> dict[str, Any]:
    operation = str(args.get("operation") or "").strip()
    translation = _AGENT_OPERATION_TRANSLATION.get(operation)
    if translation is None:
        raise ValueError(f"Unsupported agent operation: {operation}")
    arguments = args.get("arguments") if isinstance(args.get("arguments"), dict) else {}
    job_args: dict[str, Any] = dict(translation)
    for source_key, target_key in [
        ("inputs", "inputs"),
        ("templates", "templates"),
        ("deliverables", "deliverables"),
        ("steps", "steps"),
        ("stabilize", "stabilize"),
        ("configPath", "configPath"),
        ("callerLabel", "callerLabel"),
        ("file", "restoreFile"),
        ("to", "restoreRevision"),
        ("dryRun", "executeDryRun"),
        ("run", "restoreRun"),
    ]:
        if source_key in arguments:
            job_args[target_key] = arguments[source_key]
    if args.get("taskId") and not job_args.get("callerLabel"):
        job_args["callerLabel"] = str(args["taskId"])
    if "confirm" in arguments:
        job_args["confirm"] = bool(arguments["confirm"])
    else:
        job_args["confirm"] = True
    if args.get("idempotencyKey"):
        job_args["idempotencyKey"] = str(args["idempotencyKey"])
    if args.get("runId"):
        job_args["runId"] = str(args["runId"])
    if args.get("capability"):
        job_args["capability"] = str(args["capability"])
    return job_args


def _agent_task_status(job: dict[str, Any], progress: dict[str, Any]) -> dict[str, Any]:
    status = str(job.get("status") or "unknown")
    # agent_status is authoritative for orchestration state, but it must not
    # narrow the richer production progress contract. The runtime merges this
    # response into the existing activity, so dropping batch/throttle/trace
    # fields here made them disappear after every status poll.
    status_progress = {
        **progress,
        "percent": _agent_progress_percent(progress, status),
        **_agent_processing_progress(job, progress),
    }
    payload: dict[str, Any] = {
        "jobId": job.get("jobId"),
        "taskId": job.get("callerLabel"),
        "agentInstanceId": _AGENT_INSTANCE_ID,
        "workspace": {"name": str(job.get("workspace") or _WORKSPACE_NAME)},
        "operation": job.get("type"),
        "status": status,
        "progress": status_progress,
        "startedAt": job.get("startedAt"),
        "updatedAt": job.get("updatedAt"),
        "finishedAt": job.get("finishedAt"),
    }
    if _terminal_status(status):
        payload["result"] = _agent_task_result(job, progress)
    return payload


def _agent_processing_progress(job: dict[str, Any], progress: dict[str, Any]) -> dict[str, Any]:
    raw_steps = job.get("steps") if isinstance(job.get("steps"), list) else []
    steps = [
        {
            "id": str(step.get("name") or f"step-{index + 1}"),
            "name": str(step.get("name") or f"Step {index + 1}"),
            "status": str(step.get("status") or "pending"),
            "startedAt": step.get("startedAt"),
            "finishedAt": step.get("finishedAt"),
        }
        for index, step in enumerate(raw_steps)
        if isinstance(step, dict)
    ]
    active_index = next(
        (index for index, step in enumerate(steps) if step["status"] in {"running", "queued", "pending"}),
        len(steps) - 1 if steps else 0,
    )
    batch_index = progress.get("batchIndex")
    batch_count = progress.get("batchCount")
    wait_ms = progress.get("waitMs")
    retry_at = progress.get("retryAt")
    last_event = str(progress.get("lastEvent") or "")
    throttling_active = bool(wait_ms or retry_at or last_event in _WAIT_EVENTS or last_event == "provider:throttle")
    processing = {
        key: value
        for key, value in {
            "sourceIndex": progress.get("sourceIndex"),
            "sourceCount": progress.get("sourceCount"),
            "sourceDoneCount": progress.get("sourceDoneCount"),
            "instructionCount": progress.get("instructionCount"),
            "stabilizeKept": progress.get("stabilizeKept"),
            "stabilizeMerged": progress.get("stabilizeMerged"),
            "stabilizeInserted": progress.get("stabilizeInserted"),
            "stabilizeRemoved": progress.get("stabilizeRemoved"),
        }.items()
        if value is not None
    }
    return {
        "stepIndex": active_index + 1 if steps else None,
        "stepTotal": len(steps),
        "steps": steps,
        "batch": {
            "index": int(batch_index) + 1,
            "total": int(batch_count),
            "status": "done" if "complete" in str(progress.get("detail") or "").lower() else "running",
            "startedAt": progress.get("currentBatchStartedAt"),
        } if batch_index is not None and batch_count is not None else None,
        "throttling": {
            "active": throttling_active,
            "waitMs": wait_ms,
            "retryAt": retry_at,
            "event": last_event or None,
        },
        "processing": processing,
    }


def _agent_progress_percent(progress: dict[str, Any], status: str) -> int:
    percent = progress.get("percent")
    if isinstance(percent, (int, float)):
        return max(0, min(100, round(float(percent))))
    if status == "done":
        return 100
    if status in {"failed", "cancelled"}:
        return 0
    return 0


def _agent_task_result(job: dict[str, Any], progress: dict[str, Any] | None = None) -> dict[str, Any]:
    status = str(job.get("status") or "")
    trace_progress = progress or {}
    metrics: dict[str, Any] = {"durationMs": _duration_ms(job)}
    input_tokens = _int_field(trace_progress.get("inputTokens"))
    output_tokens = _int_field(trace_progress.get("outputTokens"))
    if input_tokens is not None:
        metrics["inputTokens"] = input_tokens
    if output_tokens is not None:
        metrics["outputTokens"] = output_tokens
    if input_tokens is not None or output_tokens is not None:
        metrics["totalTokens"] = (input_tokens or 0) + (output_tokens or 0)
    result: dict[str, Any] = {
        "status": _agent_result_status(status),
        "outputRefs": _agent_output_refs_for_job(job),
        "metrics": metrics,
    }
    if status in {"failed", "cancelled", "canceled", "error"}:
        result["error"] = _agent_error_from_job(job)
    return result


def _agent_result_status(status: str) -> str:
    if status in {"done", "complete", "completed", "success"}:
        return "succeeded"
    if status in {"cancelled", "canceled"}:
        return "cancelled"
    return "failed"


def _agent_output_refs_for_job(job: dict[str, Any]) -> list[dict[str, str]]:
    refs: list[dict[str, str]] = []
    seen: set[str] = set()
    for ref in _output_refs_for_job(job):
        value = str(ref.get("ref") or ref.get("path") or "")
        if not value or value in seen:
            continue
        seen.add(value)
        refs.append({"type": "file", "ref": value})
    return refs


def _duration_ms(job: dict[str, Any]) -> int:
    started = _parse_iso_datetime(job.get("startedAt") or job.get("createdAt"))
    finished = _parse_iso_datetime(job.get("finishedAt") or job.get("updatedAt"))
    if not started or not finished:
        return 0
    return max(0, round((finished - started).total_seconds() * 1000))


def _parse_iso_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    with contextlib.suppress(Exception):
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return None


def _agent_error_from_job(job: dict[str, Any]) -> dict[str, Any]:
    status = str(job.get("status") or "")
    if status in {"cancelled", "canceled"}:
        return {"code": "cancelled", "message": "Job cancelled.", "retryable": False}
    message = str(job.get("error") or "Execution failed.")
    return {
        "code": "execution_failed",
        "message": message,
        "retryable": _is_retryable_error(message),
    }


def _is_retryable_error(message: str) -> bool:
    return bool(re.search(r"(?i)\b(timeout|temporar|throttl|rate.?limit|quota|busy|unavailable)\b", message))


def _agent_description() -> dict[str, Any]:
    return {
        "contractVersion": "1",
        "agentType": "production",
        "agentInstanceId": _AGENT_INSTANCE_ID,
        "displayName": "Production",
        "capabilities": _agent_capabilities(),
        "orchestration": {
            "canPlan": True,
            "canExpandPlan": False,
            "canExecute": True,
            "canCancel": True,
            "canResume": False,
            "supportsIdempotency": True,
            "supportsParallelWorkers": True,
        },
        "limits": _agent_limits(),
        "health": {"status": "available" if _WORKSPACE_PATH.exists() else "unavailable"},
    }


def _agent_limits() -> dict[str, int]:
    limits = {
        "recommendedConcurrency": _RECOMMENDED_CONCURRENCY,
        "maxConcurrency": _MAX_CONCURRENCY,
    }
    if _MAX_TASKS_PER_PLAN:
        limits["maxTasksPerPlan"] = _MAX_TASKS_PER_PLAN
    if _MAX_TASK_DURATION_MS:
        limits["maxTaskDurationMs"] = _MAX_TASK_DURATION_MS
    return limits


def _agent_capabilities() -> list[dict[str, Any]]:
    capabilities: list[dict[str, Any]] = []
    for capability_id, steps in _CAPABILITY_STEP_MAP.items():
        supported = [step for step in steps if step in _ALLOWED_STEPS]
        if not supported:
            continue
        mutating = any(step in _MUTATING_STEPS for step in supported)
        capabilities.append(
            {
                "id": capability_id,
                "version": "1",
                "description": _capability_description(capability_id),
                "inputSchema": _capability_input_schema(capability_id, supported),
                "outputSchema": {"type": "object", "additionalProperties": True},
                "supportedOperations": supported,
                **({"mutationClass": "workspace", "defaultRequiresApproval": True} if mutating else {}),
            }
        )
    return capabilities


def _capability_description(capability_id: str) -> str:
    # Descriptions are written to be self-sufficient for ANY MCP operator
    # (our own orchestrator or a third-party host such as Claude): they state
    # what the capability does, what it reads/writes, and how it is split, so
    # no caller-side hardcoded knowledge is required.
    descriptions = {
        "knowledge.update": (
            "Ingest Markdown source files already present in the workspace (by default the pending files under raw/untracked) "
            "into the llm-wiki knowledge base. Reads local Markdown only — it never fetches from Confluence or any remote source. "
            "Files are processed per source file; the runtime decides ordering and how many run in parallel (callers do not set a batch size). "
            "Use knowledge.update to build or refresh wiki knowledge from documents that are already on disk."
        ),
        "document.build": (
            "Build llm-wiki deliverables from templates in templates/, writing the results under deliverables/. "
            "Use when the user wants to (re)generate a deliverable document from the current knowledge base and templates."
        ),
        "document.publish": (
            "Export or polish EXISTING llm-wiki deliverables from deliverables/ (final publication of wiki documents). "
            "This is not a source/Confluence export: to pull external source content, use the source-export agent instead."
        ),
        "workspace.diagnose": "Diagnose workspace configuration and runtime readiness (read-only doctor checks); it creates no jobs.",
        "workspace.restore": (
            "Restore a workspace file to a Git revision or revert one production run to its parent. "
            "This is a mutating operation, always workspace-scoped, and requires explicit approval."
        ),
        "knowledge.pipeline": "Run the full production pipeline (ingest → build → export/polish) over the workspace in one ordered sequence.",
    }
    return descriptions.get(capability_id, capability_id)


def _capability_input_schema(capability_id: str, supported: list[str]) -> dict[str, Any]:
    properties: dict[str, Any] = {
        "operation": {"type": "string", "enum": supported, "description": f"Which supported operation to run for this capability. One of: {', '.join(supported)}."},
        "configPath": {"type": "string", "description": "Optional .wikirc file path relative to the workspace root (e.g. .wikirc.yaml.openai). Omit to use the workspace default."},
        "callerLabel": {"type": "string", "description": "Optional identifier of the caller, logged with the job for traceability."},
    }
    if capability_id == "knowledge.update":
        properties["inputs"] = {"type": "array", "items": {"type": "string"}, "description": "Optional explicit list of Markdown source files (relative to the workspace root) to ingest. If omitted, all pending files under raw/untracked are ingested."}
    if capability_id == "document.build":
        properties["templates"] = {"type": "array", "items": {"type": "string"}, "description": "Optional template files (relative to the workspace root or templates/) to build. If omitted, all applicable templates are built."}
        properties["stabilize"] = {"type": "boolean", "description": "When true, run the build in stabilize mode (deterministic re-generation) instead of a normal build."}
    if capability_id == "document.publish":
        properties["deliverables"] = {"type": "array", "items": {"type": "string"}, "description": "Deliverable files (relative to the workspace root or deliverables/) to export or polish. These must already exist under deliverables/."}
    if capability_id == "knowledge.pipeline":
        properties["steps"] = {"type": "array", "items": {"type": "string"}, "description": "Ordered pipeline steps to run (e.g. ingest, build, export). If omitted, the default full pipeline order is used."}
    if capability_id == "workspace.restore":
        properties["file"] = {"type": "string", "description": "Workspace-relative file to restore (use with to)."}
        properties["to"] = {"type": "string", "description": "Git revision for file restore."}
        properties["run"] = {"type": "string", "description": "Git run commit to revert to its parent (alternative to file+to)."}
        properties["dryRun"] = {"type": "boolean", "description": "Validate the restore without writing files."}
    return {
        "type": "object",
        "properties": properties,
        # No "required": this schema describes the task-level `arguments`
        # object; `operation` is a sibling field of the task in the
        # orchestration contract. Requiring it here made Donna's plan
        # validator reject every fragment this very agent proposed
        # (invalid_arguments), which silently killed the parallel path.
        "required": [],
        "additionalProperties": False,
    }


def _agent_operations() -> list[str]:
    """Operations this agent will actually accept, from a single source.

    The plan schema used to hard-code five names while `_CAPABILITY_STEP_MAP`
    (declared right above it) routes `workspace.diagnose` to `doctor` and
    `agent_describe` advertises `doctor`, `copy`, `ingest_plan` and
    `ingest_apply` in `allowedSteps`. Donna planned the operation the agent had
    itself advertised and `agent_plan` rejected it with
    "'doctor' is not one of [...]". Deriving both schemas from the translation
    table intersected with `PRODUCTION_ALLOWED_STEPS` makes it impossible for
    what the agent advertises and what it validates to drift apart again.
    """
    return sorted(set(_AGENT_OPERATION_TRANSLATION) & _ALLOWED_STEPS)


def _agent_plan_input_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "capability": {"type": "string", "enum": sorted(_CAPABILITY_STEP_MAP)},
            "operation": {
                "type": "string",
                "enum": _agent_operations(),
            },
            "objective": {"type": "string"},
            "workspace": {
                "type": "object",
                "properties": {
                    "revision": {"type": "string"},
                },
                "additionalProperties": True,
            },
            "arguments": {
                "type": "object",
                "properties": {
                    "inputs": {"type": "array", "items": {"type": "string"}},
                    "templates": {"type": "array", "items": {"type": "string"}},
                    "deliverables": {"type": "array", "items": {"type": "string"}},
                    "steps": {"type": "array", "items": {"type": "string"}},
                    "stabilize": {"type": "boolean"},
                    "configPath": {"type": "string"},
                    "callerLabel": {"type": "string"},
                    "file": {"type": "string"},
                    "to": {"type": "string"},
                    "run": {"type": "string"},
                    "dryRun": {"type": "boolean"},
                },
                "additionalProperties": True,
            },
            "constraints": {
                "type": "object",
                "properties": {
                    "maxTasks": {"type": "integer", "minimum": 1},
                    "maxConcurrency": {"type": "integer", "minimum": 1},
                    "maxDepth": {"type": "integer", "minimum": 1},
                    "requireApprovalForMutations": {"type": "boolean"},
                },
                "additionalProperties": True,
            },
        },
        "additionalProperties": True,
    }


def _agent_execute_input_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "taskId": {"type": "string"},
            "runId": {"type": "string"},
            "capability": {"type": "string"},
            "idempotencyKey": {"type": "string"},
            "operation": {"type": "string", "enum": _agent_operations()},
            "workspace": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "path": {"type": "string"},
                    "revision": {"type": "string"},
                },
                "additionalProperties": True,
            },
            "arguments": {
                "type": "object",
                "properties": {
                    "inputs": {"type": "array", "items": {"type": "string"}},
                    "templates": {"type": "array", "items": {"type": "string"}},
                    "deliverables": {"type": "array", "items": {"type": "string"}},
                    "steps": {"type": "array", "items": {"type": "string"}},
                    "stabilize": {"type": "boolean"},
                    "configPath": {"type": "string"},
                    "callerLabel": {"type": "string"},
                    "file": {"type": "string"},
                    "to": {"type": "string"},
                    "run": {"type": "string"},
                    "dryRun": {"type": "boolean"},
                    "confirm": {"type": "boolean"},
                },
                "additionalProperties": True,
            },
            "constraints": {
                "type": "object",
                "properties": {
                    "requireApprovalForMutations": {"type": "boolean"},
                },
                "additionalProperties": True,
            },
        },
        "required": ["operation", "workspace"],
        "additionalProperties": True,
    }


def _agent_job_id_input_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {"jobId": {"type": "string"}},
        "required": ["jobId"],
        "additionalProperties": False,
    }


def _agent_status_input_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "jobId": {"type": "string", "minLength": 1},
            "capability": {"type": "string", "minLength": 1},
            "operation": {"type": "string", "minLength": 1},
        },
        "oneOf": [
            {"required": ["jobId"]},
            {"required": ["capability", "operation"]},
        ],
        "additionalProperties": False,
    }


def _activity_for_job(job: dict[str, Any], progress: dict[str, Any] | None = None) -> dict[str, Any]:
    job_id = str(job.get("jobId") or "")
    status = str(job.get("status") or (progress or {}).get("status") or "running")
    step = (progress or {}).get("phase") or (progress or {}).get("currentStep") or job.get("type") or "production"
    percent = (progress or {}).get("percent")
    label_parts = [
        "Production",
        str(job.get("type") or "job"),
        status,
        str(step) if step else None,
        f"{round(float(percent))}%" if isinstance(percent, (int, float)) else None,
    ]
    targets = [*(job.get("inputs") or []), *(job.get("templates") or []), *(job.get("deliverables") or [])]
    return {
        "id": job_id,
        "source": "production",
        "kind": str(job.get("type") or "job"),
        "label": " · ".join(part for part in label_parts if part),
        "status": status,
        "progress": progress or {"step": step},
        "targets": targets,
        "outputRefs": _output_refs_for_job(job),
        "poll": {
            "server": "production",
            "tool": "production_job_status",
            "args": {"jobId": job_id},
            "intervalMs": 2500,
        } if job_id and not _terminal_status(status) else None,
        "startedAt": job.get("startedAt") or job.get("createdAt"),
        "updatedAt": job.get("updatedAt") or _now(),
        "error": job.get("error"),
        "terminal": _terminal_status(status),
    }


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_dirs() -> None:
    (_JOBS_DIR / "jobs").mkdir(parents=True, exist_ok=True)
    (_JOBS_DIR / "logs").mkdir(parents=True, exist_ok=True)
    _LOCKS_DIR.mkdir(parents=True, exist_ok=True)


def _idempotency_path() -> Path:
    return _JOBS_DIR / "idempotency.json"


def _job_path(job_id: str) -> Path:
    return _JOBS_DIR / "jobs" / f"{job_id}.json"


def _log_path(job_id: str) -> Path:
    return _JOBS_DIR / "logs" / f"{job_id}.log"


def _lock_path(scope: str = "workspace-write", job_id: str | None = None) -> Path:
    digest = hashlib.sha256(scope.encode("utf-8")).hexdigest()[:16]
    safe_workspace = re.sub(r"[^A-Za-z0-9_.-]+", "_", _WORKSPACE_NAME)
    # "read" is a shared scope: each read-only holder gets its own lock file so
    # multiple concurrent read jobs (e.g. per-file ingest --plan-only) coexist
    # instead of colliding on a single per-scope file (FileExistsError ->
    # target_busy). Exclusive scopes keep one per-scope file whose atomic
    # creation is the mutex; concurrency is decided by _conflicting_lock.
    if scope == "read" and job_id is not None:
        safe_job = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(job_id))
        return _LOCKS_DIR / safe_workspace / f"{digest}.{safe_job}.lock"
    return _LOCKS_DIR / safe_workspace / f"{digest}.lock"


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(f"{path.suffix}.tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp_path.replace(path)


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
    _update_idempotency_status_for_job(job)


def _normalize_idempotency_key(value: Any) -> str | None:
    key = str(value or "").strip()
    return key or None


def _load_idempotency_store() -> dict[str, Any]:
    path = _idempotency_path()
    if not path.exists():
        return {}
    with contextlib.suppress(Exception):
        payload = _read_json(path)
        if isinstance(payload, dict):
            return {str(key): value for key, value in payload.items() if isinstance(value, dict)}
    return {}


def _save_idempotency_store(payload: dict[str, Any]) -> None:
    _ensure_dirs()
    _write_json(_idempotency_path(), payload)


def _record_idempotency(key: str, job_id: str, status: str) -> None:
    store = _load_idempotency_store()
    store[key] = {"jobId": job_id, "status": status}
    _save_idempotency_store(store)


def _update_idempotency_status_for_job(job: dict[str, Any]) -> None:
    job_id = str(job.get("jobId") or "")
    if not job_id:
        return
    store = _load_idempotency_store()
    changed = False
    for entry in store.values():
        if isinstance(entry, dict) and entry.get("jobId") == job_id:
            entry["status"] = str(job.get("status") or entry.get("status") or "unknown")
            changed = True
    if changed:
        _save_idempotency_store(store)


def _idempotent_job_for_key(key: str) -> dict[str, Any] | None:
    store = _load_idempotency_store()
    entry = store.get(key)
    if not isinstance(entry, dict):
        return None
    job_id = str(entry.get("jobId") or "").strip()
    if not job_id:
        return None
    try:
        job = _load_job(job_id)
    except ValueError:
        store.pop(key, None)
        _save_idempotency_store(store)
        return None
    status = str(job.get("status") or entry.get("status") or "unknown")
    if entry.get("status") != status:
        entry["status"] = status
        _save_idempotency_store(store)
    return job


def _recent_jobs(limit: int = 10) -> list[dict[str, Any]]:
    _ensure_dirs()
    jobs = []
    for path in (_JOBS_DIR / "jobs").glob("*.json"):
        with contextlib.suppress(Exception):
            jobs.append(_read_json(path))
    jobs.sort(key=lambda item: str(item.get("createdAt", "")), reverse=True)
    return jobs[: max(1, min(limit, 100))]


def _read_lock(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    with contextlib.suppress(Exception):
        lock = _read_json(path)
        lock["_path"] = str(path)
        return lock
    return {"workspace": _WORKSPACE_NAME, "invalid": True, "_path": str(path)}


def _active_lock_for_path(path: Path) -> dict[str, Any] | None:
    lock = _read_lock(path)
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
        path.unlink()
    return None


def _active_locks() -> list[dict[str, Any]]:
    _ensure_dirs()
    locks: list[dict[str, Any]] = []
    workspace_lock_dir = _LOCKS_DIR / re.sub(r"[^A-Za-z0-9_.-]+", "_", _WORKSPACE_NAME)
    for path in sorted(workspace_lock_dir.glob("*.lock")) if workspace_lock_dir.exists() else []:
        lock = _active_lock_for_path(path)
        if lock:
            locks.append(lock)
    legacy_lock = _LOCKS_DIR / f"{_WORKSPACE_NAME}.lock"
    if legacy_lock.exists():
        lock = _active_lock_for_path(legacy_lock)
        if lock:
            locks.append(lock)
    return locks


def _active_lock() -> dict[str, Any] | None:
    locks = _active_locks()
    return locks[0] if locks else None


def _conflicting_lock(scopes: list[str]) -> dict[str, Any] | None:
    requested = set(scopes)
    requested_workspace = "workspace-write" in requested
    # "read" is a shared scope: read-only jobs (e.g. ingest --plan-only, which
    # only reads sources and writes a uniquely-named plan file) may run
    # concurrently. Only exclusive scopes (workspace-write, deliverable:*,
    # template:*) conflict. Without this, per-file ingest_plan tasks serialize
    # and every parallel one fails with target_busy.
    requested_exclusive = requested - {"read"}
    for lock in _active_locks():
        active_scopes = set(lock.get("scopes") or [lock.get("scope") or "workspace-write"])
        if requested_workspace or "workspace-write" in active_scopes:
            return lock
        active_exclusive = active_scopes - {"read"}
        if requested_exclusive & active_exclusive:
            return lock
    return None


def _create_locks(job_id: str, scopes: list[str]) -> None:
    _ensure_dirs()
    created: list[Path] = []
    payload = {"workspace": _WORKSPACE_NAME, "jobId": job_id, "scopes": scopes, "createdAt": _now()}
    try:
        for scope in scopes:
            path = _lock_path(scope, job_id)
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("x", encoding="utf-8") as handle:
                handle.write(json.dumps({**payload, "scope": scope}, ensure_ascii=False, indent=2) + "\n")
            created.append(path)
    except FileExistsError:
        for path in created:
            with contextlib.suppress(FileNotFoundError):
                path.unlink()
        raise


def _clear_locks(job_id: str) -> None:
    workspace_lock_dir = _LOCKS_DIR / re.sub(r"[^A-Za-z0-9_.-]+", "_", _WORKSPACE_NAME)
    for path in sorted(workspace_lock_dir.glob("*.lock")) if workspace_lock_dir.exists() else []:
        lock = _read_lock(path)
        if lock and str(lock.get("jobId")) == job_id:
            with contextlib.suppress(FileNotFoundError):
                path.unlink()
    legacy_lock = _LOCKS_DIR / f"{_WORKSPACE_NAME}.lock"
    lock = _read_lock(legacy_lock)
    if lock and str(lock.get("jobId")) == job_id:
        with contextlib.suppress(FileNotFoundError):
            legacy_lock.unlink()


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
        <dt>Version</dt><dd><code>{_escape_html(_AGENT_VERSION)}</code></dd>
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
        <li><code>agent_describe</code><span>Return the orchestration contract.</span></li>
        <li><code>agent_plan</code><span>Plan orchestrated task fragments.</span></li>
        <li><code>agent_execute</code><span>Execute one planned production operation.</span></li>
        <li><code>agent_status</code><span>Read one orchestrated task status.</span></li>
        <li><code>agent_cancel</code><span>Cancel one orchestrated task.</span></li>
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
            name="agent_describe",
            description=(
                "Return this agent's generic multi-agent orchestration contract: its declared capabilities "
                "(knowledge.update, document.build, document.publish, knowledge.pipeline) and their input schemas. "
                "It tells an orchestrator what this agent can do and how to plan it. A caller that just wants to run a "
                "job directly does not need this — use the production_* tools (e.g. production_start_job) instead."
            ),
            inputSchema={"type": "object", "properties": {}, "additionalProperties": False},
        ),
        Tool(
            name="agent_plan",
            description=(
                "Orchestration-contract planning. Given a capability and objective, return an executable task-graph "
                "fragment (tasks, dependencies, recommended concurrency) WITHOUT executing anything. Intended for an "
                "orchestrator that composes work across agents; a direct caller should use production_start_job instead."
            ),
            inputSchema=_agent_plan_input_schema(),
        ),
        Tool(
            name="agent_execute",
            description=(
                "Orchestration-contract execution. Run ONE planned task (typically produced by agent_plan) from its "
                "operation and concrete arguments. Returns a jobId immediately; poll agent_status. Direct callers who "
                "are not orchestrating a task graph should prefer production_start_job."
            ),
            inputSchema=_agent_execute_input_schema(),
        ),
        Tool(
            name="agent_status",
            description=(
                "Orchestration-contract status. Read one task by jobId; or, given a capability and operation with no "
                "jobId, discover the inputs that operation would use (e.g. the pending source files) without starting work."
            ),
            inputSchema=_agent_status_input_schema(),
        ),
        Tool(
            name="agent_cancel",
            description="Orchestration-contract cancel. Cancel one planned/running production task by its jobId.",
            inputSchema=_agent_job_id_input_schema(),
        ),
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
                "For type=\"ingest\" or type=\"ingest_plan\", this reads Markdown files already present in the workspace, including files exported by agent-cme; it does not fetch from Confluence directly. "
                "Use type=\"ingest_apply\" only to apply ingest plan files produced by type=\"ingest_plan\". "
                "type=\"export\" (and type=\"polish\") means publishing an existing llm-wiki DELIVERABLE from deliverables/ — it is NOT a source/Confluence export. To export Confluence or other external source content into Markdown, use the source-export agent's export tool (e.g. agent-cme's cme_export_run), not this job. "
                "Bearer authentication and the step allowlist are the primary controls. "
                "If PRODUCTION_REQUIRE_CONFIRMATION=true, mutating jobs also require confirm=true."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": ["doctor", "copy", "ingest", "ingest_plan", "ingest_apply", "taxonomy", "build", "export", "polish", "restore", "pipeline"],
                    },
                    "steps": {
                        "type": "array",
                        "items": {"type": "string", "enum": ["doctor", "copy", "ingest", "ingest_plan", "ingest_apply", "taxonomy", "build", "export", "polish", "restore"]},
                        "description": "Required for type=pipeline. Ordered steps to run.",
                    },
                    "templates": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional template files for build steps, relative to the workspace root or templates/.",
                    },
                    "inputs": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional source files for ingest/ingest_plan steps, or ingest plan files for ingest_apply, relative to the workspace root.",
                    },
                    "deliverables": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Required for export or polish steps. Paths are relative to the workspace root or deliverables/.",
                    },
                    "stabilize": {
                        "type": "boolean",
                        "description": (
                            "Apply section-by-section stabilization on build steps. "
                            "Unchanged sections are preserved verbatim; only changed sections are rewritten by LLM. "
                            "Ignored for first builds and has no effect on ingest, export, or polish steps."
                        ),
                    },
                    "configPath": {
                        "type": "string",
                        "description": "Optional .wikirc file path relative to the workspace root, for example .wikirc.yaml.openai.",
                    },
                    "callerLabel": {
                        "type": "string",
                        "description": "Optional identifier of the caller (e.g. 'my-project/wiki-manager'). Logged in the job for traceability.",
                    },
                    "restoreFile": {"type": "string", "description": "Workspace-relative file to restore."},
                    "restoreRevision": {"type": "string", "description": "Git revision used for file restore."},
                    "restoreRun": {"type": "string", "description": "Git run commit to revert to its parent."},
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
    print(f"{_LOG_PREFIX} tools/call {name}")
    try:
        denied = _require_tool_scope(name)
        if denied is not None:
            return denied
        match name:
            case "agent_describe":
                result = _tool_agent_describe()
            case "agent_plan":
                result = _tool_agent_plan(arguments)
            case "agent_execute":
                result = await _tool_agent_execute(arguments)
            case "agent_status":
                result = _tool_agent_status(arguments)
            case "agent_cancel":
                result = await _tool_agent_cancel(arguments)
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
        print(f"{_LOG_PREFIX} tools/result {name} ok {int((time.time() - start) * 1000)}ms")
        return result
    except Exception as exc:
        print(f"{_LOG_PREFIX} tools/result {name} error {int((time.time() - start) * 1000)}ms {_mask_secret_text(exc)}")
        return _json_text({"ok": False, "error": str(exc)})


def _tool_agent_describe() -> list[TextContent]:
    return _json_text(_agent_description())


def _tool_agent_plan(args: dict[str, Any]) -> list[TextContent]:
    return _json_text(_agent_plan(args))


async def _tool_agent_execute(args: dict[str, Any]) -> list[TextContent]:
    workspace = _agent_request_workspace(args)
    idempotency_key = _normalize_idempotency_key(args.get("idempotencyKey"))
    if idempotency_key:
        existing = _idempotent_job_for_key(idempotency_key)
        if existing:
            status_payload = _agent_task_status(existing, _job_progress(existing, _read_log_tail(str(existing["jobId"]), 500)))
            response = {
                "accepted": True,
                "idempotent": True,
                "agentInstanceId": _AGENT_INSTANCE_ID,
                "taskId": args.get("taskId"),
                "jobId": existing["jobId"],
                "status": existing.get("status", "unknown"),
            }
            if _terminal_status(existing.get("status")):
                response["terminal"] = True
                response["result"] = status_payload.get("result")
            elif _active_status(existing.get("status")):
                response["terminal"] = False
            return _json_text(response)

    start_args = _agent_start_job_args(args, workspace)
    result = _json_payload(await _tool_start_job(start_args, workspace_name_override=workspace["name"]))
    if not result.get("ok"):
        return _json_text(
            {
                "accepted": False,
                "agentInstanceId": _AGENT_INSTANCE_ID,
                "taskId": args.get("taskId"),
                "error": result.get("error") or "execution_rejected",
                **({"activeJobId": result.get("activeJobId")} if result.get("activeJobId") else {}),
            }
        )
    if idempotency_key:
        _record_idempotency(idempotency_key, result["jobId"], result.get("status", "queued"))
    return _json_text(
        {
            "accepted": True,
            **({"idempotent": False} if idempotency_key else {}),
            "agentInstanceId": _AGENT_INSTANCE_ID,
            "taskId": args.get("taskId"),
            "jobId": result["jobId"],
            "status": result.get("status", "queued"),
        }
    )


def _tool_agent_status(args: dict[str, Any]) -> list[TextContent]:
    job_id = str(args.get("jobId", "")).strip()
    if not job_id:
        return _json_text(_agent_capability_status(args))
    job = _load_job(job_id)
    progress = _job_progress(job, _read_log_tail(job_id, 500))
    return _json_text(_agent_task_status(job, progress))


def _agent_capability_status(args: dict[str, Any]) -> dict[str, Any]:
    capability = str(args.get("capability", "")).strip()
    operation = str(args.get("operation", "")).strip()
    if capability != "knowledge.update" or operation != "ingest":
        raise ValueError(f"Input discovery is not supported for {capability or 'missing capability'}/{operation or 'missing operation'}.")
    files = _resolve_plan_files(None, "raw/untracked", default_scan=True)
    return {
        "contractVersion": "1",
        "agentInstanceId": _AGENT_INSTANCE_ID,
        "capability": capability,
        "operation": operation,
        "available": _WORKSPACE_PATH.exists(),
        "pendingInputs": [
            {
                "type": "file",
                "ref": file_ref,
                "label": Path(file_ref).name,
                "mediaType": "text/markdown",
            }
            for file_ref in files
        ],
    }


async def _tool_agent_cancel(args: dict[str, Any]) -> list[TextContent]:
    job_id = str(args.get("jobId", "")).strip()
    result = _json_payload(await _tool_cancel_job({"jobId": job_id}))
    if not result.get("ok"):
        payload = {
            "ok": False,
            "jobId": job_id,
            "error": result.get("error") or "cancel_failed",
        }
        job = result.get("job")
        if isinstance(job, dict):
            payload["status"] = _agent_task_status(job, _job_progress(job, _read_log_tail(job_id, 500)))
        return _json_text(payload)
    job = result.get("job") if isinstance(result.get("job"), dict) else _load_job(job_id)
    return _json_text(_agent_task_status(job, _job_progress(job, _read_log_tail(job_id, 500))))


def _tool_status() -> list[TextContent]:
    return _json_text(
        {
            "ok": True,
            "service": "agent-wiki-production",
            "version": _AGENT_VERSION,
            "workspace": {"name": _WORKSPACE_NAME, "path": str(_WORKSPACE_PATH), "exists": _WORKSPACE_PATH.exists()},
            "allowedSteps": sorted(_ALLOWED_STEPS),
            "requireConfirmation": _REQUIRE_CONFIRMATION,
            "importsConfigured": len(_parse_imports()),
            "activeLock": _active_lock(),
            "activeLocks": _active_locks(),
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
            if any(part.startswith(".tmp.") or part.startswith(".changes.") for part in path.parts):
                continue
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


async def _tool_start_job(args: dict[str, Any], workspace_name_override: str | None = None) -> list[TextContent]:
    job_type = str(args.get("type", "")).strip()
    preview_dry_run = bool(args.get("dryRun", False))
    execute_dry_run = bool(args.get("executeDryRun", False))
    confirm = bool(args.get("confirm", False))
    steps = _resolve_steps(job_type, args.get("steps"))
    inputs = _expand_input_globs(_resolve_string_list(args.get("inputs"), "inputs"))
    templates = _resolve_string_list(args.get("templates"), "templates")
    deliverables = _resolve_string_list(args.get("deliverables"), "deliverables")
    stabilize = bool(args.get("stabilize", False))
    restore_file = str(args.get("restoreFile") or "").strip() or None
    restore_revision = str(args.get("restoreRevision") or "").strip() or None
    restore_run = str(args.get("restoreRun") or "").strip() or None
    config_path = _resolve_config_path(args.get("configPath"))
    caller_label = str(args["callerLabel"]).strip()[:120] if args.get("callerLabel") else None
    idempotency_key = _normalize_idempotency_key(args.get("idempotencyKey"))
    capability = str(args.get("capability") or "").strip() or None
    run_id = str(args.get("runId") or "").strip() or None
    workspace_name = str(workspace_name_override or _WORKSPACE_NAME).strip() or _WORKSPACE_NAME

    if job_type not in _ALLOWED_STEPS:
        raise ValueError(f"Job type is not allowed: {job_type}")
    for step in steps:
        if step not in _ALLOWED_STEPS:
            raise ValueError(f"Step is not allowed: {step}")
    if not _WORKSPACE_PATH.exists():
        raise ValueError(f"Workspace path does not exist: {_WORKSPACE_PATH}")
    if any(step in {"export", "polish"} for step in steps) and not deliverables:
        raise ValueError("export and polish jobs require at least one deliverable.")
    if "restore" in steps and not ((restore_file and restore_revision) or restore_run):
        raise ValueError("restore jobs require restoreFile+restoreRevision or restoreRun.")
    if inputs and not any(step in {"ingest", "ingest_plan", "ingest_apply"} for step in steps):
        raise ValueError("inputs can only be used with ingest, ingest_plan, ingest_apply, or pipeline jobs.")
    lock_scopes = _job_lock_scopes(steps, inputs, templates, deliverables)

    plan = {
        "type": job_type,
        "steps": steps,
        "workspace": workspace_name,
        "inputs": inputs,
        "templates": templates,
        "deliverables": deliverables,
        "stabilize": stabilize,
        "configPath": config_path,
        **({"restoreFile": restore_file, "restoreRevision": restore_revision, "restoreRun": restore_run, "dryRun": execute_dry_run} if "restore" in steps else {}),
        "lockScopes": lock_scopes,
    }
    if preview_dry_run:
        return _json_text({
            "ok": True,
            "dryRun": True,
            "plan": plan,
            "commands": _command_labels(
                steps,
                inputs,
                templates,
                deliverables,
                stabilize,
                restore_file=restore_file,
                restore_revision=restore_revision,
                restore_run=restore_run,
                dry_run=True,
            ),
        })
    if _REQUIRE_CONFIRMATION and not confirm and any(step in _MUTATING_STEPS for step in steps):
        raise ValueError("Production jobs require confirm=true after explicit user approval.")

    active = _conflicting_lock(lock_scopes)
    if active:
        return _json_text({"ok": False, "error": "target_busy", "activeJobId": active.get("jobId"), "lock": active, "requestedLockScopes": lock_scopes})

    _ensure_dirs()
    job_id = f"prod_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    job = {
        "jobId": job_id,
        "workspace": workspace_name,
        "type": job_type,
        "inputs": inputs,
        "templates": templates,
        "deliverables": deliverables,
        "lockScopes": lock_scopes,
        "stabilize": stabilize,
        "configPath": config_path,
        **({"restoreFile": restore_file, "restoreRevision": restore_revision, "restoreRun": restore_run, "dryRun": execute_dry_run} if "restore" in steps else {}),
        **({"callerLabel": caller_label} if caller_label else {}),
        **({"idempotencyKey": idempotency_key} if idempotency_key else {}),
        **({"capability": capability} if capability else {}),
        **({"runId": run_id} if run_id else {}),
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
    try:
        _create_locks(job_id, lock_scopes)
    except FileExistsError:
        active = _conflicting_lock(lock_scopes)
        if active:
            return _json_text({"ok": False, "error": "target_busy", "activeJobId": active.get("jobId"), "lock": active, "requestedLockScopes": lock_scopes})
        try:
            _create_locks(job_id, lock_scopes)
        except FileExistsError:
            active = _conflicting_lock(lock_scopes)
            return _json_text({"ok": False, "error": "target_busy", "activeJobId": active.get("jobId") if active else None, "lock": active, "requestedLockScopes": lock_scopes})
    _save_job(job)
    task = asyncio.create_task(_run_job(job_id))
    _ACTIVE_TASKS[job_id] = task
    return _json_text({"ok": True, "jobId": job_id, "status": "queued", "plan": plan, "_activity": _activity_for_job(job)})


def _tool_job_status(args: dict[str, Any]) -> list[TextContent]:
    job_id = str(args.get("jobId", "")).strip()
    job = _load_job(job_id)
    log_tail = _read_log_tail(job_id, 30)
    progress_log_tail = _read_log_tail(job_id, 500)
    progress = _job_progress(job, progress_log_tail)
    return _json_text(
        {
            "ok": True,
            "job": _with_duration(job),
            "logTail": log_tail,
            "progress": progress,
            "producedFiles": job.get("producedFiles") or _produced_files_from_job(job),
            "_activity": _activity_for_job(job, progress),
        }
    )


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
    for step in job.get("steps", []):
        if isinstance(step, dict) and step.get("status") in {"pending", "queued", "running"}:
            step["status"] = "cancelled"
            step["finishedAt"] = job["finishedAt"]
            step["exitCode"] = -15
    _append_log(job_id, "[cancelled] Job cancelled by request.")
    _save_job(job)
    _clear_locks(job_id)
    return _json_text({"ok": True, "job": _with_duration(job)})


def _tool_list_jobs(args: dict[str, Any]) -> list[TextContent]:
    limit = int(args.get("limit", 20))
    jobs = [_job_summary(_with_duration(job)) for job in _recent_jobs(limit)]
    return _json_text({"ok": True, "workspace": _WORKSPACE_NAME, "jobs": jobs})


def _agent_plan(args: dict[str, Any]) -> dict[str, Any]:
    request_args = args.get("arguments") if isinstance(args.get("arguments"), dict) else {}
    constraints = _planning_constraints(args.get("constraints"))
    capability, operation = _plan_capability_operation(args)
    workspace_revision = _workspace_revision_from_request(args)

    if capability == "knowledge.update":
        return _plan_knowledge_update(operation, request_args, constraints, workspace_revision)
    if capability == "document.build":
        return _plan_document_build(request_args, constraints, workspace_revision, depends_on=[])
    if capability == "document.publish":
        return _plan_document_publish(operation, request_args, constraints, workspace_revision, depends_on=[])
    if capability == "knowledge.pipeline":
        return _plan_pipeline(request_args, constraints, workspace_revision)
    if capability == "workspace.diagnose":
        return _empty_plan("workspace.diagnose", "Diagnose workspace", ["Production planning does not create diagnose tasks."])
    if capability == "workspace.restore":
        restore_file = str(request_args.get("file") or "").strip()
        restore_revision = str(request_args.get("to") or request_args.get("revision") or "").strip()
        restore_run = str(request_args.get("run") or "").strip()
        if not restore_run and (not restore_file or not restore_revision):
            raise ValueError("workspace.restore requires arguments.run, or arguments.file and arguments.to.")
        label = f"Restore run {restore_run}" if restore_run else f"Restore {restore_file}"
        arguments = ({"run": restore_run} if restore_run else {"file": restore_file, "to": restore_revision})
        if request_args.get("dryRun") is True:
            arguments["dryRun"] = True
        input_refs = [] if restore_run else [{"type": "file", "ref": restore_file}]
        output_refs = [] if restore_run else [{"type": "file", "ref": restore_file}]
        task = _planned_task(
            "restore-run" if restore_run else "restore-file",
            label,
            "workspace.restore",
            "restore",
            arguments,
            [],
            False,
            input_refs,
            output_refs,
            ["workspace-write"],
            constraints,
            workspace_revision,
        )
        return _fragment(
            "workspace.restore",
            "Restore workspace run" if restore_run else "Restore workspace file",
            [f"Restore run {restore_run}." if restore_run else f"Restore {restore_file} from revision {restore_revision}."],
            [],
            [task],
            task["expectedOutputRefs"],
        )
    raise ValueError(f"Unsupported planning capability: {capability}")


def _planning_constraints(raw_constraints: Any) -> dict[str, Any]:
    constraints = raw_constraints if isinstance(raw_constraints, dict) else {}
    max_tasks = _positive_int(constraints.get("maxTasks"))
    max_concurrency = _positive_int(constraints.get("maxConcurrency")) or _RECOMMENDED_CONCURRENCY or 1
    if _MAX_CONCURRENCY:
        max_concurrency = min(max_concurrency, _MAX_CONCURRENCY)
    max_depth = _positive_int(constraints.get("maxDepth"))
    return {
        "maxTasks": max_tasks,
        "maxConcurrency": max(1, max_concurrency),
        "maxDepth": max_depth,
        "requireApprovalForMutations": bool(constraints.get("requireApprovalForMutations", False)),
    }


def _positive_int(value: Any) -> int | None:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _plan_capability_operation(args: dict[str, Any]) -> tuple[str, str]:
    capability = str(args.get("capability") or "").strip()
    operation = str(args.get("operation") or "").strip()
    objective = str(args.get("objective") or "").lower()
    request_args = args.get("arguments") if isinstance(args.get("arguments"), dict) else {}
    legacy_type = str(request_args.get("type") or args.get("type") or "").strip()
    if not operation and legacy_type:
        operation = legacy_type

    if not capability:
        if operation in {"ingest", "ingest_plan", "ingest_apply", "taxonomy"} or "ingest" in objective or "taxonomy" in objective:
            capability = "knowledge.update"
        elif operation == "build" or "build" in objective or "document" in objective:
            capability = "document.build"
        elif operation in {"export", "polish"} or "publish" in objective or "publication" in objective:
            capability = "document.publish"
        elif operation == "pipeline" or "pipeline" in objective:
            capability = "knowledge.pipeline"
        # Deliberately no objective-keyword inference here, unlike the
        # capabilities above: workspace.restore overwrites files from a Git
        # revision, and "restore"/"rollback" appear in plenty of objectives
        # that mean nothing of the sort ("restore the ingest pipeline").
        # A destructive capability is only ever selected from an explicit
        # operation or an explicit capability.
        elif operation == "restore":
            capability = "workspace.restore"

    if not operation:
        defaults = {
            "knowledge.update": "ingest",
            "document.build": "build",
            "document.publish": "export",
            "knowledge.pipeline": "pipeline",
            "workspace.restore": "restore",
        }
        operation = defaults.get(capability, "")

    if capability not in _CAPABILITY_STEP_MAP:
        raise ValueError("agent_plan requires a supported capability.")
    if operation and operation not in {"ingest", "ingest_plan", "ingest_apply", "taxonomy", "build", "export", "polish", "restore", "pipeline"}:
        raise ValueError(f"Unsupported planning operation: {operation}")
    if operation == "ingest_plan":
        operation = "ingest"
    return capability, operation


def _workspace_revision_from_request(args: dict[str, Any]) -> str:
    workspace = args.get("workspace") if isinstance(args.get("workspace"), dict) else {}
    revision = workspace.get("revision") or args.get("workspaceRevision")
    return str(revision).strip() if revision else "unknown"


def _empty_plan(capability: str, label: str, synthesis: list[str]) -> dict[str, Any]:
    return {
        "contractVersion": "1",
        "agentInstanceId": _AGENT_INSTANCE_ID,
        "capability": capability,
        "summary": {
            "label": label,
            "initialSynthesis": synthesis,
            "estimatedTasks": 0,
        },
        "groups": [],
        "tasks": [],
        "expectedOutputs": [],
    }


def _plan_knowledge_update(
    operation: str,
    request_args: dict[str, Any],
    constraints: dict[str, Any],
    workspace_revision: str,
) -> dict[str, Any]:
    if operation not in {"ingest", "ingest_apply", "taxonomy"}:
        raise ValueError(f"knowledge.update cannot plan operation: {operation}")
    if operation == "taxonomy":
        if "taxonomy" not in _ALLOWED_STEPS:
            raise ValueError("taxonomy is not allowed by PRODUCTION_ALLOWED_STEPS.")
        task = _planned_task(
            "taxonomy-synthesis",
            "Synthesize graph taxonomy",
            "knowledge.update",
            "taxonomy",
            {},
            [],
            False,
            [{"type": "directory", "ref": "wiki"}],
            [{"type": "directory", "ref": ".wiki/graph"}],
            _job_lock_scopes(["taxonomy"], [], [], []),
            constraints,
            workspace_revision,
            barrier=True,
            progress_weight=1,
        )
        return _fragment(
            "knowledge.update",
            "Synthesize graph taxonomy",
            ["The current wiki corpus will be synthesized into semantic domains."],
            [],
            [task],
            task["expectedOutputRefs"],
        )
    if "ingest" not in _ALLOWED_STEPS:
        raise ValueError("ingest is not allowed by PRODUCTION_ALLOWED_STEPS.")
    files = _resolve_plan_files(request_args.get("inputs"), "raw/untracked", default_scan=True)
    if not files:
        return _empty_plan(
            "knowledge.update",
            "Update knowledge",
            ["No Markdown files were found in raw/untracked; no ingest operation was planned."],
        )

    max_depth = constraints["maxDepth"]
    max_tasks = constraints["maxTasks"]
    # The parallel ingest graph uses the internal plan/apply operations. A
    # deployment may deliberately allow only the aggregate `ingest` step; in
    # that case advertise and execute a valid sequential plan instead of
    # returning tasks that the same agent has forbidden itself to execute.
    parallel_ingest_available = {"ingest_plan", "ingest_apply"}.issubset(_ALLOWED_STEPS)
    if not parallel_ingest_available or (max_depth is not None and max_depth < 2) or (max_tasks is not None and max_tasks < 2):
        task = _planned_task(
            "ingest-all",
            # Single aggregated task: this one really does analyze *and* write.
            f"Analyze and write {len(files)} source file(s)",
            "knowledge.update",
            "ingest",
            {"inputs": files},
            [],
            False,
            _file_refs(files),
            [{"type": "directory", "ref": "wiki"}],
            _job_lock_scopes(["ingest"], files, [], []),
            constraints,
            workspace_revision,
            progress_weight=len(files),
        )
        tasks = [task]
        expected = task["expectedOutputRefs"]
        synthesis = [f"{len(files)} source file(s) will be ingested as one aggregated task."]
        if "taxonomy" in _ALLOWED_STEPS and (max_depth is None or max_depth >= 2) and (max_tasks is None or max_tasks >= 2):
            taxonomy = _planned_task(
                "taxonomy-synthesis",
                "Synthesize graph taxonomy",
                "knowledge.update",
                "taxonomy",
                {},
                [task["id"]],
                False,
                [{"type": "directory", "ref": "wiki"}],
                [{"type": "directory", "ref": ".wiki/graph"}],
                _job_lock_scopes(["taxonomy"], [], [], []),
                constraints,
                workspace_revision,
                barrier=True,
                progress_weight=max(1, len(files) / 4),
            )
            tasks.append(taxonomy)
            expected = [{"type": "directory", "ref": "wiki"}, {"type": "directory", "ref": ".wiki/graph"}]
        elif "taxonomy" in _ALLOWED_STEPS:
            synthesis.append("Taxonomy synthesis was omitted because the requested plan depth or task limit cannot represent it safely.")
        return _fragment("knowledge.update", "Update knowledge", synthesis, [], tasks, expected)

    aggregate_ingest = max_tasks is not None and len(files) + 1 > max_tasks
    ingest_group = {
        "id": "ingest",
        # "Plan"/"Apply" described the CLI flags, not what the user sees
        # happening: two tasks per file with no hint that one reads and the
        # other writes. Analyze/Write names the phase by its effect, which is
        # also what makes the barrier and the serialized writes legible.
        "label": "Analyze sources",
        "recommendedConcurrency": constraints["maxConcurrency"],
        "progressWeight": len(files),
    }
    plan_refs: list[dict[str, Any]] = []
    tasks: list[dict[str, Any]] = []
    if aggregate_ingest:
        plan_ref = _ingest_plan_ref(files)
        plan_refs.append(plan_ref)
        tasks.append(
            _planned_task(
                "ingest-batch",
                f"Analyze {len(files)} source file(s)",
                "knowledge.update",
                "ingest_plan",
                {"inputs": files},
                [],
                True,
                _file_refs(files),
                [plan_ref],
                ["read"],
                constraints,
                workspace_revision,
                group_id="ingest",
                recommended_concurrency=constraints["maxConcurrency"],
                progress_weight=len(files),
            )
        )
    else:
        for index, file_ref in enumerate(files, 1):
            plan_ref = _ingest_plan_ref([file_ref])
            plan_refs.append(plan_ref)
            tasks.append(
                _planned_task(
                    _task_id("ingest", file_ref),
                    f"Analyze {Path(file_ref).name}",
                    "knowledge.update",
                    "ingest_plan",
                    {"inputs": [file_ref]},
                    [],
                    True,
                    _file_refs([file_ref]),
                    [plan_ref],
                    [f"ingest-plan:{file_ref}"],
                    constraints,
                    workspace_revision,
                    group_id="ingest",
                    recommended_concurrency=constraints["maxConcurrency"],
                    progress_weight=1,
                    priority=index,
                )
            )

    apply_allowed = "ingest_apply" in _ALLOWED_STEPS
    # Declared before the branch: the expected-output list below reads it even
    # when the apply phase is not allowlisted at all.
    taxonomy_planned = False
    if apply_allowed:
        plan_tasks = list(tasks)
        if aggregate_ingest:
            apply_specs = [("ingest-apply", plan_tasks[0], plan_refs[0], f"{len(files)} source files", len(files), None)]
        else:
            apply_specs = [
                (
                    "ingest-apply" if len(files) == 1 else _task_id("ingest-apply", file_ref),
                    plan_task,
                    plan_ref,
                    Path(file_ref).name,
                    1,
                    plan_task.get("priority"),
                )
                for file_ref, plan_task, plan_ref in zip(files, plan_tasks, plan_refs, strict=True)
            ]
        # ingest_apply takes the global workspace-write lock, which conflicts
        # with BOTH another apply and every still-running ingest_plan read lock.
        # The group barrier waits for the complete parallel planning group, and
        # the lock itself serializes the writes.
        #
        # The applies used to be chained to each other on top of that. It was
        # belt AND braces, and the belt cost a run: an apply whose plan failed
        # is skipped, and every apply behind it in the chain inherited a
        # dependency that would never succeed. One unreadable source file left
        # nine perfectly good ones unwritten. Nothing was gained — two applies
        # already cannot start together, since the second cannot acquire
        # workspace-write while the first holds it.
        for apply_id, plan_task, plan_ref, source_label, progress_weight, priority in apply_specs:
            depends_on = [plan_task["id"]]
            tasks.append(
                _planned_task(
                    apply_id,
                    f"Write {source_label} to the wiki",
                    "knowledge.update",
                    "ingest_apply",
                    {"inputs": [plan_ref["ref"]]},
                    depends_on,
                    False,
                    [plan_ref],
                    [{"type": "directory", "ref": "wiki"}],
                    _job_lock_scopes(["ingest_apply"], [plan_ref["ref"]], [], []),
                    constraints,
                    workspace_revision,
                    depends_on_group=ingest_group["id"],
                    barrier=True,
                    progress_weight=progress_weight,
                    priority=priority,
                )
            )

        # Une seule synthèse globale, après TOUTES les écritures. Elle classe
        # des familles condensées ; elle ne déclenche jamais un appel par page.
        # Dépendre de chaque apply garantit qu'un corpus partiel n'est pas
        # présenté comme la taxonomie finale et qu'un apply en échec bloque la
        # synthèse au lieu de publier par-dessus un wiki incomplet.
        # The budget must be honoured on THIS path too.
        #
        # The sequential branch above refuses the taxonomy task when the caller's
        # maxTasks/maxDepth cannot represent it, and says so. This branch simply
        # appended it, and — unlike the pipeline path — never passes through
        # `_aggregate_if_needed`, so nothing downstream caught the overflow. A
        # caller asking for a two-level plan received a three-level one:
        # ingest_plan -> ingest_apply -> taxonomy.
        taxonomy_fits = (
            (max_depth is None or max_depth >= 3)
            and (max_tasks is None or len(tasks) + 1 <= max_tasks)
        )
        taxonomy_planned = "taxonomy" in _ALLOWED_STEPS and taxonomy_fits
        if taxonomy_planned:
            apply_ids = [spec[0] for spec in apply_specs]
            tasks.append(
                _planned_task(
                    "taxonomy-synthesis",
                    "Synthesize graph taxonomy",
                    "knowledge.update",
                    "taxonomy",
                    {},
                    apply_ids,
                    False,
                    [{"type": "directory", "ref": "wiki"}],
                    [{"type": "directory", "ref": ".wiki/graph"}],
                    _job_lock_scopes(["taxonomy"], [], [], []),
                    constraints,
                    workspace_revision,
                    barrier=True,
                    progress_weight=max(1, len(files) / 4),
                )
            )

    synthesis = [f"{len(files)} source file(s) resolved from raw/untracked."]
    if aggregate_ingest:
        synthesis.append("Source analysis was aggregated into one task to satisfy maxTasks.")
    if not apply_allowed:
        synthesis.append("ingest_apply is not allowlisted; the apply barrier was omitted.")
    if apply_allowed and "taxonomy" in _ALLOWED_STEPS and not taxonomy_planned:
        synthesis.append(
            "Taxonomy synthesis was omitted because the requested plan depth or task limit cannot represent it safely."
        )
    # The promise must match the plan: announcing `.wiki/graph` while the
    # taxonomy task was dropped would have the orchestrator wait on an output
    # nothing produces.
    expected = ([{"type": "directory", "ref": "wiki"}, {"type": "directory", "ref": ".wiki/graph"}]
                if apply_allowed and taxonomy_planned
                else [{"type": "directory", "ref": "wiki"}] if apply_allowed else plan_refs)
    return _fragment("knowledge.update", "Update knowledge", synthesis, [ingest_group], tasks, expected)


def _plan_document_build(
    request_args: dict[str, Any],
    constraints: dict[str, Any],
    workspace_revision: str,
    depends_on: list[str],
) -> dict[str, Any]:
    if "build" not in _ALLOWED_STEPS:
        raise ValueError("build is not allowed by PRODUCTION_ALLOWED_STEPS.")
    templates = _resolve_plan_files(request_args.get("templates"), "templates", default_scan=True)
    if not templates:
        return _empty_plan(
            "document.build",
            "Build documents",
            ["No Markdown templates were found in templates; no build operation was planned."],
        )
    tasks, expected = _build_tasks(templates, request_args, constraints, workspace_revision, depends_on)
    return _fragment(
        "document.build",
        "Build documents",
        [f"{len(templates)} template file(s) resolved for document generation."],
        _groups_for_tasks(tasks, "build", "Build deliverables", constraints["maxConcurrency"], len(templates)),
        tasks,
        expected,
    )


def _plan_document_publish(
    operation: str,
    request_args: dict[str, Any],
    constraints: dict[str, Any],
    workspace_revision: str,
    depends_on: list[str],
) -> dict[str, Any]:
    if operation not in {"export", "polish"}:
        raise ValueError(f"document.publish cannot plan operation: {operation}")
    if operation not in _ALLOWED_STEPS:
        raise ValueError(f"{operation} is not allowed by PRODUCTION_ALLOWED_STEPS.")
    deliverables = _resolve_plan_files(request_args.get("deliverables"), "deliverables", default_scan=True)
    if not deliverables:
        return _empty_plan(
            "document.publish",
            "Publish documents",
            ["No Markdown deliverables were found in deliverables; no publication operation was planned."],
        )
    tasks = _publish_tasks(operation, deliverables, request_args, constraints, workspace_revision, depends_on)
    return _fragment(
        "document.publish",
        "Publish documents",
        [f"{len(deliverables)} deliverable file(s) resolved for {operation}."],
        _groups_for_tasks(tasks, operation, f"{operation.title()} deliverables", constraints["maxConcurrency"], len(deliverables)),
        tasks,
        _file_refs(deliverables),
    )


def _plan_pipeline(request_args: dict[str, Any], constraints: dict[str, Any], workspace_revision: str) -> dict[str, Any]:
    if "pipeline" not in _ALLOWED_STEPS:
        raise ValueError("pipeline is not allowed by PRODUCTION_ALLOWED_STEPS.")
    requested_steps = _pipeline_requested_steps(request_args)
    max_depth = constraints["maxDepth"]
    if max_depth is not None and max_depth < 3:
        task = _planned_task(
            "pipeline",
            "Run full production pipeline",
            "knowledge.pipeline",
            "pipeline",
            _pipeline_arguments(request_args),
            [],
            False,
            [],
            [{"type": "directory", "ref": "deliverables"}],
            ["workspace-write"],
            constraints,
            workspace_revision,
            progress_weight=1,
        )
        return _fragment(
            "knowledge.pipeline",
            "Run production pipeline",
            ["Pipeline planning was aggregated to satisfy maxDepth."],
            [],
            [task],
            task["expectedOutputRefs"],
        )

    tasks: list[dict[str, Any]] = []
    groups: list[dict[str, Any]] = []
    expected_outputs: list[dict[str, Any]] = []

    input_files = _resolve_plan_files(request_args.get("inputs"), "raw/untracked", default_scan=True)
    if "ingest" in requested_steps and input_files and "ingest" in _ALLOWED_STEPS:
        ingest_fragment = _plan_knowledge_update("ingest", request_args, constraints, workspace_revision)
        tasks.extend(ingest_fragment["tasks"])
        groups.extend(ingest_fragment["groups"])
        expected_outputs.extend(ingest_fragment.get("expectedOutputs") or [])
    previous_ids = [task["id"] for task in tasks if task.get("barrier")] or [task["id"] for task in tasks]

    templates = _resolve_plan_files(request_args.get("templates"), "templates", default_scan=True)
    if "build" in requested_steps and templates and "build" in _ALLOWED_STEPS:
        build_tasks, build_outputs = _build_tasks(templates, request_args, constraints, workspace_revision, previous_ids, capability="document.build")
        tasks.extend(build_tasks)
        groups.extend(_groups_for_tasks(build_tasks, "build", "Build deliverables", constraints["maxConcurrency"], len(templates)))
        expected_outputs.extend(build_outputs)
        previous_ids = [task["id"] for task in build_tasks]

    deliverables = [ref["ref"] for ref in expected_outputs if isinstance(ref, dict) and str(ref.get("ref", "")).startswith("deliverables/")]
    if not deliverables:
        deliverables = _resolve_plan_files(request_args.get("deliverables"), "deliverables", default_scan=True)
    publish_operation = "export" if "export" in requested_steps and "export" in _ALLOWED_STEPS else ""
    if deliverables and publish_operation:
        publish_tasks = _publish_tasks(publish_operation, deliverables, request_args, constraints, workspace_revision, previous_ids, capability="document.publish")
        tasks.extend(publish_tasks)
        groups.extend(_groups_for_tasks(publish_tasks, publish_operation, f"{publish_operation.title()} deliverables", constraints["maxConcurrency"], len(deliverables)))
        expected_outputs.extend(_file_refs(deliverables))
        previous_ids = [task["id"] for task in publish_tasks]
    if deliverables and "polish" in requested_steps and "polish" in _ALLOWED_STEPS:
        polish_tasks = _publish_tasks("polish", deliverables, request_args, constraints, workspace_revision, previous_ids, capability="document.publish")
        tasks.extend(polish_tasks)
        groups.extend(_groups_for_tasks(polish_tasks, "polish", "Polish deliverables", constraints["maxConcurrency"], len(deliverables)))
        expected_outputs.extend(_file_refs(deliverables))

    if not tasks:
        return _empty_plan(
            "knowledge.pipeline",
            "Run production pipeline",
            ["No raw/untracked inputs, templates, or deliverables were found; no pipeline operation was planned."],
        )

    planned_count = len(tasks)
    tasks = _aggregate_if_needed(tasks, constraints, workspace_revision, "knowledge.pipeline")
    if len(tasks) != planned_count:
        groups = []
    return _fragment(
        "knowledge.pipeline",
        "Run production pipeline",
        ["Full production pipeline planned from concrete workspace files."],
        _unique_groups(groups),
        tasks,
        _unique_refs(expected_outputs),
    )


def _fragment(
    capability: str,
    label: str,
    synthesis: list[str],
    groups: list[dict[str, Any]],
    tasks: list[dict[str, Any]],
    expected_outputs: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "contractVersion": "1",
        "agentInstanceId": _AGENT_INSTANCE_ID,
        "capability": capability,
        "summary": {
            "label": label,
            "initialSynthesis": synthesis,
            "estimatedTasks": len(tasks),
        },
        "groups": groups,
        "tasks": tasks,
        "expectedOutputs": _unique_refs(expected_outputs),
    }


def _build_tasks(
    templates: list[str],
    request_args: dict[str, Any],
    constraints: dict[str, Any],
    workspace_revision: str,
    depends_on: list[str],
    capability: str = "document.build",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    stabilize = bool(request_args.get("stabilize", False))
    config_path = _safe_optional_config_path(request_args)
    if _must_aggregate(len(templates), constraints):
        deliverables = [_deliverable_for_template(template) for template in templates]
        task = _planned_task(
            "build-batch",
            f"Build {len(templates)} deliverable(s)",
            capability,
            "build",
            {"templates": templates, "stabilize": stabilize, **({"configPath": config_path} if config_path else {})},
            depends_on,
            True,
            _file_refs(templates),
            _file_refs(deliverables),
            _job_lock_scopes(["build"], [], templates, []),
            constraints,
            workspace_revision,
            group_id="build",
            recommended_concurrency=constraints["maxConcurrency"],
            progress_weight=len(templates),
        )
        return [task], task["expectedOutputRefs"]

    tasks: list[dict[str, Any]] = []
    outputs: list[dict[str, Any]] = []
    for index, template in enumerate(templates, 1):
        deliverable = _deliverable_for_template(template)
        outputs.append({"type": "file", "ref": deliverable})
        tasks.append(
            _planned_task(
                _task_id("build", template),
                f"Build {Path(deliverable).name}",
                capability,
                "build",
                {"templates": [template], "stabilize": stabilize, **({"configPath": config_path} if config_path else {})},
                depends_on,
                True,
                _file_refs([template]),
                [{"type": "file", "ref": deliverable}],
                _job_lock_scopes(["build"], [], [template], []),
                constraints,
                workspace_revision,
                group_id="build",
                recommended_concurrency=constraints["maxConcurrency"],
                progress_weight=1,
                priority=index,
            )
        )
    return tasks, outputs


def _publish_tasks(
    operation: str,
    deliverables: list[str],
    request_args: dict[str, Any],
    constraints: dict[str, Any],
    workspace_revision: str,
    depends_on: list[str],
    capability: str = "document.publish",
) -> list[dict[str, Any]]:
    config_path = _safe_optional_config_path(request_args)
    if _must_aggregate(len(deliverables), constraints):
        return [
            _planned_task(
                f"{operation}-batch",
                f"{operation.title()} {len(deliverables)} deliverable(s)",
                capability,
                operation,
                {"deliverables": deliverables, **({"configPath": config_path} if config_path else {})},
                depends_on,
                True,
                _file_refs(deliverables),
                _file_refs(deliverables),
                _job_lock_scopes([operation], [], [], deliverables),
                constraints,
                workspace_revision,
                group_id=operation,
                recommended_concurrency=constraints["maxConcurrency"],
                progress_weight=len(deliverables),
            )
        ]

    tasks: list[dict[str, Any]] = []
    for index, deliverable in enumerate(deliverables, 1):
        tasks.append(
            _planned_task(
                _task_id(operation, deliverable),
                f"{operation.title()} {Path(deliverable).name}",
                capability,
                operation,
                {"deliverables": [deliverable], **({"configPath": config_path} if config_path else {})},
                depends_on,
                True,
                _file_refs([deliverable]),
                _file_refs([deliverable]),
                _job_lock_scopes([operation], [], [], [deliverable]),
                constraints,
                workspace_revision,
                group_id=operation,
                recommended_concurrency=constraints["maxConcurrency"],
                progress_weight=1,
                priority=index,
            )
        )
    return tasks


def _planned_task(
    task_id: str,
    label: str,
    capability: str,
    operation: str,
    arguments: dict[str, Any],
    depends_on: list[str],
    parallelizable: bool,
    input_refs: list[dict[str, Any]],
    expected_output_refs: list[dict[str, Any]],
    locks: list[str],
    constraints: dict[str, Any],
    workspace_revision: str,
    group_id: str | None = None,
    depends_on_group: str | None = None,
    barrier: bool = False,
    recommended_concurrency: int | None = None,
    progress_weight: int | float = 1,
    priority: int | None = None,
) -> dict[str, Any]:
    requires_approval = bool(constraints["requireApprovalForMutations"] and operation in _MUTATING_STEPS)
    task = {
        "id": task_id,
        "label": label,
        "requiredCapability": capability,
        "operation": operation,
        "arguments": arguments,
        "dependsOn": depends_on,
        "parallelizable": parallelizable,
        "inputRefs": input_refs,
        "expectedOutputRefs": expected_output_refs,
        "locks": locks,
        "requiresApproval": requires_approval,
        "idempotencyKey": _idempotency_key(workspace_revision, capability, operation, arguments, input_refs) if operation in _MUTATING_STEPS else None,
        "progressWeight": progress_weight,
        # Retries belong to the specialized agent contract. The runtime keeps
        # the same logical task id and creates new technical attempts; Donna
        # must never cancel/restart production jobs herself.
        "retryPolicy": {
            "maxAttempts": 3,
            "retryableErrors": ["execution_failed", "workspace_busy", "dispatcher_error"],
            "allowAgentFallback": False,
        },
    }
    if group_id:
        task["groupId"] = group_id
    if depends_on_group:
        task["dependsOnGroup"] = depends_on_group
    if barrier:
        task["barrier"] = True
    if recommended_concurrency:
        task["recommendedConcurrency"] = recommended_concurrency
    if priority is not None:
        task["priority"] = priority
    if requires_approval:
        task["approvalClass"] = "mutation"
        task["approvalSummary"] = f"{operation} mutates the production workspace."
    return task


def _resolve_plan_files(raw_values: Any, root: str, default_scan: bool = False) -> list[str]:
    values = _resolve_string_list(raw_values, root) if raw_values is not None else []
    if not values and default_scan:
        return _scan_markdown_files(root)
    resolved: list[str] = []
    for value in values:
        if _has_glob_pattern(value):
            resolved.extend(_expand_plan_glob(value, root))
            continue
        rel = _normalize_plan_path(value, root)
        path = (_WORKSPACE_PATH / rel).resolve()
        try:
            path.relative_to(_WORKSPACE_PATH)
        except ValueError:
            raise ValueError(f"Invalid {root} path: {value}") from None
        if not path.is_file():
            raise ValueError(f"{root} file does not exist: {value}")
        resolved.append(rel)
    return _unique_strings(resolved)


def _scan_markdown_files(root: str) -> list[str]:
    base = (_WORKSPACE_PATH / root).resolve()
    if not base.is_dir():
        return []
    files: list[str] = []
    for path in sorted(base.rglob("*.md")):
        if root == "deliverables" and any(part.startswith(".tmp.") or part.startswith(".changes.") for part in path.parts):
            continue
        files.append(_relative_workspace_path(path))
    return files


def _normalize_plan_path(value: str, root: str) -> str:
    path = Path(value)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError(f"Invalid {root} path: {value}")
    root_parts = Path(root).parts
    if path.parts[: len(root_parts)] == root_parts:
        return path.as_posix()
    return Path(root, path).as_posix()


def _expand_plan_glob(value: str, root: str) -> list[str]:
    pattern = Path(value)
    if pattern.is_absolute() or ".." in pattern.parts:
        raise ValueError(f"Invalid {root} glob: {value}")
    normalized = _normalize_plan_path(value, root)
    matches = sorted(path for path in _WORKSPACE_PATH.glob(normalized) if path.is_file())
    if not matches:
        raise ValueError(f"No files match {value}")
    return [_relative_workspace_path(path) for path in matches]


def _safe_optional_config_path(request_args: dict[str, Any]) -> str | None:
    return _resolve_config_path(request_args.get("configPath")) if request_args.get("configPath") else None


def _deliverable_for_template(template: str) -> str:
    normalized = _normalize_target_path(template, "templates")
    path = (_WORKSPACE_PATH / normalized).resolve()
    fallback = Path(normalized).relative_to("templates").as_posix()
    return _normalize_target_path(_template_output(path, fallback), "deliverables")


def _file_refs(paths: list[str]) -> list[dict[str, Any]]:
    return [{"type": "file", "ref": path, "label": Path(path).name} for path in paths]


def _ingest_plan_ref(paths: list[str]) -> dict[str, Any]:
    digest = hashlib.sha256("\n".join(paths).encode("utf-8")).hexdigest()[:16]
    return {"type": "file", "ref": f".wiki/ingest-plans/{digest}.json", "label": "ingest plan"}


def _idempotency_key(
    workspace_revision: str,
    capability: str,
    operation: str,
    arguments: dict[str, Any],
    input_refs: list[dict[str, Any]],
) -> str:
    payload = {
        "workspaceRevision": workspace_revision,
        "capability": capability,
        "operation": operation,
        "arguments": arguments,
        "checksums": _checksums_for_refs(input_refs),
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _checksums_for_refs(refs: list[dict[str, Any]]) -> dict[str, str]:
    checksums: dict[str, str] = {}
    for ref in refs:
        path_value = str(ref.get("ref") or "")
        path = (_WORKSPACE_PATH / path_value).resolve()
        try:
            path.relative_to(_WORKSPACE_PATH)
        except ValueError:
            continue
        if path.is_file():
            checksums[path_value] = hashlib.sha256(path.read_bytes()).hexdigest()
    return checksums


def _task_id(prefix: str, path_value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", Path(path_value).with_suffix("").as_posix()).strip("-").lower()
    digest = hashlib.sha256(path_value.encode("utf-8")).hexdigest()[:8]
    return f"{prefix}-{slug[:48]}-{digest}"


def _must_aggregate(count: int, constraints: dict[str, Any]) -> bool:
    max_tasks = constraints["maxTasks"]
    return max_tasks is not None and count > max_tasks


def _groups_for_tasks(
    tasks: list[dict[str, Any]],
    group_id: str,
    label: str,
    recommended_concurrency: int,
    progress_weight: int,
) -> list[dict[str, Any]]:
    if not tasks:
        return []
    return [{
        "id": group_id,
        "label": label,
        "recommendedConcurrency": recommended_concurrency,
        "progressWeight": progress_weight,
    }]


def _aggregate_if_needed(
    tasks: list[dict[str, Any]],
    constraints: dict[str, Any],
    workspace_revision: str,
    capability: str,
) -> list[dict[str, Any]]:
    max_tasks = constraints["maxTasks"]
    if max_tasks is None or len(tasks) <= max_tasks:
        return tasks
    arguments = {
        "tasks": [
            {
                "operation": task["operation"],
                "arguments": task.get("arguments") or {},
            }
            for task in tasks
        ]
    }
    input_refs = _unique_refs([ref for task in tasks for ref in task.get("inputRefs", [])])
    expected_refs = _unique_refs([ref for task in tasks for ref in task.get("expectedOutputRefs", [])])
    locks = sorted({scope for task in tasks for scope in task.get("locks", [])} or {"workspace-write"})
    return [
        _planned_task(
            "pipeline-aggregated",
            f"Run aggregated pipeline with {len(tasks)} planned operation(s)",
            capability,
            "pipeline",
            arguments,
            [],
            False,
            input_refs,
            expected_refs,
            locks,
            constraints,
            workspace_revision,
            progress_weight=sum(float(task.get("progressWeight", 1)) for task in tasks),
        )
    ]


def _pipeline_arguments(request_args: dict[str, Any]) -> dict[str, Any]:
    allowed = {"inputs", "templates", "deliverables", "steps", "stabilize", "configPath", "callerLabel"}
    return {key: value for key, value in request_args.items() if key in allowed}


def _pipeline_requested_steps(request_args: dict[str, Any]) -> list[str]:
    raw_steps = request_args.get("steps")
    if raw_steps is None:
        return ["ingest", "build", "export", "polish"]
    if not isinstance(raw_steps, list):
        raise ValueError("pipeline steps must be an array of strings.")
    steps = [str(step).strip() for step in raw_steps if str(step).strip()]
    invalid = [step for step in steps if step not in {"ingest", "build", "export", "polish"}]
    if invalid:
        raise ValueError(f"Unsupported pipeline planning step: {invalid[0]}")
    return steps


def _unique_refs(refs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unique: list[dict[str, Any]] = []
    seen: set[str] = set()
    for ref in refs:
        key = json.dumps(ref, sort_keys=True)
        if key in seen:
            continue
        seen.add(key)
        unique.append(ref)
    return unique


def _unique_groups(groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unique: list[dict[str, Any]] = []
    seen: set[str] = set()
    for group in groups:
        group_id = str(group.get("id") or "")
        if group_id in seen:
            continue
        seen.add(group_id)
        unique.append(group)
    return unique


def _unique_strings(values: list[str]) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        unique.append(value)
    return unique


def _resolve_steps(job_type: str, raw_steps: Any) -> list[str]:
    if job_type == "pipeline":
        if not isinstance(raw_steps, list) or not raw_steps:
            return ["ingest", "build", "export", "polish"]
        steps = [str(item).strip() for item in raw_steps if str(item).strip()]
    elif job_type in {"doctor", "copy", "ingest", "ingest_plan", "ingest_apply", "taxonomy", "build", "export", "polish", "restore"}:
        steps = [job_type]
    else:
        raise ValueError(f"Unknown production job type: {job_type}")
    invalid = [step for step in steps if step not in {"doctor", "copy", "ingest", "ingest_plan", "ingest_apply", "taxonomy", "build", "export", "polish", "restore"}]
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
        if ".." in Path(item).parts:
            raise ValueError(f"Invalid {name} path: {item}")
    return items


def _has_glob_pattern(value: str) -> bool:
    return any(char in value for char in "*?[")


def _expand_input_globs(inputs: list[str]) -> list[str]:
    expanded: list[str] = []
    for item in inputs:
        if not _has_glob_pattern(item):
            expanded.append(item)
            continue
        pattern = Path(item)
        if pattern.is_absolute() or ".." in pattern.parts:
            raise ValueError(f"Invalid input glob: {item}")
        matches = sorted(path for path in _WORKSPACE_PATH.glob(pattern.as_posix()) if path.is_file())
        if not matches:
            raise ValueError(f"No files match {item}")
        expanded.extend(_relative_workspace_path(path) for path in matches)
    return expanded


def _normalize_target_path(value: str, root: str) -> str:
    path = Path(value)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError(f"Invalid {root} path: {value}")
    parts = path.parts
    if parts and parts[0] == root:
        return path.as_posix()
    return Path(root, path).as_posix()


def _template_deliverable_scope(template: str) -> str:
    relative_template = _normalize_target_path(template, "templates")
    template_path = (_WORKSPACE_PATH / relative_template).resolve()
    fallback = Path(relative_template).relative_to("templates").as_posix()
    output = _template_output(template_path, fallback) if template_path.is_file() else fallback
    return f"deliverable:{_normalize_target_path(output, 'deliverables')}"


def _job_lock_scopes(
    steps: list[str],
    inputs: list[str],
    templates: list[str],
    deliverables: list[str],
) -> list[str]:
    scopes: set[str] = set()
    if any(step in {"copy", "ingest", "ingest_apply", "taxonomy", "restore"} for step in steps):
        scopes.add("workspace-write")
    if "build" in steps:
        if templates:
            scopes.update(_template_deliverable_scope(template) for template in templates)
        else:
            scopes.add("workspace-write")
    if any(step in {"export", "polish"} for step in steps):
        scopes.update(f"deliverable:{_normalize_target_path(deliverable, 'deliverables')}" for deliverable in deliverables)
    if not scopes:
        scopes.add("read")
    return sorted(scopes)


def _resolve_config_path(value: Any) -> str | None:
    if value is None or str(value).strip() == "":
        return None
    item = str(value).strip()
    path = Path(item)
    file_name = path.name
    if item.startswith("-") or path.is_absolute() or ".." in path.parts:
        raise ValueError(f"Invalid configPath: {item}")
    if file_name != ".wikirc.yaml" and file_name != ".wikirc.yml" and not (
        file_name.startswith(".wikirc.yaml.") or file_name.startswith(".wikirc.yml.")
    ):
        raise ValueError(f"Invalid configPath: {item}")
    resolved = (_WORKSPACE_PATH / path).resolve()
    try:
        resolved.relative_to(_WORKSPACE_PATH)
    except ValueError:
        raise ValueError(f"Invalid configPath: {item}") from None
    if not resolved.is_file():
        raise ValueError(f"configPath does not exist: {item}")
    return path.as_posix()


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
    caller_info = f" caller={job['callerLabel']}" if job.get("callerLabel") else ""
    _append_log(job_id, f"[start] workspace={_WORKSPACE_NAME} type={job['type']}{caller_info}")
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
                cli_kwargs = {
                    "stabilize": bool(job.get("stabilize", False)),
                    "config_path": job.get("configPath"),
                    "job_metadata": job,
                }
                if step["name"] == "restore":
                    cli_kwargs.update({
                        "restore_file": job.get("restoreFile"),
                        "restore_revision": job.get("restoreRevision"),
                        "restore_run": job.get("restoreRun"),
                        "dry_run": bool(job.get("dryRun", False)),
                    })
                exit_code = await _run_cli_step(
                    job_id,
                    step["name"],
                    job.get("inputs", []),
                    job.get("templates", []),
                    job.get("deliverables", []),
                    **cli_kwargs,
                )
                step["exitCode"] = exit_code
                step["result"] = _step_result_from_logs(job_id, step["name"])
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
        job["producedFiles"] = _produced_files_from_job(job)
        for step in job["steps"]:
            if step.get("status") == "running":
                step["status"] = job["status"]
                step["finishedAt"] = _now()
        _save_job(job)
        _clear_locks(job_id)
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


async def _run_cli_step(
    job_id: str,
    step: str,
    inputs: list[str],
    templates: list[str],
    deliverables: list[str],
    stabilize: bool = False,
    config_path: str | None = None,
    restore_file: str | None = None,
    restore_revision: str | None = None,
    restore_run: str | None = None,
    dry_run: bool = False,
    job_metadata: dict[str, Any] | None = None,
) -> int:
    env = dict(os.environ)
    env["WIKI_RUN_CALLER"] = job_id
    metadata = job_metadata or {}
    env["WIKI_RUN_ID"] = str(metadata.get("runId") or job_id)
    env["WIKI_TASK_ID"] = str(metadata.get("callerLabel") or step)
    capability = str(metadata.get("capability") or "").strip()
    if capability:
        env["WIKI_CAPABILITY"] = capability
    else:
        env.pop("WIKI_CAPABILITY", None)
    job_idempotency = metadata.get("idempotencyKey")
    if job_idempotency:
        env["WIKI_IDEMPOTENCY_KEY"] = str(job_idempotency)
    else:
        env.pop("WIKI_IDEMPOTENCY_KEY", None)
    if config_path:
        env["WIKI_CONFIG_PATH"] = config_path
        _append_log(job_id, f"[config] WIKI_CONFIG_PATH={config_path}")
    commands = _step_commands(
        step,
        inputs,
        templates,
        deliverables,
        stabilize=stabilize,
        restore_file=restore_file,
        restore_revision=restore_revision,
        restore_run=restore_run,
        dry_run=dry_run,
    )
    for command in commands:
        _append_log(job_id, f"[cmd] cwd={_WORKSPACE_PATH} {' '.join(command)}")
        proc = await asyncio.create_subprocess_exec(
            *command,
            cwd=str(_WORKSPACE_PATH),
            env=env,
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


def _exported_files_from_logs(lines: list[str]) -> list[str]:
    files: list[str] = []
    seen: set[str] = set()
    for line in lines:
        match = re.search(r"Exported\s+→\s+(.+)$", str(line))
        if not match:
            continue
        value = match.group(1).strip()
        if value and value not in seen:
            seen.add(value)
            files.append(value)
    return files


def _ingest_plan_files_from_logs(lines: list[str]) -> list[str]:
    files: list[str] = []
    seen: set[str] = set()
    for line in lines:
        match = re.search(r"Ingest plan written:\s*(.+)$", str(line))
        if not match:
            continue
        value = match.group(1).strip()
        if value and value not in seen:
            seen.add(value)
            files.append(value)
    return files


def _step_result_from_logs(job_id: str, step: str) -> dict[str, Any]:
    if step == "ingest_plan":
        plan_files = _ingest_plan_files_from_logs(_read_log_tail(job_id, 500))
        return {"producedFiles": plan_files} if plan_files else {}
    if step not in {"export", "polish"}:
        return {}
    produced_files = _exported_files_from_logs(_read_log_tail(job_id, 500))
    return {"producedFiles": produced_files} if produced_files else {}


def _produced_files_from_job(job: dict[str, Any]) -> list[str]:
    files: list[str] = []
    seen: set[str] = set()
    for step in job.get("steps", []):
        result = step.get("result") if isinstance(step, dict) else None
        if not isinstance(result, dict):
            continue
        for value in result.get("producedFiles") or []:
            text = str(value)
            if text and text not in seen:
                seen.add(text)
                files.append(text)
    return files


def _output_refs_for_job(job: dict[str, Any]) -> list[dict[str, Any]]:
    refs: list[dict[str, Any]] = []
    for path_value in job.get("producedFiles") or _produced_files_from_job(job):
        refs.append({"type": "file", "path": str(path_value)})
    for template in job.get("templates") or []:
        if job.get("type") == "build" or any(step.get("name") == "build" for step in job.get("steps", []) if isinstance(step, dict)):
            refs.append({"type": "file", "path": _template_deliverable_scope(str(template)).replace("deliverable:", "")})
    for deliverable in job.get("deliverables") or []:
        refs.append({"type": "file", "path": _normalize_target_path(str(deliverable), "deliverables")})
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for ref in refs:
        key = json.dumps(ref, sort_keys=True)
        if key not in seen:
            seen.add(key)
            unique.append(ref)
    return unique


def _job_progress(job: dict[str, Any], log_tail: list[str]) -> dict[str, Any]:
    current_step = _current_step(job)
    trace_file = _trace_file_from_logs(log_tail)
    trace = _parse_trace_progress(trace_file) if trace_file else {}
    status = str(job.get("status") or "")
    progress = {
        "status": status,
        "phase": trace.get("phase") or current_step.get("name") or status,
        "label": trace.get("label") or _progress_label(job, current_step),
        "detail": trace.get("detail") or _progress_detail(job, current_step),
        "percent": trace.get("percent") if trace.get("percent") is not None else _step_percent(job),
        "currentStep": current_step.get("name"),
        "traceFile": trace_file,
        "template": trace.get("template"),
        "deliverable": trace.get("deliverable"),
        "batchIndex": trace.get("batchIndex"),
        "batchCount": trace.get("batchCount"),
        "instructionCount": trace.get("instructionCount"),
        "stabilizeKept": trace.get("stabilizeKept"),
        "stabilizeMerged": trace.get("stabilizeMerged"),
        "stabilizeInserted": trace.get("stabilizeInserted"),
        "stabilizeRemoved": trace.get("stabilizeRemoved"),
        "stabilizeSection": trace.get("stabilizeSection"),
        "lastEvent": trace.get("lastEvent"),
        "lastEventAt": trace.get("lastEventAt"),
        "waitMs": trace.get("waitMs"),
        "retryAt": trace.get("retryAt"),
        "currentBatchStartedAt": trace.get("currentBatchStartedAt"),
        "inputTokens": trace.get("inputTokens"),
        "outputTokens": trace.get("outputTokens"),
    }
    if status == "done":
        progress["percent"] = 100
        progress["detail"] = progress["detail"] or "Job complete"
    if status in {"failed", "cancelled"}:
        progress["detail"] = str(job.get("error") or progress["detail"] or status)
    return progress


def _current_step(job: dict[str, Any]) -> dict[str, Any]:
    steps = job.get("steps") if isinstance(job.get("steps"), list) else []
    for step in steps:
        if isinstance(step, dict) and step.get("status") in {"running", "queued", "pending"}:
            return step
    for step in reversed(steps):
        if isinstance(step, dict):
            return step
    return {}


def _progress_label(job: dict[str, Any], step: dict[str, Any]) -> str:
    targets = [*(job.get("templates") or []), *(job.get("deliverables") or [])]
    target = ", ".join(str(item) for item in targets if item)
    step_name = str(step.get("name") or job.get("type") or "production")
    return f"{step_name} {target}".strip()


def _progress_detail(job: dict[str, Any], step: dict[str, Any]) -> str:
    status = step.get("status") or job.get("status")
    return f"Step {step.get('name')}: {status}" if step.get("name") else str(status or "")


def _step_percent(job: dict[str, Any]) -> int | None:
    steps = job.get("steps") if isinstance(job.get("steps"), list) else []
    if not steps:
        return None
    done = sum(1 for step in steps if isinstance(step, dict) and step.get("status") == "done")
    status = str(job.get("status") or "")
    if status == "done":
        return 100
    if status in {"failed", "cancelled"}:
        return max(0, min(99, round((done / len(steps)) * 100)))
    running_bonus = 0.35 if any(isinstance(step, dict) and step.get("status") == "running" for step in steps) else 0
    return max(0, min(99, round(((done + running_bonus) / len(steps)) * 100)))


def _trace_file_from_logs(lines: list[str]) -> str | None:
    for line in reversed(lines):
        match = re.search(r"Trace file:\s*(.+)$", str(line))
        if match:
            return match.group(1).strip()
    return None


_WAIT_EVENTS = frozenset({
    "provider:throttle",
    "llm:rate-limit-wait",
    "rerank:rate-limit-wait",
    "embedding:rate-limit-wait",
})


def _service_name(event_name: str, fields: dict[str, str]) -> str:
    label = fields.get("label", "")
    if event_name.startswith("embedding:") or label == "embedding":
        return "Embedding"
    if event_name.startswith("rerank:") or label == "rerank":
        return "Rerank"
    return "LLM"


def _quota_wait_detail(event_name: str, fields: dict[str, str], retry_at_iso: str | None = None) -> str:
    service = _service_name(event_name, fields)
    retry_iso = retry_at_iso or fields.get("retryAt")
    wait_ms = _int_field(fields.get("waitMs"))
    remaining_s: int | None = None
    if retry_iso:
        with contextlib.suppress(Exception):
            retry_dt = datetime.fromisoformat(retry_iso.replace("Z", "+00:00"))
            remaining_s = max(0, round((retry_dt - datetime.now(timezone.utc)).total_seconds()))
    if remaining_s is None and wait_ms is not None:
        remaining_s = max(0, round(wait_ms / 1000))
    if remaining_s is not None and remaining_s > 0:
        return f"{service} quota wait, resuming in {remaining_s}s"
    return f"{service} quota wait, resuming shortly"


def _parse_trace_progress(trace_file: str) -> dict[str, Any]:
    path = (_WORKSPACE_PATH / trace_file).resolve()
    try:
        path.relative_to(_WORKSPACE_PATH)
    except ValueError:
        return {}
    if not path.exists():
        return {}
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    state: dict[str, Any] = {"traceFile": trace_file}
    ingest_input_count: int | None = None
    ingest_done_count = 0
    stabilize_kept = 0
    stabilize_merged = 0
    stabilize_inserted = 0
    stabilize_removed = 0
    for line in lines:
        event = _trace_event(line)
        if not event:
            continue
        name = event["name"]
        fields = event["fields"]
        state["lastEvent"] = name
        state["lastEventAt"] = event["at"]
        if name not in _WAIT_EVENTS:
            state.pop("waitMs", None)
            state.pop("retryAt", None)
        if name == "build:run-start":
            state["phase"] = "build"
            state["templateCount"] = _int_field(fields.get("templateCount"))
        elif name == "ingest:run-start":
            state["phase"] = "ingest"
            ingest_input_count = _int_field(fields.get("inputCount"))
            state["sourceCount"] = ingest_input_count
            state["detail"] = "Selecting sources"
            state["percent"] = 1
        elif name == "ingest:source-selection":
            state["phase"] = "ingest"
            resolved_count = _int_field(fields.get("resolvedCount"))
            if resolved_count is not None:
                ingest_input_count = resolved_count
                state["sourceCount"] = resolved_count
                state["sourceDoneCount"] = ingest_done_count
                state["detail"] = f"{resolved_count} source(s) to ingest"
                state["percent"] = 1 if resolved_count > 0 else 100
        elif name == "ingest:source-start":
            state["phase"] = "ingest"
            state["source"] = fields.get("sourcePath") or state.get("source")
            source_name = Path(str(state.get("source") or "")).name
            state["label"] = f"Ingest {source_name}".strip()
            if ingest_input_count is not None and ingest_input_count > 0:
                state["sourceIndex"] = ingest_done_count
                state["sourceCount"] = ingest_input_count
                state["sourceDoneCount"] = ingest_done_count
                state["detail"] = f"Source {ingest_done_count + 1}/{ingest_input_count}"
                state["percent"] = _ingest_source_percent(ingest_done_count, ingest_input_count, 0.15)
        elif name == "ingest:source":
            state["phase"] = "ingest"
            state["source"] = fields.get("source") or state.get("source")
            source_name = Path(str(state.get("source") or "")).name
            state["label"] = f"Ingest {source_name}".strip()
        elif name == "ingest:prompt":
            state["phase"] = "ingest"
            state["source"] = fields.get("source") or state.get("source")
            source_name = Path(str(state.get("source") or "")).name
            state["label"] = f"Ingest {source_name}".strip()
            state["detail"] = "Preparing LLM"
            if ingest_input_count is not None and ingest_input_count > 0:
                state["percent"] = _ingest_source_percent(ingest_done_count, ingest_input_count, 0.35)
        elif name == "ingest:plan":
            state["phase"] = "ingest"
            state["source"] = fields.get("source") or state.get("source")
            source_name = Path(str(state.get("source") or "")).name
            state["label"] = f"Ingest {source_name}".strip()
            state["detail"] = "Ingestion plan received"
            if ingest_input_count is not None and ingest_input_count > 0:
                state["percent"] = _ingest_source_percent(ingest_done_count, ingest_input_count, 0.85)
        elif name == "ingest:source-done":
            state["phase"] = "ingest"
            ingest_done_count += 1
            state["source"] = fields.get("source") or state.get("source")
            source_name = Path(str(state.get("source") or "")).name
            state["label"] = f"Ingest {source_name}".strip()
            if ingest_input_count is not None and ingest_input_count > 0:
                state["sourceIndex"] = max(0, ingest_done_count - 1)
                state["sourceCount"] = ingest_input_count
                state["sourceDoneCount"] = ingest_done_count
                state["detail"] = f"Source {ingest_done_count}/{ingest_input_count}"
                state["percent"] = max(1, min(99, round((ingest_done_count / ingest_input_count) * 100)))
        elif name == "ingest:run-done":
            state["phase"] = "done"
            state["detail"] = "Ingest complete"
            state["percent"] = 100
        elif name == "build:template-start":
            state["phase"] = "build"
            state["template"] = fields.get("template")
            state["deliverable"] = fields.get("output")
            state["instructionCount"] = _int_field(fields.get("instructions"))
            state["label"] = f"Build {state.get('template') or ''}".strip()
        elif name == "build:stabilize-start":
            state["phase"] = "stabilize"
            state["template"] = fields.get("template") or state.get("template")
            state["deliverable"] = fields.get("output") or state.get("deliverable")
            state["label"] = f"Stabilize {state.get('template') or ''}".strip()
            state["detail"] = "Preparing stabilization"
            state["percent"] = 95
        elif name == "build:stabilize-section-skip":
            stabilize_kept += 1
            section = _heading_path_label(fields.get("headingPath"))
            state["phase"] = "stabilize"
            state["label"] = f"Stabilize {state.get('template') or ''}".strip()
            state["detail"] = f"Section kept: {section}" if section else "Section kept"
            state["percent"] = 96
            state["stabilizeKept"] = stabilize_kept
            state["stabilizeSection"] = section
        elif name == "build:stabilize-section-llm":
            stabilize_merged += 1
            section = _heading_path_label(fields.get("headingPath"))
            state["phase"] = "stabilize"
            state["label"] = f"Stabilize {state.get('template') or ''}".strip()
            state["detail"] = f"Stabilisation LLM: {section}" if section else "Stabilisation LLM"
            state["percent"] = 97
            state["stabilizeMerged"] = stabilize_merged
            state["stabilizeSection"] = section
        elif name == "build:stabilize-done":
            kept = _int_field(fields.get("kept"))
            merged = _int_field(fields.get("merged"))
            inserted = _int_field(fields.get("inserted"))
            removed = _int_field(fields.get("removed"))
            if kept is not None:
                stabilize_kept = kept
            if merged is not None:
                stabilize_merged = merged
            if inserted is not None:
                stabilize_inserted = inserted
            if removed is not None:
                stabilize_removed = removed
            state["phase"] = "stabilize"
            state["label"] = f"Stabilize {state.get('template') or ''}".strip()
            state["detail"] = (
                f"Stabilization complete: {stabilize_kept} kept · "
                f"{stabilize_merged} modified · {stabilize_inserted} new · "
                f"{stabilize_removed} removed"
            )
            state["percent"] = 98
            state["stabilizeKept"] = stabilize_kept
            state["stabilizeMerged"] = stabilize_merged
            state["stabilizeInserted"] = stabilize_inserted
            state["stabilizeRemoved"] = stabilize_removed
        elif name == "build:stabilize-failed":
            state["phase"] = "stabilize"
            state["template"] = fields.get("template") or state.get("template")
            state["deliverable"] = fields.get("output") or state.get("deliverable")
            state["label"] = f"Stabilize {state.get('template') or ''}".strip()
            message = fields.get("message")
            state["detail"] = f"Stabilization failed: {message}" if message else "Stabilization failed"
            state["percent"] = 98
        elif name == "trace:summary":
            input_tokens = _int_field(fields.get("llmInputTokens"))
            output_tokens = _int_field(fields.get("llmOutputTokens"))
            if input_tokens is not None:
                state["inputTokens"] = input_tokens
            if output_tokens is not None:
                state["outputTokens"] = output_tokens
        elif name == "llm:start":
            is_stabilize_llm = fields.get("label") == "build:stabilize"
            state["phase"] = "stabilize" if is_stabilize_llm else "llm"
            state["source"] = fields.get("source") or state.get("source")
            state["template"] = fields.get("template") or state.get("template")
            state["batchIndex"] = _int_field(fields.get("batchIndex"))
            state["batchCount"] = _int_field(fields.get("batchCount"))
            state["instructionCount"] = _int_field(fields.get("instructionCount")) or state.get("instructionCount")
            state["currentBatchStartedAt"] = event["at"]
            if is_stabilize_llm:
                section = str(state.get("stabilizeSection") or "")
                state["label"] = f"Stabilize {state.get('template') or ''}".strip()
                state["detail"] = f"Stabilisation LLM: {section}" if section else "Stabilisation LLM"
                state["percent"] = 97
            elif state.get("source") and not state.get("template"):
                source_name = Path(str(state.get("source") or "")).name
                state["label"] = f"Ingest {source_name}".strip()
                state["detail"] = "LLM running"
            else:
                state["label"] = f"Build {state.get('template') or ''}".strip()
            if not is_stabilize_llm and state.get("batchIndex") is not None and state.get("batchCount"):
                state["detail"] = f"Batch {state['batchIndex'] + 1}/{state['batchCount']} · LLM running"
                state["percent"] = _batch_percent(state["batchIndex"], state["batchCount"], False)
        elif name == "llm:end":
            is_stabilize_llm = fields.get("label") == "build:stabilize"
            state["phase"] = "stabilize" if is_stabilize_llm else "llm"
            state["source"] = fields.get("source") or state.get("source")
            state["batchIndex"] = _int_field(fields.get("batchIndex"))
            state["batchCount"] = _int_field(fields.get("batchCount"))
            if is_stabilize_llm:
                section = str(state.get("stabilizeSection") or "")
                state["label"] = f"Stabilize {state.get('template') or ''}".strip()
                state["detail"] = f"LLM stabilization complete: {section}" if section else "LLM stabilization complete"
                state["percent"] = 97
            elif state.get("source") and not state.get("template"):
                source_name = Path(str(state.get("source") or "")).name
                state["label"] = f"Ingest {source_name}".strip()
                state["detail"] = "LLM complete"
            if not is_stabilize_llm and state.get("batchIndex") is not None and state.get("batchCount"):
                state["detail"] = f"Batch {state['batchIndex'] + 1}/{state['batchCount']} complete"
                state["percent"] = _batch_percent(state["batchIndex"], state["batchCount"], True)
        elif name == "build:template-done":
            state["phase"] = "build"
            state["template"] = fields.get("template") or state.get("template")
            state["deliverable"] = fields.get("output") or state.get("deliverable")
            state["detail"] = "Template complete"
            state["percent"] = 98
        elif name == "build:run-done":
            state["phase"] = "done"
            state["detail"] = "Build complete"
            state["percent"] = 100
            state.pop("waitMs", None)
            state.pop("retryAt", None)
        elif name == "provider:throttle":
            wait_ms = _int_field(fields.get("waitMs"))
            retry_at_iso: str | None = None
            with contextlib.suppress(Exception):
                event_dt = datetime.fromisoformat(event["at"].replace("Z", "+00:00"))
                retry_at_iso = (event_dt + timedelta(milliseconds=wait_ms or 0)).isoformat()
            state["waitMs"] = wait_ms
            state["retryAt"] = retry_at_iso
            state["detail"] = _quota_wait_detail(name, fields, retry_at_iso)
        elif name in {"llm:rate-limit-wait", "rerank:rate-limit-wait", "embedding:rate-limit-wait"}:
            wait_ms = _int_field(fields.get("waitMs"))
            retry_at = fields.get("retryAt")
            state["waitMs"] = wait_ms
            state["retryAt"] = retry_at
            state["detail"] = _quota_wait_detail(name, fields)
        elif name.startswith("export:"):
            state["phase"] = "export"
            state["detail"] = name
    return state


def _trace_event(line: str) -> dict[str, Any] | None:
    match = re.match(r"^(?P<at>\S+)\s+\+\d+ms\s+\S+\s+(?P<name>\S+)\s*(?P<rest>.*)$", line)
    if not match:
        return None
    return {
        "at": match.group("at"),
        "name": match.group("name"),
        "fields": _trace_fields(match.group("rest")),
    }


def _trace_fields(rest: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for key, value in re.findall(r"(\w+)=((?:\"[^\"]*\")|(?:\[[^\]]*\])|(?:\S+))", rest):
        fields[key] = value.strip('"')
    return fields


def _heading_path_label(value: str | None) -> str:
    if not value:
        return ""
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return " > ".join(str(item) for item in parsed if str(item))
    except Exception:
        pass
    return value.strip()


def _int_field(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _ingest_source_percent(done_count: int, source_count: int, source_fraction: float) -> int:
    """Map a real per-source ingest milestone onto the whole ingest job."""
    fraction = max(0.0, min(1.0, float(source_fraction)))
    return max(1, min(99, round(((done_count + fraction) / source_count) * 100)))


def _batch_percent(batch_index: int, batch_count: int, completed: bool) -> int | None:
    if batch_count <= 0:
        return None
    current = batch_index + (1 if completed else 0.25)
    return max(1, min(99, round((current / batch_count) * 100)))


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
        "inputs": job.get("inputs") or [],
        "templates": job.get("templates") or [],
        "deliverables": job.get("deliverables") or [],
        "lockScopes": job.get("lockScopes") or [],
        "createdAt": job.get("createdAt"),
        "updatedAt": job.get("updatedAt"),
        "durationSeconds": job.get("durationSeconds"),
        "error": job.get("error"),
        "producedFiles": job.get("producedFiles") or _produced_files_from_job(job),
    }


def _step_commands(
    step: str,
    inputs: list[str],
    templates: list[str],
    deliverables: list[str],
    stabilize: bool = False,
    restore_file: str | None = None,
    restore_revision: str | None = None,
    restore_run: str | None = None,
    dry_run: bool = False,
) -> list[list[str]]:
    if step == "copy":
        return []
    command = [*_STEP_COMMANDS[step]]
    if step in {"ingest", "ingest_plan", "ingest_apply"}:
        command.extend(inputs)
        return [command]
    if step == "build":
        if stabilize:
            command.append("--stabilize")
        command.extend(templates)
        return [command]
    if step in {"export", "polish"}:
        return [[*command, deliverable] for deliverable in deliverables]
    if step == "restore":
        if restore_run:
            command.extend(["--run", restore_run])
        elif restore_file and restore_revision:
            command.extend(["--file", restore_file, "--to", restore_revision])
        else:
            raise ValueError("restore requires file+to or run arguments")
        if dry_run:
            command.append("--dry-run")
        return [command]
    return [command]


def _command_labels(
    steps: list[str],
    inputs: list[str],
    templates: list[str],
    deliverables: list[str],
    stabilize: bool = False,
    restore_file: str | None = None,
    restore_revision: str | None = None,
    restore_run: str | None = None,
    dry_run: bool = False,
) -> list[str]:
    labels: list[str] = []
    for step in steps:
        if step == "copy":
            labels.append(f"copy {len(_parse_imports())} import(s) to raw/untracked")
            continue
        labels.extend(" ".join(command) for command in _step_commands(
            step,
            inputs,
            templates,
            deliverables,
            stabilize=stabilize,
            restore_file=restore_file,
            restore_revision=restore_revision,
            restore_run=restore_run,
            dry_run=dry_run,
        ))
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
    print(f"{_LOG_PREFIX} Streamable HTTP on http://{host}:{port}/mcp")
    print(f"{_LOG_PREFIX} path={_WORKSPACE_PATH}")
    uvicorn.run(create_starlette_app(), host=host, port=port)


if __name__ == "__main__":
    main()
