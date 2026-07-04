import asyncio
import importlib.util
import json
import os
import sys
import tempfile
import types
import unittest
from dataclasses import dataclass
from pathlib import Path


@dataclass
class TextContent:
    type: str
    text: str


class Tool:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class Server:
    def __init__(self, *_args, **_kwargs):
        pass

    def list_tools(self):
        return lambda fn: fn

    def call_tool(self):
        return lambda fn: fn


def install_stubs():
    modules = {
        "mcp": types.ModuleType("mcp"),
        "mcp.server": types.ModuleType("mcp.server"),
        "mcp.server.streamable_http_manager": types.ModuleType("mcp.server.streamable_http_manager"),
        "mcp.types": types.ModuleType("mcp.types"),
    }
    modules["mcp.server"].Server = Server
    modules["mcp.server.streamable_http_manager"].StreamableHTTPSessionManager = object
    modules["mcp.types"].TextContent = TextContent
    modules["mcp.types"].Tool = Tool
    sys.modules.update(modules)
    for name in [
        "starlette.applications",
        "starlette.middleware",
        "starlette.middleware.base",
        "starlette.middleware.cors",
        "starlette.requests",
        "starlette.responses",
        "starlette.routing",
        "starlette.types",
        "uvicorn",
    ]:
        sys.modules[name] = types.ModuleType(name)
    sys.modules["starlette.applications"].Starlette = object
    sys.modules["starlette.middleware"].Middleware = lambda *args, **kwargs: (args, kwargs)
    sys.modules["starlette.middleware.base"].BaseHTTPMiddleware = object
    sys.modules["starlette.middleware.cors"].CORSMiddleware = object
    sys.modules["starlette.requests"].Request = object
    sys.modules["starlette.responses"].HTMLResponse = object
    sys.modules["starlette.responses"].PlainTextResponse = object
    sys.modules["starlette.routing"].Mount = object
    sys.modules["starlette.types"].Receive = object
    sys.modules["starlette.types"].Scope = dict
    sys.modules["starlette.types"].Send = object
    sys.modules["uvicorn"].run = lambda *args, **kwargs: None


def load_module(workspace):
    install_stubs()
    os.environ["WIKI_WORKSPACE_PATH"] = str(workspace)
    os.environ["WORKSPACE_NAME"] = "test-workspace"
    os.environ["PRODUCTION_REQUIRE_CONFIRMATION"] = "true"
    path = Path(__file__).with_name("production_mcp_server.py")
    spec = importlib.util.spec_from_file_location("production_mcp_server_test_subject", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ProductionMcpServerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.workspace = Path(self.tmp.name)
        (self.workspace / "templates").mkdir()
        (self.workspace / "deliverables").mkdir()
        self.server = load_module(self.workspace)

    def tearDown(self):
        self.tmp.cleanup()

    def payload(self, result):
        return json.loads(result[0].text)

    def test_start_job_dry_run_returns_plan_without_confirmation(self):
        result = asyncio.run(self.server._tool_start_job({"type": "doctor", "dryRun": True}))
        payload = self.payload(result)
        self.assertTrue(payload["ok"])
        self.assertTrue(payload["dryRun"])
        self.assertEqual(payload["plan"]["steps"], ["doctor"])

    def test_ingest_dry_run_accepts_target_inputs(self):
        result = asyncio.run(
            self.server._tool_start_job(
                {
                    "type": "ingest",
                    "inputs": ["raw/untracked/doc-a.md", "doc-b.md"],
                    "dryRun": True,
                }
            )
        )
        payload = self.payload(result)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["plan"]["inputs"], ["raw/untracked/doc-a.md", "doc-b.md"])
        self.assertIn("node /app/bin/wiki.js ingest raw/untracked/doc-a.md doc-b.md", payload["commands"])

    def test_mutating_job_requires_confirmation(self):
        with self.assertRaisesRegex(ValueError, "confirm=true"):
            asyncio.run(self.server._tool_start_job({"type": "build"}))

    def test_targeted_build_jobs_can_run_in_parallel_but_conflicting_export_waits(self):
        async def scenario():
            async def hold_job(_job_id):
                await asyncio.Future()

            self.server._run_job = hold_job
            (self.workspace / "templates" / "a.md").write_text("---\noutput: a.md\n---\n# A\n", encoding="utf-8")
            (self.workspace / "templates" / "b.md").write_text("---\noutput: b.md\n---\n# B\n", encoding="utf-8")

            first = self.payload(await self.server._tool_start_job({"type": "build", "templates": ["a.md"], "confirm": True}))
            second = self.payload(await self.server._tool_start_job({"type": "build", "templates": ["b.md"], "confirm": True}))
            conflict = self.payload(await self.server._tool_start_job({"type": "export", "deliverables": ["a.md"], "confirm": True}))

            self.assertTrue(first["ok"])
            self.assertTrue(second["ok"])
            self.assertFalse(conflict["ok"])
            self.assertEqual(conflict["error"], "target_busy")
            self.assertEqual(first["plan"]["lockScopes"], ["deliverable:deliverables/a.md"])
            self.assertEqual(second["plan"]["lockScopes"], ["deliverable:deliverables/b.md"])

            await self.server._tool_cancel_job({"jobId": first["jobId"]})
            await self.server._tool_cancel_job({"jobId": second["jobId"]})

        asyncio.run(scenario())

    def test_log_redaction_masks_secret_values(self):
        masked = self.server._mask_secret_text("Authorization: Bearer abc123 token=runtime password:secret")
        self.assertNotIn("abc123", masked)
        self.assertNotIn("runtime", masked)
        self.assertNotIn("secret", masked)

    def test_read_scope_cannot_start_or_cancel_job(self):
        token = self.server._CURRENT_SCOPES.set({"read"})
        try:
            denied = self.server._require_tool_scope("production_start_job")
            allowed = self.server._require_tool_scope("production_status")
        finally:
            self.server._CURRENT_SCOPES.reset(token)

        self.assertFalse(self.payload(denied)["ok"])
        self.assertIn("write scope", self.payload(denied)["error"])
        self.assertIsNone(allowed)


if __name__ == "__main__":
    unittest.main()
