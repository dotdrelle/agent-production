import asyncio
import importlib.util
import json
import os
import sys
import tempfile
import time
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


def load_module(workspace, env=None):
    install_stubs()
    updates = {
        "WIKI_WORKSPACE_PATH": str(workspace),
        "WORKSPACE_NAME": "test-workspace",
        "PRODUCTION_REQUIRE_CONFIRMATION": "true",
        **(env or {}),
    }
    previous = {key: os.environ.get(key) for key in updates}
    os.environ.update(updates)
    try:
        path = Path(__file__).with_name("production_mcp_server.py")
        spec = importlib.util.spec_from_file_location(f"production_mcp_server_test_subject_{time.time_ns()}", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


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

    def assert_task_graph_fragment(self, fragment):
        self.assertEqual(fragment["contractVersion"], "1")
        self.assertEqual(fragment["agentInstanceId"], "production-main")
        self.assertIn("capability", fragment)
        self.assertIn("summary", fragment)
        self.assertIn("groups", fragment)
        self.assertIn("tasks", fragment)
        self.assertEqual(fragment["summary"]["estimatedTasks"], len(fragment["tasks"]))
        for task in fragment["tasks"]:
            for key in [
                "id",
                "label",
                "requiredCapability",
                "operation",
                "dependsOn",
                "parallelizable",
                "inputRefs",
                "locks",
                "requiresApproval",
                "idempotencyKey",
                "progressWeight",
            ]:
                self.assertIn(key, task)
            self.assertNotIn("executor", task)
            self.assertIsInstance(task["dependsOn"], list)
            self.assertIsInstance(task["inputRefs"], list)
            self.assertIsInstance(task["locks"], list)
            for ref in task["inputRefs"]:
                self.assertNotIn("*", ref["ref"])
                self.assertNotIn("?", ref["ref"])

    def test_agent_describe_returns_valid_contract(self):
        description = self.payload(self.server._tool_agent_describe())

        self.assertEqual(description["contractVersion"], "1")
        self.assertEqual(description["agentType"], "production")
        self.assertEqual(description["agentInstanceId"], "production-main")
        self.assertEqual(description["displayName"], "Production")
        self.assertEqual(description["health"]["status"], "available")
        self.assertEqual(description["orchestration"]["canPlan"], True)
        self.assertEqual(description["orchestration"]["canExecute"], True)
        self.assertEqual(description["orchestration"]["canCancel"], True)
        self.assertEqual(description["orchestration"]["supportsIdempotency"], False)
        self.assertEqual(description["orchestration"]["supportsParallelWorkers"], True)
        self.assertIn("recommendedConcurrency", description["limits"])
        self.assertIn("maxConcurrency", description["limits"])

        capabilities = {item["id"]: item for item in description["capabilities"]}
        self.assertEqual(capabilities["knowledge.update"]["supportedOperations"], ["ingest"])
        self.assertEqual(capabilities["document.build"]["supportedOperations"], ["build"])
        self.assertEqual(capabilities["document.publish"]["supportedOperations"], ["export", "polish"])
        self.assertEqual(capabilities["workspace.diagnose"]["supportedOperations"], ["doctor"])
        self.assertEqual(capabilities["knowledge.pipeline"]["supportedOperations"], ["pipeline"])
        self.assertTrue(capabilities["knowledge.update"]["defaultRequiresApproval"])
        self.assertEqual(capabilities["knowledge.update"]["mutationClass"], "workspace")
        self.assertNotIn("defaultRequiresApproval", capabilities["workspace.diagnose"])

    def test_agent_describe_uses_env_instance_and_limits(self):
        server = load_module(
            self.workspace,
            {
                "PRODUCTION_INSTANCE_ID": "production-test",
                "PRODUCTION_RECOMMENDED_CONCURRENCY": "3",
                "PRODUCTION_MAX_CONCURRENCY": "8",
                "PRODUCTION_MAX_TASKS_PER_PLAN": "42",
                "PRODUCTION_MAX_TASK_DURATION_MS": "900000",
            },
        )
        description = self.payload(server._tool_agent_describe())

        self.assertEqual(description["agentInstanceId"], "production-test")
        self.assertEqual(description["limits"]["recommendedConcurrency"], 3)
        self.assertEqual(description["limits"]["maxConcurrency"], 8)
        self.assertEqual(description["limits"]["maxTasksPerPlan"], 42)
        self.assertEqual(description["limits"]["maxTaskDurationMs"], 900000)

    def test_agent_describe_capabilities_follow_allowed_steps(self):
        server = load_module(
            self.workspace,
            {"PRODUCTION_ALLOWED_STEPS": "doctor,ingest,polish,pipeline"},
        )
        description = self.payload(server._tool_agent_describe())
        capabilities = {item["id"]: item for item in description["capabilities"]}

        self.assertEqual(capabilities["knowledge.update"]["supportedOperations"], ["ingest"])
        self.assertNotIn("document.build", capabilities)
        self.assertEqual(capabilities["document.publish"]["supportedOperations"], ["polish"])
        self.assertEqual(capabilities["workspace.diagnose"]["supportedOperations"], ["doctor"])
        self.assertEqual(capabilities["knowledge.pipeline"]["supportedOperations"], ["pipeline"])

    def test_agent_describe_is_listed_and_callable(self):
        tools = asyncio.run(self.server.list_tools())
        self.assertIn("agent_describe", [tool.name for tool in tools])

        payload = self.payload(asyncio.run(self.server.call_tool("agent_describe", {})))
        self.assertEqual(payload["agentType"], "production")

    def test_agent_plan_ingest_scans_untracked_and_adds_apply_barrier(self):
        (self.workspace / "raw" / "untracked").mkdir(parents=True)
        (self.workspace / "raw" / "untracked" / "b.md").write_text("# B\n", encoding="utf-8")
        (self.workspace / "raw" / "untracked" / "a.md").write_text("# A\n", encoding="utf-8")

        fragment = self.payload(
            self.server._tool_agent_plan(
                {
                    "capability": "knowledge.update",
                    "operation": "ingest",
                    "workspace": {"revision": "rev-1"},
                    "constraints": {"maxConcurrency": 2, "requireApprovalForMutations": True},
                }
            )
        )

        self.assert_task_graph_fragment(fragment)
        self.assertEqual(fragment["capability"], "knowledge.update")
        self.assertEqual([task["operation"] for task in fragment["tasks"]], ["ingest", "ingest", "ingest_apply"])
        self.assertNotIn("export", [task["operation"] for task in fragment["tasks"]])
        self.assertEqual(fragment["groups"][0]["id"], "ingest")
        self.assertEqual(fragment["groups"][0]["recommendedConcurrency"], 2)
        apply_task = fragment["tasks"][-1]
        self.assertTrue(apply_task["barrier"])
        self.assertEqual(apply_task["dependsOnGroup"], "ingest")
        self.assertEqual(apply_task["locks"], ["workspace-write"])
        self.assertTrue(all(task["requiresApproval"] for task in fragment["tasks"]))
        self.assertTrue(all(len(task["idempotencyKey"]) == 64 for task in fragment["tasks"]))
        self.assertEqual(fragment["tasks"][0]["inputRefs"][0]["ref"], "raw/untracked/a.md")
        self.assertEqual(fragment["tasks"][1]["inputRefs"][0]["ref"], "raw/untracked/b.md")

    def test_agent_plan_empty_ingest_returns_empty_fragment(self):
        fragment = self.payload(
            self.server._tool_agent_plan(
                {
                    "capability": "knowledge.update",
                    "operation": "ingest",
                    "workspace": {"revision": "rev-empty"},
                }
            )
        )

        self.assert_task_graph_fragment(fragment)
        self.assertEqual(fragment["tasks"], [])
        self.assertIn("No Markdown files", fragment["summary"]["initialSynthesis"][0])

    def test_agent_plan_build_creates_one_task_per_template(self):
        (self.workspace / "templates" / "a.md").write_text("---\noutput: alpha.md\n---\n# A\n", encoding="utf-8")
        (self.workspace / "templates" / "nested").mkdir()
        (self.workspace / "templates" / "nested" / "b.md").write_text("# B\n", encoding="utf-8")

        fragment = self.payload(
            self.server._tool_agent_plan(
                {
                    "capability": "document.build",
                    "operation": "build",
                    "workspace": {"revision": "rev-build"},
                    "constraints": {"requireApprovalForMutations": True},
                }
            )
        )

        self.assert_task_graph_fragment(fragment)
        self.assertEqual([task["operation"] for task in fragment["tasks"]], ["build", "build"])
        self.assertEqual([task["inputRefs"][0]["ref"] for task in fragment["tasks"]], ["templates/a.md", "templates/nested/b.md"])
        self.assertEqual(
            [task["expectedOutputRefs"][0]["ref"] for task in fragment["tasks"]],
            ["deliverables/alpha.md", "deliverables/nested/b.md"],
        )
        self.assertTrue(all(task["requiresApproval"] for task in fragment["tasks"]))
        self.assertNotIn("export", [task["operation"] for task in fragment["tasks"]])

    def test_agent_plan_publish_creates_one_task_per_deliverable(self):
        (self.workspace / "deliverables" / "a.md").write_text("# A\n", encoding="utf-8")
        (self.workspace / "deliverables" / "b.md").write_text("# B\n", encoding="utf-8")

        fragment = self.payload(
            self.server._tool_agent_plan(
                {
                    "capability": "document.publish",
                    "operation": "export",
                    "workspace": {"revision": "rev-publish"},
                    "constraints": {"maxConcurrency": 3},
                }
            )
        )

        self.assert_task_graph_fragment(fragment)
        self.assertEqual([task["operation"] for task in fragment["tasks"]], ["export", "export"])
        self.assertEqual([task["inputRefs"][0]["ref"] for task in fragment["tasks"]], ["deliverables/a.md", "deliverables/b.md"])
        self.assertEqual(fragment["tasks"][0]["locks"], ["deliverable:deliverables/a.md"])
        self.assertEqual(fragment["groups"][0]["recommendedConcurrency"], 3)

    def test_agent_plan_pipeline_full_scenario(self):
        (self.workspace / "raw" / "untracked").mkdir(parents=True)
        (self.workspace / "raw" / "untracked" / "source.md").write_text("# Source\n", encoding="utf-8")
        (self.workspace / "templates" / "report.md").write_text("---\noutput: report.md\n---\n# Report\n", encoding="utf-8")

        fragment = self.payload(
            self.server._tool_agent_plan(
                {
                    "capability": "knowledge.pipeline",
                    "operation": "pipeline",
                    "workspace": {"revision": "rev-pipeline"},
                    "constraints": {"requireApprovalForMutations": True},
                }
            )
        )

        self.assert_task_graph_fragment(fragment)
        operations = [task["operation"] for task in fragment["tasks"]]
        self.assertEqual(operations, ["ingest", "ingest_apply", "build", "export", "polish"])
        build = next(task for task in fragment["tasks"] if task["operation"] == "build")
        export = next(task for task in fragment["tasks"] if task["operation"] == "export")
        polish = next(task for task in fragment["tasks"] if task["operation"] == "polish")
        self.assertIn("ingest-apply", build["dependsOn"])
        self.assertIn(build["id"], export["dependsOn"])
        self.assertIn(export["id"], polish["dependsOn"])
        self.assertEqual(export["inputRefs"][0]["ref"], "deliverables/report.md")

    def test_agent_plan_pipeline_respects_requested_steps(self):
        (self.workspace / "raw" / "untracked").mkdir(parents=True)
        (self.workspace / "raw" / "untracked" / "source.md").write_text("# Source\n", encoding="utf-8")
        (self.workspace / "templates" / "report.md").write_text("# Report\n", encoding="utf-8")

        fragment = self.payload(
            self.server._tool_agent_plan(
                {
                    "capability": "knowledge.pipeline",
                    "operation": "pipeline",
                    "workspace": {"revision": "rev-pipeline-steps"},
                    "arguments": {"steps": ["ingest"]},
                }
            )
        )

        self.assert_task_graph_fragment(fragment)
        operations = [task["operation"] for task in fragment["tasks"]]
        self.assertEqual(operations, ["ingest", "ingest_apply"])
        self.assertNotIn("build", operations)
        self.assertNotIn("export", operations)

    def test_agent_plan_aggregates_when_max_tasks_is_exceeded(self):
        (self.workspace / "templates" / "a.md").write_text("# A\n", encoding="utf-8")
        (self.workspace / "templates" / "b.md").write_text("# B\n", encoding="utf-8")

        fragment = self.payload(
            self.server._tool_agent_plan(
                {
                    "capability": "document.build",
                    "operation": "build",
                    "workspace": {"revision": "rev-aggregate"},
                    "constraints": {"maxTasks": 1},
                }
            )
        )

        self.assert_task_graph_fragment(fragment)
        self.assertEqual(len(fragment["tasks"]), 1)
        self.assertEqual(fragment["tasks"][0]["id"], "build-batch")
        self.assertEqual(fragment["tasks"][0]["arguments"]["templates"], ["templates/a.md", "templates/b.md"])
        self.assertEqual(fragment["tasks"][0]["progressWeight"], 2)

    def test_agent_plan_is_listed_and_callable(self):
        tools = asyncio.run(self.server.list_tools())
        self.assertIn("agent_plan", [tool.name for tool in tools])

        payload = self.payload(
            asyncio.run(
                self.server.call_tool(
                    "agent_plan",
                    {
                        "capability": "knowledge.update",
                        "operation": "ingest",
                        "workspace": {"revision": "rev-call"},
                    },
                )
            )
        )
        self.assertEqual(payload["capability"], "knowledge.update")

    def test_agent_execute_status_done_on_doctor(self):
        async def scenario():
            async def successful_cli_step(
                _job_id,
                _step,
                _inputs,
                _templates,
                _deliverables,
                stabilize=False,
                config_path=None,
            ):
                return 0

            self.server._run_cli_step = successful_cli_step
            accepted = self.payload(
                await self.server._tool_agent_execute(
                    {
                        "operation": "doctor",
                        "workspace": {"name": "requested-workspace"},
                    }
                )
            )

            self.assertTrue(accepted["accepted"])
            await self.server._ACTIVE_TASKS[accepted["jobId"]]
            status = self.payload(self.server._tool_agent_status({"jobId": accepted["jobId"]}))

            self.assertEqual(status["status"], "done")
            self.assertEqual(status["progress"]["percent"], 100)
            self.assertEqual(status["result"]["status"], "succeeded")
            self.assertIn("durationMs", status["result"]["metrics"])

        asyncio.run(scenario())

    def test_agent_cancel_cancels_one_job(self):
        async def scenario():
            async def hold_job(_job_id):
                await asyncio.Future()

            self.server._run_job = hold_job
            accepted = self.payload(
                await self.server._tool_agent_execute(
                    {
                        "operation": "doctor",
                        "workspace": {"name": "requested-workspace"},
                    }
                )
            )

            status = self.payload(await self.server._tool_agent_cancel({"jobId": accepted["jobId"]}))

            self.assertEqual(status["jobId"], accepted["jobId"])
            self.assertEqual(status["status"], "cancelled")
            self.assertEqual(status["result"]["status"], "cancelled")

        asyncio.run(scenario())

    def test_agent_execute_uses_request_workspace_name(self):
        async def scenario():
            async def hold_job(_job_id):
                await asyncio.Future()

            self.server._run_job = hold_job
            accepted = self.payload(
                await self.server._tool_agent_execute(
                    {
                        "operation": "doctor",
                        "workspace": {"name": "request-wins"},
                    }
                )
            )
            job = self.server._load_job(accepted["jobId"])
            status = self.payload(self.server._tool_agent_status({"jobId": accepted["jobId"]}))

            self.assertEqual(job["workspace"], "request-wins")
            self.assertEqual(status["workspace"], {"name": "request-wins"})
            await self.server._tool_agent_cancel({"jobId": accepted["jobId"]})

        asyncio.run(scenario())

    def test_agent_status_failed_job_includes_task_result_error(self):
        async def scenario():
            async def failing_cli_step(
                _job_id,
                _step,
                _inputs,
                _templates,
                _deliverables,
                stabilize=False,
                config_path=None,
            ):
                return 2

            self.server._run_cli_step = failing_cli_step
            accepted = self.payload(
                await self.server._tool_agent_execute(
                    {
                        "operation": "doctor",
                        "workspace": {"name": "requested-workspace"},
                    }
                )
            )

            await self.server._ACTIVE_TASKS[accepted["jobId"]]
            status = self.payload(self.server._tool_agent_status({"jobId": accepted["jobId"]}))

            self.assertEqual(status["status"], "failed")
            self.assertEqual(status["result"]["status"], "failed")
            self.assertEqual(status["result"]["error"]["code"], "execution_failed")
            self.assertIs(status["result"]["error"]["retryable"], False)

        asyncio.run(scenario())

    def test_agent_execute_status_cancel_are_listed_and_callable(self):
        async def scenario():
            async def hold_job(_job_id):
                await asyncio.Future()

            self.server._run_job = hold_job
            tools = await self.server.list_tools()
            names = [tool.name for tool in tools]
            self.assertIn("agent_execute", names)
            self.assertIn("agent_status", names)
            self.assertIn("agent_cancel", names)

            accepted = self.payload(
                await self.server.call_tool(
                    "agent_execute",
                    {
                        "operation": "doctor",
                        "workspace": {"name": "requested-workspace"},
                    },
                )
            )
            status = self.payload(await self.server.call_tool("agent_status", {"jobId": accepted["jobId"]}))
            cancelled = self.payload(await self.server.call_tool("agent_cancel", {"jobId": accepted["jobId"]}))

            self.assertTrue(accepted["accepted"])
            self.assertEqual(status["status"], "queued")
            self.assertEqual(cancelled["status"], "cancelled")

        asyncio.run(scenario())

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

    def test_ingest_dry_run_expands_input_globs(self):
        (self.workspace / "raw" / "untracked").mkdir(parents=True)
        (self.workspace / "raw" / "untracked" / "b.md").write_text("# B\n", encoding="utf-8")
        (self.workspace / "raw" / "untracked" / "a.md").write_text("# A\n", encoding="utf-8")
        result = asyncio.run(
            self.server._tool_start_job(
                {
                    "type": "ingest",
                    "inputs": ["raw/untracked/*.md"],
                    "dryRun": True,
                }
            )
        )
        payload = self.payload(result)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["plan"]["inputs"], ["raw/untracked/a.md", "raw/untracked/b.md"])

    def test_ingest_dry_run_rejects_unmatched_input_globs(self):
        with self.assertRaisesRegex(ValueError, r"No files match raw/untracked/\*.md"):
            asyncio.run(
                self.server._tool_start_job(
                    {
                        "type": "ingest",
                        "inputs": ["raw/untracked/*.md"],
                        "dryRun": True,
                    }
                )
            )

    def test_ingest_plan_dry_run_accepts_target_inputs_without_workspace_write_lock(self):
        result = asyncio.run(
            self.server._tool_start_job(
                {
                    "type": "ingest_plan",
                    "inputs": ["raw/untracked/doc-a.md", "doc-b.md"],
                    "dryRun": True,
                }
            )
        )
        payload = self.payload(result)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["plan"]["steps"], ["ingest_plan"])
        self.assertEqual(payload["plan"]["lockScopes"], ["read"])
        self.assertIn("node /app/bin/wiki.js ingest --plan-only raw/untracked/doc-a.md doc-b.md", payload["commands"])

    def test_ingest_apply_dry_run_accepts_plan_files_with_workspace_write_lock(self):
        result = asyncio.run(
            self.server._tool_start_job(
                {
                    "type": "ingest_apply",
                    "inputs": [".wiki/ingest-plans/plan-a.json"],
                    "dryRun": True,
                }
            )
        )
        payload = self.payload(result)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["plan"]["steps"], ["ingest_apply"])
        self.assertEqual(payload["plan"]["lockScopes"], ["workspace-write"])
        self.assertIn("node /app/bin/wiki.js ingest --apply .wiki/ingest-plans/plan-a.json", payload["commands"])

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

    def test_targeted_build_jobs_are_faster_in_parallel(self):
        async def scenario():
            async def slow_cli_step(
                _job_id,
                _step,
                _inputs,
                _templates,
                _deliverables,
                stabilize=False,
                config_path=None,
            ):
                await asyncio.sleep(0.5)
                return 0

            async def run_build(template):
                payload = self.payload(
                    await self.server._tool_start_job(
                        {"type": "build", "templates": [template], "confirm": True}
                    )
                )
                self.assertTrue(payload["ok"])
                task = self.server._ACTIVE_TASKS[payload["jobId"]]
                await task
                return payload["jobId"]

            self.server._run_cli_step = slow_cli_step
            (self.workspace / "templates" / "a.md").write_text("---\noutput: a.md\n---\n# A\n", encoding="utf-8")
            (self.workspace / "templates" / "b.md").write_text("---\noutput: b.md\n---\n# B\n", encoding="utf-8")

            sequential_start = time.perf_counter()
            await run_build("a.md")
            await run_build("b.md")
            sequential_seconds = time.perf_counter() - sequential_start

            parallel_start = time.perf_counter()
            await asyncio.gather(run_build("a.md"), run_build("b.md"))
            parallel_seconds = time.perf_counter() - parallel_start

            self.assertLess(parallel_seconds, sequential_seconds * 0.65)

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
